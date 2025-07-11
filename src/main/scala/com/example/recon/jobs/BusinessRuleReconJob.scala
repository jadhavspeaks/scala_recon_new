package com.example.recon.jobs

import com.example.recon.config.ReconJobConfig
import com.example.recon.readers.DataReader
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

class BusinessRuleReconJob extends ReconciliationJob with LazyLogging {

  override def run(spark: SparkSession, config: ReconJobConfig): ReconResult = {
    logger.info(s"Starting Business Rule reconciliation for job: ${config.jobName}")

    if (config.businessRuleSQL.isEmpty || config.businessRuleSQL.get.trim.isEmpty) {
      val msg = s"BusinessRuleSQL is not defined or empty for job ${config.jobName}. Skipping business rule check."
      logger.warn(msg)
      val emptySummary = createEmptySummary(spark, "SKIPPED", msg)
      return ReconResult(emptySummary, None, "SKIPPED", Some(msg))
    }

    val ruleSQL = config.businessRuleSQL.get

    val overallResult = for {
      sourceDFRaw <- DataReader.readSourceData(spark, config)
      _ = sourceDFRaw.createOrReplaceTempView("source_data_view") // Register source DF as a temp view
      businessRuleResultDF <- executeBusinessRuleSQL(spark, ruleSQL, "source_data_view")
      targetDFRaw <- DataReader.readTargetData(spark, config.targetTable)
      // The comparison logic is very similar to SourceToTargetReconJob.
      // We'll use a helper or reuse parts if this becomes too duplicative.
      // For now, let's adapt the logic. The "source" for comparison is businessRuleResultDF.
      reconResult <- performComparison(spark, businessRuleResultDF, targetDFRaw, config)
    } yield reconResult

    // Clean up the temp view
    spark.catalog.dropTempView("source_data_view")

    overallResult match {
      case Success(result) =>
        logger.info(s"Business Rule reconciliation for job ${config.jobName} completed with status: ${result.status}")
        result
      case Failure(e) =>
        logger.error(s"Business Rule reconciliation for job ${config.jobName} failed: ${e.getMessage}", e)
        val emptySummary = createEmptySummary(spark, "FAILURE", s"Job failed: ${e.getMessage}")
        ReconResult(emptySummary, None, "FAILURE", Some(s"Job execution failed: ${e.getMessage}"))
    }
  }

  private def executeBusinessRuleSQL(spark: SparkSession, sqlQuery: String, sourceViewName: String): Try[DataFrame] = Try {
    logger.info(s"Executing Business Rule SQL: $sqlQuery on view: $sourceViewName")
    // Ensure the SQL references the view correctly, e.g., "SELECT * FROM source_data_view WHERE ..."
    // Or, if the SQL is self-contained and refers to tables Spark can access, that's also fine.
    // For this implementation, we assume the SQL uses the registered temp view "source_data_view".
    spark.sql(sqlQuery)
  }.recoverWith {
    case e: Exception =>
      logger.error(s"Failed to execute Business Rule SQL: $sqlQuery. Error: ${e.getMessage}", e)
      Failure(e)
  }

  /**
   * Performs the comparison between the business rule output and the target table.
   * This logic is largely similar to SourceToTargetReconJob's performReconciliation.
   * The `sourceDF` here is the result of the businessRuleSQL.
   * The `columnMapping` maps columns from the businessRuleSQL output to the target table.
   */
  private def performComparison(
    spark: SparkSession,
    businessRuleOutputDF: DataFrame, // This is the "source" for comparison
    targetDFRaw: DataFrame,
    config: ReconJobConfig
  ): Try[ReconResult] = Try {
    import spark.implicits._

    if (config.primaryKeyColumns.isEmpty) {
      throw new IllegalArgumentException(s"Primary key columns cannot be empty for business rule job ${config.jobName}")
    }
    val pkCols = config.primaryKeyColumns

    // 1. Alias columns from Business Rule Output (acting as source)
    // The `sourceColumn` in `parsedColumnMappings` refers to columns from `businessRuleOutputDF`.
    val sourceMappingMap = config.parsedColumnMappings.map(m => m.sourceColumn -> m.targetColumn).toMap
    val requiredSourceCols = (config.parsedColumnMappings.map(_.sourceColumn) ++ pkCols).distinct

    val missingSourceCols = requiredSourceCols.filterNot(businessRuleOutputDF.columns.contains)
    if (missingSourceCols.nonEmpty) {
      throw new IllegalArgumentException(s"Business Rule output for job ${config.jobName} is missing required columns (check your businessRuleSQL and columnMapping): ${missingSourceCols.mkString(", ")}")
    }

    // Suffix "_br_" for Business Rule output columns
    val sourceDF = businessRuleOutputDF.select(requiredSourceCols.map(col): _*)
      .transform(df => aliasSourceColumnsForJoin(df, config.parsedColumnMappings, pkCols, "_br_"))

    // 2. Alias columns from Target Table
    // The `targetColumn` in `parsedColumnMappings` refers to columns from `targetDFRaw`.
    val requiredTargetCols = (config.parsedColumnMappings.map(_.targetColumn) ++ pkCols).distinct

    val missingTargetCols = requiredTargetCols.filterNot(targetDFRaw.columns.contains)
    if (missingTargetCols.nonEmpty) {
      throw new IllegalArgumentException(s"Target DataFrame for job ${config.jobName} (${config.targetTable}) is missing required columns for business rule check: ${missingTargetCols.mkString(", ")}")
    }

    // Suffix "_tgt_" for Target table columns
    val targetDF = targetDFRaw.select(requiredTargetCols.map(col): _*)
      .transform(df => aliasTargetColumnsForJoin(df, config.parsedColumnMappings, pkCols, "_tgt_"))

    // 3. Join Business Rule Output and Target on Primary Key Columns
    val joinCondition = pkCols.map(pk => sourceDF(s"${pk}_br_") <=> targetDF(s"${pk}_tgt_")).reduce(_ && _)
    val joinedDF = sourceDF.join(targetDF, joinCondition, "full_outer")
    joinedDF.persist()
    logger.info(s"Job ${config.jobName} (Business Rule): Joined business rule output and target. Count: ${joinedDF.count()}")

    // 4. Determine Match Status (using the same helper as SourceToTargetReconJob)
    // The `buildComparisonExpressions` needs to know the correct suffixes.
    // We can adapt it or pass suffixes. For now, let's assume it expects "_src_" and "_tgt_".
    // So we might need to rename columns before calling it, or make it more flexible.
    // Let's create a local version or adapt.

    val (comparisonExpr, mismatchDetailsExpr) = buildComparisonExpressionsBR(
      config.parsedColumnMappings,
      pkCols,
      sourceDF.schema, // Schema of aliased business rule output
      targetDF.schema  // Schema of aliased target
    )

    val resultDF = joinedDF.withColumn("recon_status", comparisonExpr)
                           .withColumn("mismatch_details", mismatchDetailsExpr)

    // 5. Generate Summary Report
    val summaryDF = resultDF.groupBy("recon_status").count()
      .withColumnRenamed("count", "record_count")
      .withColumn("job_name", lit(s"${config.jobName}_BusinessRule"))
      .withColumn("timestamp", current_timestamp())
    logger.info(s"Job ${config.jobName} (Business Rule): Summary report generated.")
    summaryDF.show(truncate = false)

    // 6. Generate Detailed Mismatch Report
    val pkSelectExpr = pkCols.map(pk => coalesce(col(s"${pk}_br_"), col(s"${pk}_tgt_")).alias(pk))
    val mappedColsSelectExpr = config.parsedColumnMappings.flatMap { mapping =>
      Seq(
        col(s"${mapping.sourceColumn}_br_").alias(s"${mapping.sourceColumn}_business_rule_output"),
        col(s"${mapping.targetColumn}_tgt_").alias(s"${mapping.targetColumn}_target")
      )
    }
    val detailedMismatchesDF = resultDF
      .filter(col("recon_status") === "MISMATCHED")
      .select((pkSelectExpr ++ mappedColsSelectExpr :+ col("mismatch_details")): _*)
    logger.info(s"Job ${config.jobName} (Business Rule): Detailed mismatch report generated.")
    detailedMismatchesDF.show(truncate = false)

    joinedDF.unpersist()
    ReconResult(summaryDF, Some(detailedMismatchesDF), "SUCCESS", Some("Business Rule reconciliation completed."))
  }

  // Copied and adapted from SourceToTargetReconJob - consider refactoring into a common utility
  private def aliasSourceColumnsForJoin(df: DataFrame, mappings: List[com.example.recon.config.ColumnMapping], pkCols: List[String], suffix: String): DataFrame = {
    val mappedSourceCols = mappings.map(_.sourceColumn)
    val columnsToSelectAndAlias = (pkCols ++ mappedSourceCols).distinct
    val selectExpressions = columnsToSelectAndAlias.map { colName => df(colName).alias(s"$colName$suffix") }
    df.select(selectExpressions: _*)
  }

  private def aliasTargetColumnsForJoin(df: DataFrame, mappings: List[com.example.recon.config.ColumnMapping], pkCols: List[String], suffix: String): DataFrame = {
    val mappedTargetCols = mappings.map(_.targetColumn)
    val columnsToSelectAndAlias = (pkCols ++ mappedTargetCols).distinct
    val selectExpressions = columnsToSelectAndAlias.map { colName => df(colName).alias(s"$colName$suffix") }
    df.select(selectExpressions: _*)
  }

  // Adapted version for Business Rule specific suffixes
  private def buildComparisonExpressionsBR(
    mappings: List[com.example.recon.config.ColumnMapping],
    pkCols: List[String],
    sourceSchema: org.apache.spark.sql.types.StructType, // Business Rule output schema
    targetSchema: org.apache.spark.sql.types.StructType  // Target schema
  ): (org.apache.spark.sql.Column, org.apache.spark.sql.Column) = {

    val brSuffix = "_br_"
    val tgtSuffix = "_tgt_"

    val sourceExistsCond = pkCols.map(pk => col(s"$pk$brSuffix").isNotNull).reduce(_ && _)
    val targetExistsCond = pkCols.map(pk => col(s"$pk$tgtSuffix").isNotNull).reduce(_ && _)

    val columnComparisons = mappings.map { mapping =>
      val srcColName = s"${mapping.sourceColumn}$brSuffix" // Column from Business Rule output
      val tgtColName = s"${mapping.targetColumn}$tgtSuffix" // Column from Target table
      (col(srcColName) <=> col(tgtColName)).alias(s"${mapping.sourceColumn}_vs_${mapping.targetColumn}_match")
    }

    val allColumnsMatchCond = if (columnComparisons.nonEmpty) columnComparisons.reduce(_ && _) else lit(true)

    val statusExpr = when(sourceExistsCond && !targetExistsCond, lit("BUSINESS_RULE_ONLY"))
      .when(!sourceExistsCond && targetExistsCond, lit("TARGET_ONLY_VS_BR")) // Target exists, but no corresponding BR output
      .when(sourceExistsCond && targetExistsCond,
        when(allColumnsMatchCond, lit("MATCHED_BR_TARGET"))
          .otherwise(lit("MISMATCHED_BR_TARGET"))
      )
      .otherwise(lit("UNKNOWN_ERROR_STATE_BR"))

    // Each struct in the array will represent a single mismatched field comparison.
    val mismatchDetailStructs = mappings.map { mapping =>
      val srcColAliased = s"${mapping.sourceColumn}$brSuffix"
      val tgtColAliased = s"${mapping.targetColumn}$tgtSuffix"
      // This is the boolean column derived from (srcCol <=> tgtCol)
      val matchCompareCol = col(s"${mapping.sourceColumn}_vs_${mapping.targetColumn}_match")

      when(not(matchCompareCol), // Only create a struct if this specific pair is a mismatch
        struct(
          lit(mapping.sourceColumn).alias("source_column_name"), // Column from BR output
          col(srcColAliased).cast(StringType).alias("source_value"),
          lit(mapping.targetColumn).alias("target_column_name"), // Column from Target table
          col(tgtColAliased).cast(StringType).alias("target_value")
        )
      ).otherwise(lit(null)) // If they match, produce null for this mapping's detail struct
    }

    // Filter out the nulls (where individual pairs matched) and create a JSON array of the mismatch structs
    val mismatchDetailsFinalExpr = when(col("recon_status") === "MISMATCHED_BR_TARGET",
      to_json(expr(s"filter(array(${mismatchDetailStructs.map(_.expr.sql).mkString(",")}), x -> x is not null)"))
    ).otherwise(lit(null).cast(StringType)) // Consistent StringType for null literal

    (statusExpr, mismatchDetailsFinalExpr.alias("mismatch_details"))
  }

  private def createEmptySummary(spark: SparkSession, status: String, message: String): DataFrame = {
    import spark.implicits._
    Seq(
      (status, 0L, message, java.sql.Timestamp.from(java.time.Instant.now()))
    ).toDF("recon_status", "record_count", "message", "timestamp")
  }
}
