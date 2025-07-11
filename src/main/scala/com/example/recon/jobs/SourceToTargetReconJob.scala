package com.example.recon.jobs

import com.example.recon.config.{ColumnMapping, ReconJobConfig}
import com.example.recon.readers.DataReader
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, Column => SparkColumn}

import scala.util.{Failure, Success, Try}

class SourceToTargetReconJob extends ReconciliationJob with LazyLogging {

  override def run(spark: SparkSession, config: ReconJobConfig): ReconResult = {
    logger.info(s"Starting Source-to-Target reconciliation for job: ${config.jobName}")

    val overallResult = for {
      sourceDFRaw <- DataReader.readSourceData(spark, config)
      targetDFRaw <- DataReader.readTargetData(spark, config.targetTable)
      result <- performReconciliation(spark, sourceDFRaw, targetDFRaw, config)
    } yield result

    overallResult match {
      case Success(reconResult) =>
        logger.info(s"Source-to-Target reconciliation for job ${config.jobName} completed with status: ${reconResult.status}")
        reconResult
      case Failure(e) =>
        logger.error(s"Source-to-Target reconciliation for job ${config.jobName} failed: ${e.getMessage}", e)
        val emptySummary = createEmptySummary(spark, s"Job failed: ${e.getMessage}")
        ReconResult(emptySummary, None, "FAILURE", Some(s"Job execution failed: ${e.getMessage}"))
    }
  }

  private def performReconciliation(
    spark: SparkSession,
    sourceDFRaw: DataFrame,
    targetDFRaw: DataFrame,
    config: ReconJobConfig
  ): Try[ReconResult] = Try {
    import spark.implicits._

    // 0. Handle empty primary keys - though config validation should catch this
    if (config.primaryKeyColumns.isEmpty) {
      throw new IllegalArgumentException(s"Primary key columns cannot be empty for job ${config.jobName}")
    }
    val pkCols = config.primaryKeyColumns

    // 1. Select and Alias source columns based on mapping (and include PKs)
    // Mappings define which columns to compare. PKs are used for joining.
    // All mapped source columns + all PK columns from source must be selected.
    val sourceMappingMap = config.parsedColumnMappings.map(m => m.sourceColumn -> m.targetColumn).toMap
    val requiredSourceCols = (config.parsedColumnMappings.map(_.sourceColumn) ++ pkCols).distinct

    // Ensure all required columns exist in source
    val missingSourceCols = requiredSourceCols.filterNot(sourceDFRaw.columns.contains)
    if (missingSourceCols.nonEmpty) {
      throw new IllegalArgumentException(s"Source DataFrame for job ${config.jobName} is missing required columns: ${missingSourceCols.mkString(", ")}")
    }

    val sourceDF = sourceDFRaw.select(requiredSourceCols.map(col): _*)
      .transform(df => aliasSourceColumnsForJoin(df, config.parsedColumnMappings, pkCols, "_src_"))

    // 2. Select and Alias target columns (and include PKs)
    // All mapped target columns + all PK columns from target must be selected.
    val requiredTargetCols = (config.parsedColumnMappings.map(_.targetColumn) ++ pkCols).distinct

    // Ensure all required columns exist in target
    val missingTargetCols = requiredTargetCols.filterNot(targetDFRaw.columns.contains)
    if (missingTargetCols.nonEmpty) {
      throw new IllegalArgumentException(s"Target DataFrame for job ${config.jobName} (${config.targetTable}) is missing required columns: ${missingTargetCols.mkString(", ")}")
    }

    val targetDF = targetDFRaw.select(requiredTargetCols.map(col): _*)
      .transform(df => aliasTargetColumnsForJoin(df, config.parsedColumnMappings, pkCols, "_tgt_"))

    // 3. Join Source and Target on Primary Key Columns
    // Use a full outer join to find matched, source-only, and target-only records.
    val joinCondition = pkCols.map(pk => sourceDF(s"${pk}_src_") <=> targetDF(s"${pk}_tgt_")).reduce(_ && _)
    val joinedDF = sourceDF.join(targetDF, joinCondition, "full_outer")

    // Cache joinedDF as it will be used multiple times
    joinedDF.persist()
    logger.info(s"Job ${config.jobName}: Joined source and target dataframes. Count: ${joinedDF.count()}")

    // 4. Determine Match Status for each row
    // Conditions for different statuses:
    // - Matched: All PKs match, and all compared column pairs match.
    // - Mismatched: All PKs match, but at least one compared column pair does not match.
    // - Source Only: PK exists in source but not in target.
    // - Target Only: PK exists in target but not in source.

    val (comparisonExpr, mismatchDetailsExpr) = buildComparisonExpressions(
      config.parsedColumnMappings,
      pkCols,
      sourceDF.schema,
      targetDF.schema
    )

    val resultDF = joinedDF.withColumn("recon_status", comparisonExpr)
                           .withColumn("mismatch_details", mismatchDetailsExpr)

    // 5. Generate Summary Report
    val summaryDF = resultDF.groupBy("recon_status").count()
      .withColumnRenamed("count", "record_count")
      .withColumn("job_name", lit(config.jobName))
      .withColumn("timestamp", current_timestamp())

    logger.info(s"Job ${config.jobName}: Summary report generated.")
    summaryDF.show(truncate = false)

    // 6. Generate Detailed Mismatch Report (only for "MISMATCHED" rows)
    // Select original PK columns, mapped source/target columns, and mismatch details string.
    val pkSelectExpr = pkCols.map(pk => coalesce(col(s"${pk}_src_"), col(s"${pk}_tgt_")).alias(pk))

    val mappedColsSelectExpr = config.parsedColumnMappings.flatMap { mapping =>
      Seq(
        col(s"${mapping.sourceColumn}_src_").alias(s"${mapping.sourceColumn}_source"),
        col(s"${mapping.targetColumn}_tgt_").alias(s"${mapping.targetColumn}_target")
      )
    }

    val detailedMismatchesDF = resultDF
      .filter(col("recon_status") === "MISMATCHED")
      .select((pkSelectExpr ++ mappedColsSelectExpr :+ col("mismatch_details")): _*)

    logger.info(s"Job ${config.jobName}: Detailed mismatch report generated for MISMATCHED records.")
    detailedMismatchesDF.show(truncate = false) // Show for logging, actual write happens later

    // Unpersist cached DataFrame
    joinedDF.unpersist()

    ReconResult(summaryDF, Some(detailedMismatchesDF), "SUCCESS", Some("Source-to-Target reconciliation completed."))
  }

  /**
   * Aliases columns in the source DataFrame.
   * PK columns are aliased with suffix.
   * Mapped source columns are aliased with suffix.
   * Other columns (not PK, not mapped source) are dropped.
   */
  private def aliasSourceColumnsForJoin(df: DataFrame, mappings: List[ColumnMapping], pkCols: List[String], suffix: String): DataFrame = {
    val mappedSourceCols = mappings.map(_.sourceColumn)
    val columnsToSelectAndAlias = (pkCols ++ mappedSourceCols).distinct

    val selectExpressions = columnsToSelectAndAlias.map { colName =>
      df(colName).alias(s"${colName}${suffix}")
    }
    df.select(selectExpressions: _*)
  }

  /**
   * Aliases columns in the target DataFrame.
   * PK columns are aliased with suffix.
   * Mapped target columns are aliased with suffix.
   * Other columns (not PK, not mapped target) are dropped.
   */
  private def aliasTargetColumnsForJoin(df: DataFrame, mappings: List[ColumnMapping], pkCols: List[String], suffix: String): DataFrame = {
    val mappedTargetCols = mappings.map(_.targetColumn)
    val columnsToSelectAndAlias = (pkCols ++ mappedTargetCols).distinct

    val selectExpressions = columnsToSelectAndAlias.map { colName =>
      df(colName).alias(s"${colName}${suffix}")
    }
    df.select(selectExpressions: _*)
  }

  /**
   * Builds the Spark SQL expressions for determining row status and mismatch details.
   * Handles type casting for comparison and null-safe comparisons.
   */
  private def buildComparisonExpressions(
    mappings: List[ColumnMapping],
    pkCols: List[String],
    sourceSchema: org.apache.spark.sql.types.StructType,
    targetSchema: org.apache.spark.sql.types.StructType
  ): (SparkColumn, SparkColumn) = {

    // Condition for source PKs being non-null (record exists in source)
    val sourceExistsCond = pkCols.map(pk => col(s"${pk}_src_").isNotNull).reduce(_ && _)
    // Condition for target PKs being non-null (record exists in target)
    val targetExistsCond = pkCols.map(pk => col(s"${pk}_tgt_").isNotNull).reduce(_ && _)

    // Build comparison for each mapped column pair
    val columnComparisons = mappings.map { mapping =>
      val srcColName = s"${mapping.sourceColumn}_src_"
      val tgtColName = s"${mapping.targetColumn}_tgt_"

      // Basic type compatibility check (more sophisticated checks might be needed)
      // For now, we cast both to String for a common comparison ground if types differ.
      // A more robust solution would inspect sourceSchema and targetSchema for types.
      val srcCol = col(srcColName) // .cast(StringType) // Example: Cast to string for universal comparison
      val tgtCol = col(tgtColName) // .cast(StringType)

      // Null-safe comparison: (src <=> tgt)
      // This evaluates to true if both are null, or if both are non-null and equal.
      // It evaluates to false if one is null and the other isn't, or if they are non-null and different.
      // It evaluates to null if Spark's comparison logic results in null (shouldn't happen with <=>).
      (srcCol <=> tgtCol).alias(s"${mapping.sourceColumn}_vs_${mapping.targetColumn}_match")
    }

    // Overall match condition: all column comparisons must be true
    val allColumnsMatchCond = if (columnComparisons.nonEmpty) {
        columnComparisons.reduce(_ && _)
    } else {
        lit(true) // If no columns to compare, treat as match (applies if only PKs are defined)
    }

    // Expression for recon_status
    val statusExpr = when(sourceExistsCond && !targetExistsCond, lit("SOURCE_ONLY"))
      .when(!sourceExistsCond && targetExistsCond, lit("TARGET_ONLY"))
      .when(sourceExistsCond && targetExistsCond,
        when(allColumnsMatchCond, lit("MATCHED"))
          .otherwise(lit("MISMATCHED"))
      )
      .otherwise(lit("UNKNOWN_ERROR_STATE")) // Should not happen with full outer join

    // Expression for mismatch_details (JSON string or concatenated string)
    // For each mapping, if not matched, include "col_name: {src_val} != {tgt_val}"
    val mismatchDetailsArrayExpr = array(
        mappings.filterNot { mapping => // Filter out PKs if they are also in mappings for detail string
            pkCols.contains(mapping.sourceColumn) || pkCols.contains(mapping.targetColumn)
        }.map { mapping =>
            val srcColAliased = s"${mapping.sourceColumn}_src_"
            val tgtColAliased = s"${mapping.targetColumn}_tgt_"
            when(not(col(s"${mapping.sourceColumn}_vs_${mapping.targetColumn}_match")),
                concat(
                    lit(s"${mapping.sourceColumn}: {"), col(srcColAliased).cast(StringType), lit("} != {"),
                    col(tgtColAliased).cast(StringType), lit("}")
                )
            ).otherwise(null) // null if matched, so it can be filtered out
        }: _*
    )

    // Filter out nulls (matched columns) and join into a string
    val mismatchDetailStringExpr = expr("filter(mismatch_details_array, x -> x is not null)")
                                     .getField("concat_ws")("; ", col("array")) // This is a bit of a hack to call concat_ws on the filtered array

    // A cleaner way to build the string if the above expr trick doesn't work directly or is complex:
    // Use create_map and then to_json, or concat_ws after filtering nulls.
    // For now, let's generate a JSON array of structs for structured details.
    // Each struct in the array will represent a single mismatched field comparison.
    val mismatchDetailStructs = mappings.map { mapping =>
      val srcColAliased = s"${mapping.sourceColumn}_src_"
      val tgtColAliased = s"${mapping.targetColumn}_tgt_"
      // This is the boolean column derived from (srcCol <=> tgtCol)
      val matchCompareCol = col(s"${mapping.sourceColumn}_vs_${mapping.targetColumn}_match")

      when(not(matchCompareCol), // Only create a struct if this specific pair is a mismatch
        struct(
          lit(mapping.sourceColumn).alias("source_column_name"),
          col(srcColAliased).cast(StringType).alias("source_value"),
          lit(mapping.targetColumn).alias("target_column_name"),
          col(tgtColAliased).cast(StringType).alias("target_value")
        )
      ).otherwise(lit(null)) // If they match, produce null for this mapping's detail struct
    }

    // Filter out the nulls (where individual pairs matched) and create a JSON array of the mismatch structs
    // Ensure that expr is available or use selectExpr if it's a DataFrame operation.
    // Here, we are building a Column expression.
    val mismatchDetailsFinalExpr = when(col("recon_status") === "MISMATCHED",
      to_json(expr(s"filter(array(${mismatchDetailStructs.map(_.expr.sql).mkString(",")}), x -> x is not null)"))
    ).otherwise(lit(null).cast(StringType))

    (statusExpr, mismatchDetailsFinalExpr.alias("mismatch_details"))
  }

  private def createEmptySummary(spark: SparkSession, message: String): DataFrame = {
    import spark.implicits._
    Seq(
      ("FAILURE", 0L, message, java.sql.Timestamp.from(java.time.Instant.now()))
    ).toDF("recon_status", "record_count", "message", "timestamp")
  }
}
