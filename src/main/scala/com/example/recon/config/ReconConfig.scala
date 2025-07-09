package com.example.recon.config

/**
 * Represents a single column mapping from source to target.
 * Example: "source_column_name:target_column_name"
 * or for more complex scenarios, a JSON string could be used if needed,
 * but simple string splitting is assumed for now.
 */
case class ColumnMapping(sourceColumn: String, targetColumn: String)

object ColumnMapping {
  /**
   * Parses a string like "source_col:target_col" into a ColumnMapping object.
   * Handles potential errors during parsing.
   */
  def parse(mappingStr: String): Either[String, ColumnMapping] = {
    mappingStr.split(":", 2) match {
      case Array(src, tgt) if src.trim.nonEmpty && tgt.trim.nonEmpty =>
        Right(ColumnMapping(src.trim, tgt.trim))
      case _ =>
        Left(s"Invalid column mapping format: '$mappingStr'. Expected 'source_col:target_col'.")
    }
  }

  /**
   * Parses a delimited string of column mappings (e.g., "src1:tgt1,src2:tgt2")
   * into a list of ColumnMapping objects.
   * Returns a list of successfully parsed mappings and a list of errors.
   */
  def parseList(mappingsStr: String, delimiter: String = ","): (List[ColumnMapping], List[String]) = {
    if (mappingsStr == null || mappingsStr.trim.isEmpty) {
      (List.empty, List.empty)
    } else {
      val results = mappingsStr.split(delimiter)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(parse)
        .toList

      val successfulMappings = results.collect { case Right(mapping) => mapping }
      val errors = results.collect { case Left(error) => error }
      (successfulMappings, errors)
    }
  }
}

/**
 * Configuration for a single reconciliation job.
 *
 * @param jobName Unique identifier for the job.
 * @param sourceType Type of the source data (e.g., "FILE", "HIVE").
 * @param sourcePath Path to the source data (Linux path for FILE, "schema.table" for HIVE).
 * @param targetTable Name of the target Hive table ("schema.table").
 * @param primaryKeyColumns List of column names that form the primary key for joining/comparison. (NEWLY ADDED - crucial for comparison)
 * @param sourceToTargetFlag 'Y' if source-to-target reconciliation is needed, 'N' otherwise.
 * @param businessRuleFlag 'Y' if business rule reconciliation is needed, 'N' otherwise.
 * @param columnMappingString Raw string from Oracle DB representing column mappings.
 *                            Format: "src_col1:tgt_col1,src_col2:tgt_col2" or JSON.
 *                            This will be parsed into a List[ColumnMapping].
 * @param businessRuleSQL Optional SQL string to run on the source data for business rule checks.
 * @param outputPath HDFS path or Hive table name for storing reconciliation results.
 * @param fileFormat Optional format for source type FILE (e.g., "csv", "parquet", "json", "text"). (NEWLY ADDED)
 * @param fileDelimiter Optional delimiter for text-based files (e.g., ",", "|", "\t"). (NEWLY ADDED)
 * @param fileHasHeader Optional boolean for text-based files indicating if there's a header row. (NEWLY ADDED)
 */
case class ReconJobConfig(
  jobName: String,
  sourceType: String, // "FILE" or "HIVE"
  sourcePath: String,
  targetTable: String, // "schema.table"
  primaryKeyColumns: List[String],
  sourceToTargetFlag: String, // "Y" or "N"
  businessRuleFlag: String, // "Y" or "N"
  columnMappingString: String, // "src_col1:tgt_col1,src_col2:tgt_col2" or JSON
  businessRuleSQL: Option[String],
  outputPath: String,
  fileFormat: Option[String], // e.g., "csv", "parquet", "json", "text" (for sourceType="FILE")
  fileDelimiter: Option[String], // e.g., ",", "|", "\t" (for text-based files)
  fileHasHeader: Option[Boolean] // true or false (for text-based files)
) {

  // Validate flags
  require(Set("Y", "N").contains(sourceToTargetFlag.toUpperCase), s"sourceToTargetFlag must be 'Y' or 'N', got $sourceToTargetFlag for job $jobName")
  require(Set("Y", "N").contains(businessRuleFlag.toUpperCase), s"businessRuleFlag must be 'Y' or 'N', got $businessRuleFlag for job $jobName")
  require(primaryKeyColumns.nonEmpty, s"primaryKeyColumns cannot be empty for job $jobName")


  val parsedColumnMappings: List[ColumnMapping] = {
    val (mappings, errors) = ColumnMapping.parseList(columnMappingString)
    if (errors.nonEmpty) {
      // In a real app, you might throw an exception or log these errors more formally
      println(s"Warning: Errors parsing column mappings for job $jobName: ${errors.mkString("; ")}")
    }
    mappings
  }

  def runSourceToTarget: Boolean = sourceToTargetFlag.equalsIgnoreCase("Y")
  def runBusinessRules: Boolean = businessRuleFlag.equalsIgnoreCase("Y") && businessRuleSQL.exists(_.trim.nonEmpty)
}

object ReconJobConfig {
  // Placeholder for potential factory methods or constants if needed in the future
}
