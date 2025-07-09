package com.example.recon.writers

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object DataWriter extends LazyLogging {

  /**
   * Writes a DataFrame to the specified output path.
   * The path can be an HDFS path (for file-based output) or a Hive table name.
   *
   * @param spark The active SparkSession.
   * @param df The DataFrame to write.
   * @param outputPath The destination path or Hive table name.
   *                   If it contains a '.', it's assumed to be a Hive table (schema.table).
   *                   Otherwise, it's treated as an HDFS path.
   * @param jobName The name of the job, used for logging and potentially for partitioning.
   * @param outputFormatForFiles The format to use if writing to a file system (e.g., "parquet", "csv", "json").
   *                             Defaults to "parquet".
   * @param saveMode The SaveMode to use (e.g., Append, Overwrite). Defaults to Overwrite.
   * @return A Try[Unit] indicating success or failure.
   */
  def writeData(
    spark: SparkSession,
    df: DataFrame,
    outputPath: String,
    jobName: String, // For logging/context
    outputFormatForFiles: String = "parquet",
    saveMode: SaveMode = SaveMode.Overwrite // Default to Overwrite, can be configured
  ): Try[Unit] = {

    if (df == null || df.isEmpty) {
      logger.warn(s"DataFrame for job '$jobName' is null or empty. Skipping write to output path: $outputPath")
      return Success(()) // Nothing to write, not an error
    }

    Try {
      // Determine if outputPath is a Hive table or an HDFS path
      if (outputPath.contains(".")) { // Simple heuristic: "schema.table" indicates Hive
        writeToHiveTable(spark, df, outputPath, jobName, saveMode)
      } else {
        writeToFileSystem(spark, df, outputPath, jobName, outputFormatForFiles, saveMode)
      }
    }.recoverWith {
      case e: Exception =>
        logger.error(s"Failed to write data for job '$jobName' to path '$outputPath': ${e.getMessage}", e)
        Failure(e)
    }
  }

  private def writeToHiveTable(
    spark: SparkSession,
    df: DataFrame,
    tableName: String,
    jobName: String,
    saveMode: SaveMode
  ): Unit = {
    logger.info(s"Writing data for job '$jobName' to Hive table: $tableName with SaveMode: $saveMode")
    // For Hive, it's common to use dynamic partitioning or insert overwrite specific partitions.
    // Here, we'll do a simple saveAsTable which might overwrite or append based on saveMode.
    // Ensure the table exists or Spark is configured to create it if needed.
    // df.write.mode(saveMode).format("hive").saveAsTable(tableName) // format("hive") is often implicit

    // Add job_name and current timestamp to easily identify the load if appending
    val dfWithMetadata = df.withColumn("recon_load_job_name", lit(jobName))
                           .withColumn("recon_load_timestamp", current_timestamp())

    dfWithMetadata.write.mode(saveMode).saveAsTable(tableName)
    logger.info(s"Successfully wrote data for job '$jobName' to Hive table: $tableName")
  }

  private def writeToFileSystem(
    spark: SparkSession,
    df: DataFrame,
    path: String,
    jobName: String,
    format: String,
    saveMode: SaveMode
  ): Unit = {
    logger.info(s"Writing data for job '$jobName' to file system path: $path, format: $format, SaveMode: $saveMode")

    val writer = df.write.mode(saveMode)

    format.toLowerCase match {
      case "parquet" => writer.parquet(path)
      case "csv" => writer.option("header", "true").csv(path) // Default to include header for CSV
      case "json" => writer.json(path)
      case "orc" => writer.orc(path)
      case "text" => writer.text(path)
      case other =>
        val errMsg = s"Unsupported file format for output: '$other' for job '$jobName', path '$path'. Defaulting to Parquet."
        logger.warn(errMsg)
        // Optionally throw an error or default to a common format like parquet
        // throw new IllegalArgumentException(errMsg)
        writer.parquet(path) // Defaulting to parquet if format is unknown
    }
    logger.info(s"Successfully wrote data for job '$jobName' to file system path: $path as $format")
  }

  // Helper function to make lit (literal) and current_timestamp available if not imported in calling scope
  // Not strictly necessary here as this object won't use them directly without SparkSession.implicits._
  // but good for completeness if these functions were used more broadly within this object.
  private def lit(value: Any) = org.apache.spark.sql.functions.lit(value)
  private def current_timestamp() = org.apache.spark.sql.functions.current_timestamp()
}
