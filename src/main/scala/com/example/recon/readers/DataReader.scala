package com.example.recon.readers

import com.example.recon.config.ReconJobConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object DataReader extends LazyLogging {

  /**
   * Reads data based on the source configuration provided in ReconJobConfig.
   *
   * @param spark  The active SparkSession.
   * @param config The configuration for the specific job, containing source details.
   * @return A Try[DataFrame] which is Success(DataFrame) if reading is successful,
   *         or Failure(Exception) otherwise.
   */
  def readSourceData(spark: SparkSession, config: ReconJobConfig): Try[DataFrame] = {
    logger.info(s"Reading source data for job: ${config.jobName}, type: ${config.sourceType}, path: ${config.sourcePath}")
    config.sourceType.toUpperCase match {
      case "FILE" =>
        readFileSource(spark, config.sourcePath, config.fileFormat, config.fileDelimiter, config.fileHasHeader, None)
      case "HIVE" =>
        readHiveTable(spark, config.sourcePath) // sourcePath is "schema.table" for Hive
      case other =>
        val errMsg = s"Unsupported source type: '$other' for job ${config.jobName}. Supported types are FILE, HIVE."
        logger.error(errMsg)
        Failure(newIllegalArgumentException(errMsg))
    }
  }

  /**
   * Reads target data, which is always a Hive table.
   *
   * @param spark The active SparkSession.
   * @param tableName The full name of the Hive table (e.g., "schema.table").
   * @return A Try[DataFrame].
   */
  def readTargetData(spark: SparkSession, tableName: String): Try[DataFrame] = {
    logger.info(s"Reading target Hive table: $tableName")
    readHiveTable(spark, tableName)
  }

  /**
   * Reads data from a file source.
   *
   * @param spark         The active SparkSession.
   * @param path          The path to the file or directory.
   * @param format        Optional format of the file (e.g., "csv", "parquet", "json", "text", "orc").
   *                      If None, Spark will try to infer it for some formats or default to parquet.
   * @param delimiter     Optional delimiter for text-based files (e.g., CSV, TSV).
   * @param hasHeader     Optional boolean for text-based files indicating if there's a header.
   *                      Schema inference is attempted if this is true.
   * @param customSchema  Optional custom schema to apply to the data.
   * @return A Try[DataFrame].
   */
  def readFileSource(
    spark: SparkSession,
    path: String,
    format: Option[String],
    delimiter: Option[String],
    hasHeader: Option[Boolean],
    customSchema: Option[StructType]
  ): Try[DataFrame] = Try {
    val reader = spark.read
    format.map(_.toLowerCase) match {
      case Some("csv") =>
        val csvReader = reader.option("inferSchema", customSchema.isEmpty.toString)
          .option("header", hasHeader.getOrElse(true).toString) // Default to true if not specified for CSV
        delimiter.foreach(del => csvReader.option("delimiter", del))
        customSchema.foreach(schema => csvReader.schema(schema))
        logger.info(s"Reading CSV file from path: $path, header: ${hasHeader.getOrElse(true)}, delimiter: ${delimiter.getOrElse(",")}")
        csvReader.csv(path)
      case Some("json") =>
        customSchema.foreach(schema => reader.schema(schema))
        logger.info(s"Reading JSON file from path: $path")
        reader.json(path)
      case Some("parquet") =>
        logger.info(s"Reading Parquet file from path: $path")
        reader.parquet(path)
      case Some("orc") =>
        logger.info(s"Reading ORC file from path: $path")
        reader.orc(path)
      case Some("text") | Some("dat") => // Treat .dat as generic text, delimiter needed
        if (customSchema.isDefined) {
           // Text files don't directly support schema application in the same way as CSV.
           // This usually requires parsing each line (RDD operations) then converting to DF.
           // For simplicity, we'll assume if schema is provided for text, it's structured text like CSV.
           // A common scenario for .dat files.
          val textReader = reader.option("inferSchema", false.toString) // Schema is provided
                                .option("header", hasHeader.getOrElse(false).toString)
          delimiter.foreach(del => textReader.option("delimiter", del))
          logger.info(s"Reading text/dat file as CSV from path: $path, header: ${hasHeader.getOrElse(false)}, delimiter: ${delimiter.getOrElse("N/A")}, applying custom schema.")
          textReader.schema(customSchema.get).csv(path) // Use CSV reader for structured text
        } else {
          logger.info(s"Reading text file from path: $path. Each line will be a 'value' column.")
          reader.textFile(path).toDF("value")
        }
      case Some(unsupportedFormat) =>
        throw newIllegalArgumentException(s"Unsupported file format: '$unsupportedFormat'. Path: $path")
      case None =>
        // Attempt to read without specifying format, Spark might infer for Parquet.
        // Or throw error if format is mandatory. For now, let's make it mandatory for clarity.
        throw newIllegalArgumentException(s"File format must be specified for path: $path")
    }
  }.recoverWith {
    case e: Exception =>
      logger.error(s"Failed to read file source from path '$path': ${e.getMessage}", e)
      Failure(e)
  }

  /**
   * Reads data from a Hive table.
   *
   * @param spark     The active SparkSession.
   * @param tableName The full name of the Hive table (e.g., "schema.table").
   * @return A Try[DataFrame].
   */
  def readHiveTable(spark: SparkSession, tableName: String): Try[DataFrame] = Try {
    logger.info(s"Reading from Hive table: $tableName")
    spark.table(tableName)
  }.recoverWith {
    case e: Exception =>
      logger.error(s"Failed to read Hive table '$tableName': ${e.getMessage}", e)
      Failure(e)
  }
}
