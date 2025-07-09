package com.example.recon

import com.example.recon.config.ReconJobConfig
import com.example.recon.jobs.{BusinessRuleReconJob, ReconResult, ReconciliationJob, SourceToTargetReconJob}
import com.example.recon.utils.{DbConfig, JdbcUtil, SparkSessionUtil}
import com.example.recon.writers.DataWriter
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

case class JobExecutionLog(
  jobName: String,
  executionTimestamp: LocalDateTime,
  status: String, // e.g., STARTED, S2T_SUCCESS, BR_SUCCESS, S2T_FAILED, BR_FAILED, COMPLETED, FAILED_SETUP
  message: String,
  s2tSummaryPath: Option[String] = None,
  s2tDetailsPath: Option[String] = None,
  brSummaryPath: Option[String] = None,
  brDetailsPath: Option[String] = None
)

object MainApp extends LazyLogging {

  def main(args: Array[String]): Unit = {
    // Basic command-line argument parsing for DB config table name
    // In a real app, use a proper CLI parsing library (e.g., Scopt)
    if (args.length < 4) {
      logger.error("Usage: spark-submit <...> com.example.recon.MainApp <jdbcUrl> <dbUser> <dbPassword> <configTableName> [sparkMasterUrl]")
      System.exit(1)
    }

    val jdbcUrl = args(0)
    val dbUser = args(1)
    val dbPassword = args(2)
    val configTableName = args(3)
    val sparkMasterUrl = if (args.length > 4) args(4) else "local[*]" // Default to local if not provided

    val appName = "DataReconciliationFramework"
    val spark = SparkSessionUtil.getSparkSession(appName, sparkMasterUrl)
    val dbConfig = DbConfig(jdbcUrl, dbUser, dbPassword)

    val executionLogs = scala.collection.mutable.ListBuffer[JobExecutionLog]()

    try {
      logger.info(s"Fetching reconciliation job configurations from Oracle table: $configTableName")
      JdbcUtil.fetchReconJobConfigs(dbConfig, configTableName) match {
        case Success(jobConfigs) =>
          if (jobConfigs.isEmpty) {
            logger.warn("No reconciliation job configurations found in the table. Exiting.")
          } else {
            logger.info(s"Found ${jobConfigs.size} job configurations. Starting processing...")
            jobConfigs.foreach(config => processReconJob(spark, config, executionLogs))
          }
        case Failure(e) =>
          logger.error(s"Failed to fetch job configurations from Oracle: ${e.getMessage}", e)
          executionLogs += JobExecutionLog("FRAMEWORK_SETUP", LocalDateTime.now(), "FAILED_SETUP", s"Failed to fetch configs: ${e.getMessage}")
      }
    } catch {
      case e: Exception =>
        logger.error(s"An unexpected error occurred in MainApp: ${e.getMessage}", e)
        executionLogs += JobExecutionLog("FRAMEWORK_GLOBAL", LocalDateTime.now(), "UNEXPECTED_FAILURE", s"Global error: ${e.getMessage}")
    } finally {
      logAndPersistExecutionSummary(spark, executionLogs.toList, dbConfig) // Placeholder for persisting logs
      JdbcUtil.closeConnection()
      SparkSessionUtil.stopSparkSession()
      logger.info("Reconciliation framework finished.")
    }
  }

  private def processReconJob(spark: SparkSession, config: ReconJobConfig, executionLogs: scala.collection.mutable.ListBuffer[JobExecutionLog]): Unit = {
    val jobStartTime = LocalDateTime.now()
    logger.info(s"Starting processing for job: ${config.jobName}")
    executionLogs += JobExecutionLog(config.jobName, jobStartTime, "STARTED", s"Processing started for job ${config.jobName}")

    var s2tSummaryWrittenTo: Option[String] = None
    var s2tDetailsWrittenTo: Option[String] = None
    var brSummaryWrittenTo: Option[String] = None
    var brDetailsWrittenTo: Option[String] = None

    try {
      if (config.runSourceToTarget) {
        logger.info(s"Job ${config.jobName}: Source-to-Target check is enabled (sourceToTargetFlag='Y').")
        val s2tJob = new SourceToTargetReconJob()
        val s2tResult = s2tJob.run(spark, config)
        handleReconResult(spark, config, s2tResult, "SourceToTarget", executionLogs)

        // Write S2T summary
        val s2tSummaryPath = s"${config.outputPath}/${config.jobName}/s2t_summary"
        DataWriter.writeData(spark, s2tResult.summary, s2tSummaryPath, s"${config.jobName}_S2T_Summary", saveMode = SaveMode.Overwrite) match {
            case Success(_) => s2tSummaryWrittenTo = Some(s2tSummaryPath)
            case Failure(e) => logger.error(s"Job ${config.jobName}: Failed to write S2T summary to $s2tSummaryPath", e)
        }

        // Write S2T detailed mismatches if present
        s2tResult.detailedMismatches.filterNot(_.isEmpty).foreach { df =>
          val s2tDetailsPath = s"${config.outputPath}/${config.jobName}/s2t_details"
          DataWriter.writeData(spark, df, s2tDetailsPath, s"${config.jobName}_S2T_Details", saveMode = SaveMode.Overwrite) match {
            case Success(_) => s2tDetailsWrittenTo = Some(s2tDetailsPath)
            case Failure(e) => logger.error(s"Job ${config.jobName}: Failed to write S2T details to $s2tDetailsPath", e)
          }
        }
      } else {
        logger.info(s"Job ${config.jobName}: Source-to-Target check is disabled (sourceToTargetFlag='N').")
      }

      if (config.runBusinessRules) {
        logger.info(s"Job ${config.jobName}: Business Rule check is enabled (businessRuleFlag='Y' and SQL is present).")
        val brJob = new BusinessRuleReconJob()
        val brResult = brJob.run(spark, config)
        handleReconResult(spark, config, brResult, "BusinessRule", executionLogs)

        // Write BR summary
        val brSummaryPath = s"${config.outputPath}/${config.jobName}/br_summary"
        DataWriter.writeData(spark, brResult.summary, brSummaryPath, s"${config.jobName}_BR_Summary", saveMode = SaveMode.Overwrite) match {
            case Success(_) => brSummaryWrittenTo = Some(brSummaryPath)
            case Failure(e) => logger.error(s"Job ${config.jobName}: Failed to write BR summary to $brSummaryPath", e)
        }

        // Write BR detailed mismatches if present
        brResult.detailedMismatches.filterNot(_.isEmpty).foreach { df =>
          val brDetailsPath = s"${config.outputPath}/${config.jobName}/br_details"
          DataWriter.writeData(spark, df, brDetailsPath, s"${config.jobName}_BR_Details", saveMode = SaveMode.Overwrite) match {
            case Success(_) => brDetailsWrittenTo = Some(brDetailsPath)
            case Failure(e) => logger.error(s"Job ${config.jobName}: Failed to write BR details to $brDetailsPath", e)
          }
        }
      } else {
        logger.info(s"Job ${config.jobName}: Business Rule check is disabled (businessRuleFlag='N' or SQL is missing).")
      }

      executionLogs += JobExecutionLog(
        config.jobName,
        LocalDateTime.now(),
        "COMPLETED",
        s"Successfully processed job ${config.jobName}.",
        s2tSummaryWrittenTo, s2tDetailsWrittenTo, brSummaryWrittenTo, brDetailsWrittenTo
      )

    } catch {
      case e: Exception =>
        logger.error(s"An error occurred while processing job ${config.jobName}: ${e.getMessage}", e)
        executionLogs += JobExecutionLog(config.jobName, LocalDateTime.now(), "JOB_FAILED", s"Error in job ${config.jobName}: ${e.getMessage}")
    } finally {
      logger.info(s"Finished processing for job: ${config.jobName}. Duration: ${java.time.Duration.between(jobStartTime, LocalDateTime.now())}")
    }
  }

  private def handleReconResult(
    spark: SparkSession,
    config: ReconJobConfig,
    result: ReconResult,
    jobType: String, // "SourceToTarget" or "BusinessRule"
    executionLogs: scala.collection.mutable.ListBuffer[JobExecutionLog]
  ): Unit = {
    logger.info(s"Job ${config.jobName} ($jobType) Result Status: ${result.status}, Message: ${result.message.getOrElse("N/A")}")
    result.summary.show(false) // Log summary to console

    val logStatus = s"${jobType.toUpperCase}_${result.status}"
    executionLogs += JobExecutionLog(config.jobName, LocalDateTime.now(), logStatus, result.message.getOrElse(s"$jobType finished with status ${result.status}."))

    // Further actions based on result.status if needed (e.g., notifications)
  }

  private def logAndPersistExecutionSummary(
      spark: SparkSession,
      logs: List[JobExecutionLog],
      dbConfig: DbConfig // For future persistence to Oracle
    ): Unit = {
    if (logs.isEmpty) return

    import spark.implicits._
    logger.info("Overall Execution Summary:")
    // Convert to DataFrame for easy display and potential persistence
    // Note: LocalDateTime might need a specific encoder or conversion to Timestamp for Spark DataFrame
    val logsDF = logs.map(log =>
        (log.jobName, log.executionTimestamp.toString, log.status, log.message,
         log.s2tSummaryPath.getOrElse("N/A"), log.s2tDetailsPath.getOrElse("N/A"),
         log.brSummaryPath.getOrElse("N/A"), log.brDetailsPath.getOrElse("N/A"))
    ).toDF("job_name", "timestamp_str", "status", "message",
           "s2t_summary_path", "s2t_details_path", "br_summary_path", "br_details_path")

    logsDF.show(numRows = logs.size, truncate = false)

    // Placeholder: Persist logsDF to an Oracle table or a file
    // Example: DataWriter.writeData(spark, logsDF, "recon_framework_runs_log_table_or_path", "FrameworkExecutionLog")
    logger.info("Execution summary logged. Persistence to DB/File can be added here.")
    // For now, we just log to console. To write to Oracle, a new table and DML operations in JdbcUtil would be needed.
  }

}
