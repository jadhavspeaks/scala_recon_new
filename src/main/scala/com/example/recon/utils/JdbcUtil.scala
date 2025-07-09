package com.example.recon.utils

import com.example.recon.config.ReconJobConfig
import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class DbConfig(jdbcUrl: String, dbUser: String, dbPass: String, driverClass: String = "oracle.jdbc.driver.OracleDriver")

object JdbcUtil extends LazyLogging {

  private var connection: Option[Connection] = None

  /**
   * Establishes a database connection.
   * This is a simplified connection management. For production, use a proper connection pool.
   */
  private def getConnection(dbConfig: DbConfig): Try[Connection] = {
    connection.filter(conn => Try(conn.isValid(1)).getOrElse(false)) match {
      case Some(conn) => Success(conn)
      case None =>
        Try {
          Class.forName(dbConfig.driverClass)
          val props = new Properties()
          props.setProperty("user", dbConfig.dbUser)
          props.setProperty("password", dbConfig.dbPass)
          // Add other Oracle specific properties if needed, e.g., connection timeout
          // props.setProperty("oracle.jdbc.ReadTimeout", "60000") // 60 seconds
          // props.setProperty("oracle.jdbc.connectTimeout", "10000") // 10 seconds
          val conn = DriverManager.getConnection(dbConfig.jdbcUrl, props)
          connection = Some(conn)
          logger.info("Successfully established new JDBC connection.")
          conn
        }.recoverWith {
          case e: SQLException =>
            logger.error(s"SQLException during JDBC connection: ${e.getMessage}", e)
            Failure(e)
          case e: ClassNotFoundException =>
            logger.error(s"JDBC driver not found: ${dbConfig.driverClass}", e)
            Failure(e)
          case e: Throwable =>
            logger.error(s"Failed to connect to database: ${e.getMessage}", e)
            Failure(e)
        }
    }
  }

  /**
   * Closes the current database connection if it's open.
   */
  def closeConnection(): Unit = {
    connection.foreach { conn =>
      Try(conn.close()) match {
        case Success(_) => logger.info("JDBC connection closed.")
        case Failure(e) => logger.warn(s"Error closing JDBC connection: ${e.getMessage}", e)
      }
      connection = None
    }
  }

  /**
   * Fetches all reconciliation job configurations from the specified Oracle table.
   *
   * @param dbConfig The database configuration.
   * @param configTableName The name of the table holding the job configurations.
   * @return A sequence of ReconJobConfig objects.
   */
  def fetchReconJobConfigs(dbConfig: DbConfig, configTableName: String): Try[Seq[ReconJobConfig]] = {
    getConnection(dbConfig).flatMap { conn =>
      var statement: PreparedStatement = null
      var resultSet: ResultSet = null
      val configs = ListBuffer[ReconJobConfig]()

      try {
        // It's good practice to select specific columns rather than SELECT *
        // Assuming column names based on ReconJobConfig fields. Adjust if they differ in the DB.
        val query =
          s"""
             |SELECT jobName, sourceType, sourcePath, targetTable, primaryKeyColumns,
             |       sourceToTargetFlag, businessRuleFlag, columnMapping,
             |       businessRuleSQL, outputPath, fileFormat, fileDelimiter, fileHasHeader
             |FROM $configTableName
           """.stripMargin
        statement = conn.prepareStatement(query)
        resultSet = statement.executeQuery()

        while (resultSet.next()) {
          try {
            val jobName = resultSet.getString("jobName")
            val pkColsRaw = resultSet.getString("primaryKeyColumns")
            val pkCols = if (pkColsRaw != null && pkColsRaw.trim.nonEmpty) {
                pkColsRaw.split(",").map(_.trim).toList
            } else {
                // This should ideally be caught by a NOT NULL constraint in DB or config validation
                throw new IllegalArgumentException(s"primaryKeyColumns is null or empty for job: $jobName")
            }

            val config = ReconJobConfig(
              jobName = jobName,
              sourceType = resultSet.getString("sourceType"),
              sourcePath = resultSet.getString("sourcePath"),
              targetTable = resultSet.getString("targetTable"),
              primaryKeyColumns = pkCols,
              sourceToTargetFlag = resultSet.getString("sourceToTargetFlag"),
              businessRuleFlag = resultSet.getString("businessRuleFlag"),
              columnMappingString = resultSet.getString("columnMapping"), // raw string
              businessRuleSQL = Option(resultSet.getString("businessRuleSQL")),
              outputPath = resultSet.getString("outputPath"),
              fileFormat = Option(resultSet.getString("fileFormat")),
              fileDelimiter = Option(resultSet.getString("fileDelimiter")),
              fileHasHeader = Option(resultSet.getString("fileHasHeader")).map(_.equalsIgnoreCase("Y")) // Assuming Y/N or true/false in DB
            )
            configs += config
          } catch {
            case e: IllegalArgumentException =>
              logger.error(s"Error processing config row: ${e.getMessage}. Skipping this config.", e)
              // Decide if one bad config row should stop everything or just be skipped.
            case e: Exception =>
              val currentJobName = Try(resultSet.getString("jobName")).getOrElse("UNKNOWN_JOB")
              logger.error(s"Error parsing configuration row for job '$currentJobName': ${e.getMessage}. Skipping this config.", e)
          }
        }
        logger.info(s"Successfully fetched ${configs.size} job configurations from table '$configTableName'.")
        Success(configs.toList)
      } catch {
        case e: SQLException =>
          logger.error(s"SQL error fetching configurations from $configTableName: ${e.getMessage}", e)
          Failure(e)
        case e: Throwable =>
          logger.error(s"Unexpected error fetching configurations: ${e.getMessage}", e)
          Failure(e)
      } finally {
        if (resultSet != null) Try(resultSet.close())
        if (statement != null) Try(statement.close())
        // The connection is kept open for potential reuse by subsequent calls in the same application run.
        // It will be closed by `closeConnection()` typically at the end of the application.
      }
    }
  }

  // It might be useful to add a method to execute DML/DDL for writing execution metadata back to Oracle later.
  // def executeUpdate(dbConfig: DbConfig, sql: String): Try[Int] = ...
}
