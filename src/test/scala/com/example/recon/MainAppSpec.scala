package com.example.recon

import com.example.recon.utils.{DbConfig, JdbcUtil, SparkSessionUtil}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.{Connection, DriverManager, Statement}
import scala.util.Try
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.{Files, Path}
import scala.reflect.io.Directory


class MainAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var h2Connection: Connection = _
  private val h2DbUrl = "jdbc:h2:mem:mainapptestdb;DB_CLOSE_DELAY=-1"
  private val dbUser = "sa"
  private val dbPass = ""
  private val configTableName = "RECON_CONFIG_TABLE_MAINAPP"
  val tempDir: Path = Files.createTempDirectory("mainAppTestOutput")


  override def beforeAll(): Unit = {
    Class.forName("org.h2.Driver")
    h2Connection = DriverManager.getConnection(h2DbUrl, dbUser, dbPass)
    val stmt: Statement = h2Connection.createStatement()
    Try {
      stmt.execute(s"DROP TABLE IF EXISTS $configTableName")
      stmt.execute(
        s"""
          |CREATE TABLE $configTableName (
          |  jobName VARCHAR(255) PRIMARY KEY, sourceType VARCHAR(50), sourcePath VARCHAR(255),
          |  targetTable VARCHAR(255), primaryKeyColumns VARCHAR(255), sourceToTargetFlag CHAR(1),
          |  businessRuleFlag CHAR(1), columnMapping VARCHAR(1000), businessRuleSQL VARCHAR(4000),
          |  outputPath VARCHAR(255), fileFormat VARCHAR(50), fileDelimiter VARCHAR(10), fileHasHeader CHAR(1)
          |)
        """.stripMargin
      )
    }
    stmt.close()
     // Initialize Spark session for the test suite, MainApp will try to get/create its own
    SparkSessionUtil.getSparkSession("MainAppSpecGlobal", "local[2]")
  }

  override def afterAll(): Unit = {
    JdbcUtil.closeConnection()
    if (h2Connection != null && !h2Connection.isClosed) {
      h2Connection.close()
    }
    SparkSessionUtil.stopSparkSession() // Stop the global one for the spec
    Try(new Directory(tempDir.toFile).deleteRecursively())
  }

  override def beforeEach(): Unit = {
    val stmt = h2Connection.createStatement()
    Try(stmt.execute(s"DELETE FROM $configTableName"))
    stmt.execute(s"DELETE FROM $configTableName") // Clean before each test
    // Also clean up any H2 tables that might be created by Spark if not using a separate catalog
    Try(stmt.execute("DROP TABLE IF EXISTS test_db.target_table_s2t_mainapp"))
    Try(stmt.execute("DROP SCHEMA IF EXISTS test_db"))
    stmt.close()
    JdbcUtil.closeConnection() // Close any connection JdbcUtil might be holding

    // Recreate test_db for Hive operations in Spark
    val spark = SparkSessionUtil.getSparkSession() // get the current session
    Try(spark.sql("CREATE DATABASE IF NOT EXISTS test_db"))
    Try(spark.sql("USE test_db"))
  }

  private def createTempCsvFile(name: String, content: String, baseDir: Path = tempDir): String = {
    val filePath = baseDir.resolve(name)
    Files.createDirectories(filePath.getParent)
    val writer = new java.io.PrintWriter(filePath.toFile)
    try writer.write(content) finally writer.close()
    filePath.toString
  }


  "MainApp" should "run successfully with a valid S2T configuration" in {
    // Setup: Create a source CSV file
    val sourceCsvPath = createTempCsvFile("source_mainapp_s2t.csv", "id,name\n1,Alice\n2,Bob")

    // Setup: Create a target Hive table (using Spark SQL against H2 if configured, or default Spark warehouse)
    val spark = SparkSessionUtil.getSparkSession() // Get current session
    import spark.implicits._
    Seq((1, "Alice"), (3, "Charlie")).toDF("pk_id", "full_name")
      .write.mode("overwrite").saveAsTable("test_db.target_table_s2t_mainapp")

    // Setup: Insert a job config into H2
    val stmt = h2Connection.createStatement()
    stmt.executeUpdate(
      s"""
        |INSERT INTO $configTableName VALUES (
        |  'mainapp_s2t_job', 'FILE', '$sourceCsvPath', 'test_db.target_table_s2t_mainapp', 'id', 'Y', 'N',
        |  'id:pk_id,name:full_name', NULL, '${tempDir.resolve("mainapp_s2t_job_output").toString}', 'csv', ',', 'Y'
        |)
      """.stripMargin)
    stmt.close()

    val args = Array(h2DbUrl, dbUser, dbPass, configTableName, "local[1]") // Use local[1] for this test run

    // Capture System.out to check logs, or check file outputs
    val outputStream = new ByteArrayOutputStream()
    val originalOut = System.out
    System.setOut(new PrintStream(outputStream))

    try {
        MainApp.main(args)
    } finally {
        System.setOut(originalOut) // Restore System.out
    }

    val logs = outputStream.toString
    // Basic checks on logs
    logs should include ("Fetching reconciliation job configurations from Oracle table: RECON_CONFIG_TABLE_MAINAPP")
    logs should include ("Found 1 job configurations. Starting processing...")
    logs should include ("Starting processing for job: mainapp_s2t_job")
    logs should include ("Job mainapp_s2t_job: Source-to-Target check is enabled")
    logs should include ("Job mainapp_s2t_job (SourceToTarget) Result Status: SUCCESS")
    logs should include ("Finished processing for job: mainapp_s2t_job")
    logs should include ("Reconciliation framework finished.")
    logs should include ("Overall Execution Summary:")
    logs should include ("mainapp_s2t_job")
    logs should include ("COMPLETED") // Check for successful completion status in summary

    // Check for output files (basic check for existence)
    val s2tSummaryPath = tempDir.resolve("mainapp_s2t_job_output/mainapp_s2t_job/s2t_summary")
    Files.exists(s2tSummaryPath) should be (true)
    Files.list(s2tSummaryPath).findFirst().isPresent should be (true) // Check if directory is not empty

    val s2tDetailsPath = tempDir.resolve("mainapp_s2t_job_output/mainapp_s2t_job/s2t_details")
    // Details might be empty if no mismatches, but the directory should exist if job ran
    Files.exists(s2tDetailsPath) should be (true)
  }

  it should "handle no job configurations found" in {
    // No data in configTableName
    val args = Array(h2DbUrl, dbUser, dbPass, configTableName, "local[1]")
    val outputStream = new ByteArrayOutputStream()
    System.setOut(new PrintStream(outputStream))
    try {
        MainApp.main(args)
    } finally {
        System.setOut(originalOut)
    }
    val logs = outputStream.toString
    logs should include ("No reconciliation job configurations found in the table. Exiting.")
    logs should include ("Overall Execution Summary:") // Should still print summary, albeit empty of job runs
  }

  it should "handle failure to connect to DB for configs" in {
    val invalidDbUrl = "jdbc:h2:mem:nonexistentdb_for_fail_test;DB_CLOSE_DELAY=-1;IFEXISTS=TRUE" // IFEXISTS=TRUE will cause failure if DB not there
    val args = Array(invalidDbUrl, "invaliduser", "invalidpass", configTableName, "local[1]")

    val outputStream = new ByteArrayOutputStream()
    System.setOut(new PrintStream(outputStream))
    try {
        MainApp.main(args)
    } finally {
        System.setOut(originalOut)
    }
    val logs = outputStream.toString
    logs should include (s"Failed to fetch job configurations from Oracle: Cannot get property") // Part of H2 connection error
    logs should include ("FRAMEWORK_SETUP")
    logs should include ("FAILED_SETUP")
  }

  // More tests could be added for:
  // - Business Rule job execution through MainApp
  // - Both S2T and BR for a single config
  // - Errors during a specific job processing (e.g., source file not found for one job)
  // - Output to Hive tables (requires more complex setup/mocking or a real local Hive)

}
