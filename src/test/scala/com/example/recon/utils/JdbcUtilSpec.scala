package com.example.recon.utils

import com.example.recon.config.ReconJobConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.{Connection, DriverManager, Statement}
import scala.util.Try

class JdbcUtilSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var h2Connection: Connection = _
  private val h2DbUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1" // In-memory DB
  private val dbUser = "sa"
  private val dbPass = ""
  private val configTableName = "RECON_CONFIG_TABLE"

  private val testDbConfig = DbConfig(h2DbUrl, dbUser, dbPass, "org.h2.Driver")

  override def beforeAll(): Unit = {
    // Load H2 Driver
    Class.forName("org.h2.Driver")
    // Initialize H2 in-memory database and create table
    h2Connection = DriverManager.getConnection(h2DbUrl, dbUser, dbPass)
    val stmt: Statement = h2Connection.createStatement()
    Try {
      stmt.execute(s"DROP TABLE IF EXISTS $configTableName")
      stmt.execute(
        s"""
          |CREATE TABLE $configTableName (
          |  jobName VARCHAR(255) PRIMARY KEY,
          |  sourceType VARCHAR(50),
          |  sourcePath VARCHAR(255),
          |  targetTable VARCHAR(255),
          |  primaryKeyColumns VARCHAR(255),
          |  sourceToTargetFlag CHAR(1),
          |  businessRuleFlag CHAR(1),
          |  columnMapping VARCHAR(1000),
          |  businessRuleSQL VARCHAR(4000),
          |  outputPath VARCHAR(255),
          |  fileFormat VARCHAR(50),
          |  fileDelimiter VARCHAR(10),
          |  fileHasHeader CHAR(1)
          |)
        """.stripMargin
      )
    }
    stmt.close()
  }

  override def afterAll(): Unit = {
    JdbcUtil.closeConnection() // Close connection managed by JdbcUtil if any
    if (h2Connection != null && !h2Connection.isClosed) {
      h2Connection.close()
    }
  }

  override def beforeEach(): Unit = {
    // Clean the table before each test
    val stmt = h2Connection.createStatement()
    Try(stmt.execute(s"DELETE FROM $configTableName"))
    stmt.close()
    // Important: Close any connection JdbcUtil might be holding from a previous test
    JdbcUtil.closeConnection()
  }

  "JdbcUtil" should "fetch ReconJobConfigs from the database" in {
    // Insert test data
    val stmt = h2Connection.createStatement()
    stmt.executeUpdate(
      s"""
        |INSERT INTO $configTableName VALUES (
        |  'job1', 'FILE', '/path/file.csv', 'schema.target1', 'id,name', 'Y', 'N',
        |  'id:pk_id,name:full_name', NULL, '/output/job1', 'csv', ',', 'Y'
        |)
      """.stripMargin)
    stmt.executeUpdate(
      s"""
        |INSERT INTO $configTableName VALUES (
        |  'job2', 'HIVE', 'db.source_table', 'schema.target2', 'key', 'N', 'Y',
        |  'value:data_value', 'SELECT key, value FROM source_data_view', '/output/job2', NULL, NULL, NULL
        |)
      """.stripMargin)
    stmt.close()

    val configsTry = JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName)

    configsTry.isSuccess should be (true)
    val configs = configsTry.get

    configs should have length 2

    val job1Config = configs.find(_.jobName == "job1").get
    job1Config.sourceType should be ("FILE")
    job1Config.sourcePath should be ("/path/file.csv")
    job1Config.targetTable should be ("schema.target1")
    job1Config.primaryKeyColumns should contain theSameElementsAs List("id", "name")
    job1Config.sourceToTargetFlag should be ("Y")
    job1Config.businessRuleFlag should be ("N")
    job1_config.columnMappingString should be ("id:pk_id,name:full_name")
    job1Config.parsedColumnMappings.map(cm => s"${cm.sourceColumn}:${cm.targetColumn}") should contain theSameElementsAs List("id:pk_id", "name:full_name")
    job1Config.businessRuleSQL should be (None)
    job1Config.outputPath should be ("/output/job1")
    job1Config.fileFormat should be (Some("csv"))
    job1Config.fileDelimiter should be (Some(","))
    job1Config.fileHasHeader should be (Some(true))

    val job2Config = configs.find(_.jobName == "job2").get
    job2Config.jobName should be ("job2")
    job2Config.sourceType should be ("HIVE")
    job2Config.runSourceToTarget should be (false)
    job2Config.runBusinessRules should be (true)
    job2Config.businessRuleSQL should be (Some("SELECT key, value FROM source_data_view"))
    job2Config.primaryKeyColumns should contain theSameElementsAs List("key")

  }

  it should "return an empty list if the config table is empty" in {
    val configsTry = JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName)
    configsTry.isSuccess should be (true)
    configsTry.get should be (empty)
  }

  it should "handle SQL errors gracefully" in {
    // Dropping a required column to cause an error during fetch
    val stmt = h2Connection.createStatement()
    stmt.executeUpdate(s"ALTER TABLE $configTableName DROP COLUMN sourceType")
    stmt.close()

    val configsTry = JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName)
    configsTry.isFailure should be (true)
    configsTry.failed.get shouldBe a [java.sql.SQLException]

    // Recreate table for other tests
    beforeAll()
  }

  it should "skip rows with missing non-nullable fields like primaryKeyColumns if they were nullable in DB" in {
    // Insert data with null primaryKeyColumns (assuming DB schema allowed it by mistake)
    val stmt = h2Connection.createStatement()
     stmt.executeUpdate(
      s"""
        |INSERT INTO $configTableName (jobName, sourceType, sourcePath, targetTable, primaryKeyColumns,
        |sourceToTargetFlag, businessRuleFlag, columnMapping, outputPath) VALUES (
        |  'job_bad_pk', 'FILE', '/path/file.csv', 'schema.target1', NULL,
        |  'Y', 'N', 'id:pk_id', '/output/job_bad_pk'
        |)
      """.stripMargin)
    stmt.executeUpdate(
      s"""
        |INSERT INTO $configTableName VALUES (
        |  'job_good_pk', 'HIVE', 'db.source_table', 'schema.target2', 'key', 'N', 'Y',
        |  'value:data_value', 'SELECT key, value FROM source_data_view', '/output/job2', NULL, NULL, NULL
        |)
      """.stripMargin)
    stmt.close()

    // The ReconJobConfig constructor requires primaryKeyColumns.
    // JdbcUtil's parsing logic for the row would fail for 'job_bad_pk' and log an error.
    // It should still return 'job_good_pk'.
    val configsTry = JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName)
    configsTry.isSuccess should be (true)
    val configs = configsTry.get

    configs should have length 1
    configs.head.jobName should be ("job_good_pk")
  }


  it should "reuse existing connection" in {
    // First call establishes connection
    JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName).isSuccess should be (true)

    // How to assert connection is reused?
    // For this simple implementation, we can't easily inspect the private `connection` variable from here.
    // We can infer it by checking logs or by ensuring a second call doesn't fail if the DB was (conceptually) single-connection.
    // Or, if the getConnection method logged "Successfully established new JDBC connection." only once.
    // This test is more conceptual for the current JdbcUtil design.
    // A more complex JdbcUtil with a proper pool would have metrics for this.

    // Second call
    JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName).isSuccess should be (true)
    // If connection pooling was more advanced, we'd check pool stats.
    // For now, just ensuring it works is the main check.
  }

  it should "establish a new connection if the existing one is closed or invalid" in {
     // First call establishes connection
    JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName).isSuccess should be (true)

    // Manually close the connection that JdbcUtil is holding (simulating network drop)
    // This is tricky as the connection is private. We rely on JdbcUtil.closeConnection()
    JdbcUtil.closeConnection()

    // Second call should re-establish
    val configsTry = JdbcUtil.fetchReconJobConfigs(testDbConfig, configTableName)
    configsTry.isSuccess should be (true)
    // We expect logs to show a new connection being established.
  }

}
