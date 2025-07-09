package com.example.recon.jobs

import com.example.recon.config.ReconJobConfig
import com.example.recon.utils.SparkSessionUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Try
import java.nio.file.{Files, Path}
import scala.reflect.io.Directory
import java.io.PrintWriter

class BusinessRuleReconJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient var spark: SparkSession = _
  val tempDir: Path = Files.createTempDirectory("brJobTest")

  override def beforeAll(): Unit = {
    spark = SparkSessionUtil.getSparkSession(appName = "BRJobSpec", master = "local[2]")
    spark.sparkContext.setLogLevel("WARN")
    Try(spark.sql("CREATE DATABASE IF NOT EXISTS test_br_db"))
    Try(spark.sql("USE test_br_db"))
  }

  override def afterAll(): Unit = {
    SparkSessionUtil.stopSparkSession()
    Try(new Directory(tempDir.toFile).deleteRecursively())
    Try(spark.sql("DROP DATABASE IF EXISTS test_br_db CASCADE"))
  }

  override def beforeEach(): Unit = {
    Try(spark.sql("DROP TABLE IF EXISTS test_br_db.target_table_br"))
    Try(spark.catalog.dropTempView("source_data_view"))
  }

  private def createTempCsvFile(name: String, content: String): String = {
    val filePath = tempDir.resolve(name)
    val writer = new PrintWriter(filePath.toFile)
    try writer.write(content) finally writer.close()
    filePath.toString
  }

  private def createTargetTable(tableName: String, data: Seq[Row], schema: StructType): Unit = {
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)
    df.write.mode("overwrite").saveAsTable(tableName)
  }

  "BusinessRuleReconJob" should "perform reconciliation between business rule output and target" in {
    import spark.implicits._

    // 1. Setup Source Data (CSV file) that the business rule SQL will run on
    val sourceCsvPath = createTempCsvFile("source_br.csv",
      """acc_id,first_name,last_name,balance
        |101,Alice,Wonder,1500
        |102,Bob,Builder,250
        |103,Charlie,Brown,3000
        |104,David,Copper,50 """.stripMargin) // David will be filtered by BR

    // 2. Setup Target Data (Hive table) - This is what the BR output is compared against
    val targetSchema = StructType(Seq(
      StructField("account_id", IntegerType, nullable = false),
      StructField("customer_name", StringType, nullable = true),
      StructField("is_high_value", BooleanType, nullable = true)
    ))
    val targetData = Seq(
      Row(101, "Alice W.", true),         // Matched (name slightly different, is_high_value matches BR)
      Row(102, "Bob Builder", false),    // Matched (is_high_value matches BR)
      Row(103, "Charles Brown", false), // Mismatched (name, is_high_value BR=true, target=false)
      Row(105, "Eve Online", true)       // Target only (not in BR output)
    )
    createTargetTable("test_br_db.target_table_br", targetData, targetSchema)

    // 3. Setup ReconJobConfig
    val businessRuleSQL =
      """
        |SELECT acc_id AS br_account_id,
        |       CONCAT(first_name, ' ', SUBSTR(last_name, 1, 1), '.') AS br_customer_name,
        |       (balance > 1000) AS br_is_high_value
        |FROM source_data_view
        |WHERE balance > 100 -- Filter out David
      """.stripMargin
      // Expected output of BR SQL:
      // 101, Alice W., true
      // 102, Bob B., false
      // 103, Charlie B., true

    val config = ReconJobConfig(
      jobName = "br_test_job",
      sourceType = "FILE",
      sourcePath = sourceCsvPath, // This is for the initial source read
      targetTable = "test_br_db.target_table_br", // This is compared against BR output
      primaryKeyColumns = List("br_account_id"), // PK column from the *business rule SQL output*
      sourceToTargetFlag = "N",
      businessRuleFlag = "Y",
      // `sourceColumn` here refers to columns from businessRuleSQL output
      columnMappingString = "br_account_id:account_id,br_customer_name:customer_name,br_is_high_value:is_high_value",
      businessRuleSQL = Some(businessRuleSQL),
      outputPath = tempDir.resolve("output_br").toString,
      fileFormat = Some("csv"),
      fileDelimiter = Some(","),
      fileHasHeader = Some(true)
    )

    // 4. Run the job
    val job = new BusinessRuleReconJob()
    val result = job.run(spark, config)

    // 5. Assertions
    result.status should be ("SUCCESS")

    val summaryDF = result.summary.orderBy("recon_status")
    summaryDF.show(false)
    val summaryMap = summaryDF.as[(String, Long, String, java.sql.Timestamp)]
                         .collect()
                         .map(r => r._1 -> r._2)
                         .toMap

    // BR Output: (101, Alice W., true), (102, Bob B., false), (103, Charlie B., true)
    // Target:    (101, Alice W., true), (102, Bob Builder, false), (103, Charles Brown, false), (105, Eve Online, true)

    summaryMap.getOrElse("MATCHED_BR_TARGET", 0L) should be (1)      // 101 (Alice) - name matches after BR SQL, high_value matches
    summaryMap.getOrElse("MISMATCHED_BR_TARGET", 0L) should be (2)   // 102 (Bob B. vs Bob Builder), 103 (Charlie B. vs Charles Brown AND true vs false)
    summaryMap.getOrElse("BUSINESS_RULE_ONLY", 0L) should be (0)    // All BR output rows have corresponding PKs in target for this test
    summaryMap.getOrElse("TARGET_ONLY_VS_BR", 0L) should be (1)      // 105 (Eve)

    // Detailed Mismatches
    result.detailedMismatches shouldNot be (None)
    val detailsDF = result.detailedMismatches.get.orderBy("account_id") // PK from target mapping
    detailsDF.count() should be (2)

    // Check mismatch for 102
    val mismatch102 = detailsDF.filter($"account_id" === 102).head()
    mismatch102.getAs[String]("br_customer_name_business_rule_output") should be ("Bob B.")
    mismatch102.getAs[String]("customer_name_target") should be ("Bob Builder")
    mismatch102.getAs[Boolean]("br_is_high_value_business_rule_output") should be (false)
    mismatch102.getAs[Boolean]("is_high_value_target") should be (false)
    val detailsJson102 = mismatch102.getAs[String]("mismatch_details")
    detailsJson102 should include ("\"br_customer_name_vs_customer_name_match_status\":\"false\"")
    detailsJson102 should include ("\"br_is_high_value_vs_is_high_value_match_status\":\"true\"") // This specific pair matches

    // Check mismatch for 103
    val mismatch103 = detailsDF.filter($"account_id" === 103).head()
    mismatch103.getAs[String]("br_customer_name_business_rule_output") should be ("Charlie B.")
    mismatch103.getAs[String]("customer_name_target") should be ("Charles Brown")
    mismatch103.getAs[Boolean]("br_is_high_value_business_rule_output") should be (true) // from balance > 1000
    mismatch103.getAs[Boolean]("is_high_value_target") should be (false)
    val detailsJson103 = mismatch103.getAs[String]("mismatch_details")
    detailsJson103 should include ("\"br_customer_name_vs_customer_name_match_status\":\"false\"")
    detailsJson103 should include ("\"br_is_high_value_vs_is_high_value_match_status\":\"false\"")
  }

  it should "return SKIPPED if businessRuleSQL is not provided" in {
    val config = ReconJobConfig(
      jobName = "br_skip_job", sourceType = "FILE", sourcePath = "dummy.csv",
      targetTable = "test_br_db.target_table_br", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "N", businessRuleFlag = "Y", // Flag is Y
      columnMappingString = "id:id", businessRuleSQL = None, // SQL is None
      outputPath = tempDir.resolve("output_br_skip").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    // Need to create a dummy source file for DataReader to not fail before the BR check
    createTempCsvFile("dummy.csv", "id\n1")

    val job = new BusinessRuleReconJob()
    val result = job.run(spark, config)
    result.status should be ("SKIPPED")
    result.message.get should include ("BusinessRuleSQL is not defined or empty")
  }

  it should "return FAILURE if businessRuleSQL is invalid" in {
     val sourceCsvPath = createTempCsvFile("source_br_invalid_sql.csv", "id,val\n1,10")
     val config = ReconJobConfig(
      jobName = "br_invalid_sql_job", sourceType = "FILE", sourcePath = sourceCsvPath,
      targetTable = "test_br_db.target_table_br", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "N", businessRuleFlag = "Y",
      columnMappingString = "id:id", businessRuleSQL = Some("SELECT non_existent_col FROM source_data_view"),
      outputPath = tempDir.resolve("output_br_invalid_sql").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new BusinessRuleReconJob()
    val result = job.run(spark, config)
    result.status should be ("FAILURE")
    result.message.get.toLowerCase should (include ("cannot resolve") or include("non_existent_col"))
  }

   it should "return FAILURE if PK column from BR SQL output is missing" in {
    val sourceCsvPath = createTempCsvFile("source_br_missing_pk.csv", "name,value\nAlice,100")
    val targetSchema = StructType(Seq(StructField("account_id", IntegerType, false)))
    createTargetTable("test_br_db.target_table_br_pk_missing", Seq(Row(1)), targetSchema)

    val businessRuleSQL = "SELECT name AS customer_name, value AS amount FROM source_data_view" // Does not select 'br_account_id'

    val config = ReconJobConfig(
      jobName = "br_missing_pk_job", sourceType = "FILE", sourcePath = sourceCsvPath,
      targetTable = "test_br_db.target_table_br_pk_missing",
      primaryKeyColumns = List("br_account_id"), // This PK is expected from BR SQL
      sourceToTargetFlag = "N", businessRuleFlag = "Y",
      columnMappingString = "customer_name:cust_name",
      businessRuleSQL = Some(businessRuleSQL),
      outputPath = tempDir.resolve("output_br_fail_pk").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new BusinessRuleReconJob()
    val result = job.run(spark, config)
    result.status should be ("FAILURE")
    result.message.get should include ("Business Rule output for job br_missing_pk_job is missing required columns (check your businessRuleSQL and columnMapping): br_account_id")
  }

}
