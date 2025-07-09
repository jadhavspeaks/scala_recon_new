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

class SourceToTargetReconJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient var spark: SparkSession = _
  val tempDir: Path = Files.createTempDirectory("s2tJobTest")

  override def beforeAll(): Unit = {
    spark = SparkSessionUtil.getSparkSession(appName = "S2TJobSpec", master = "local[2]")
    spark.sparkContext.setLogLevel("WARN") // Reduce verbosity during tests

    // Create dummy Hive target table for tests (if Hive is properly configured for local testing)
    // For simplicity, we'll often mock the readTargetData if full Hive setup is too complex for unit tests.
    // Here, let's try to create a simple one.
    Try {
      spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
      spark.sql("USE test_db")
      // Define a schema for a target table to be created by tests if needed
    }
  }

  override def afterAll(): Unit = {
    SparkSessionUtil.stopSparkSession()
    Try(new Directory(tempDir.toFile).deleteRecursively())
    Try {
        spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    }
  }

  override def beforeEach(): Unit = {
    // Clean up tables or views from previous test if necessary
    Try(spark.sql("DROP TABLE IF EXISTS test_db.target_table_s2t"))
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

  "SourceToTargetReconJob" should "perform reconciliation between a source file and a target Hive table" in {
    import spark.implicits._

    // 1. Setup Source Data (CSV file)
    val sourceCsvPath = createTempCsvFile("source_s2t.csv",
      """id,name,value,extra_src_col
        |1,Alice,100,src_extra1
        |2,Bob,200,src_extra2
        |3,Charlie,300,src_extra3
        |4,David,400,src_extra4 """.stripMargin) // 4 is source_only

    // 2. Setup Target Data (Hive table)
    val targetSchema = StructType(Seq(
      StructField("pk_id", IntegerType, nullable = false),
      StructField("full_name", StringType, nullable = true),
      StructField("amount", IntegerType, nullable = true),
      StructField("extra_tgt_col", StringType, nullable = true)
    ))
    val targetData = Seq(
      Row(1, "Alice", 100, "tgt_extra1"),    // Matched
      Row(2, "Robert", 200, "tgt_extra2"),  // Mismatched (name)
      // Row for id=3 is missing in target (Charlie)
      Row(5, "Eve", 500, "tgt_extra5")       // Target only
    )
    createTargetTable("test_db.target_table_s2t", targetData, targetSchema)

    // 3. Setup ReconJobConfig
    val config = ReconJobConfig(
      jobName = "s2t_test_job",
      sourceType = "FILE",
      sourcePath = sourceCsvPath,
      targetTable = "test_db.target_table_s2t",
      primaryKeyColumns = List("id"), // This 'id' should map to 'pk_id' in target via columnMapping
      sourceToTargetFlag = "Y",
      businessRuleFlag = "N",
      columnMappingString = "id:pk_id,name:full_name,value:amount", // value maps to amount
      businessRuleSQL = None,
      outputPath = tempDir.resolve("output_s2t").toString,
      fileFormat = Some("csv"),
      fileDelimiter = Some(","),
      fileHasHeader = Some(true)
    )

    // 4. Run the job
    val job = new SourceToTargetReconJob()
    val result = job.run(spark, config)

    // 5. Assertions
    result.status should be ("SUCCESS")

    // Summary assertions
    val summaryDF = result.summary.orderBy("recon_status")
    summaryDF.show(false)
    val summaryMap = summaryDF.as[(String, Long, String, java.sql.Timestamp)] // Adjust if job_name/timestamp are different
                         .collect()
                         .map(r => r._1 -> r._2) // (status, count)
                         .toMap

    summaryMap.getOrElse("MATCHED", 0L) should be (1)      // Record id=1 (Alice)
    summaryMap.getOrElse("MISMATCHED", 0L) should be (1)   // Record id=2 (Bob vs Robert)
    summaryMap.getOrElse("SOURCE_ONLY", 0L) should be (2)  // Records id=3 (Charlie), id=4 (David)
    summaryMap.getOrElse("TARGET_ONLY", 0L) should be (1)  // Record id=5 (Eve)

    // Detailed Mismatches assertions
    result.detailedMismatches shouldNot be (None)
    val detailsDF = result.detailedMismatches.get
    detailsDF.count() should be (1) // Only Bob/Robert

    val mismatchRow = detailsDF.filter($"pk_id" === 2).head() // Using target PK col name from mapping
    mismatchRow.getAs[String]("name_source") should be ("Bob")
    mismatchRow.getAs[String]("full_name_target") should be ("Robert")
    mismatchRow.getAs[String]("value_source") should be ("200") // Spark CSV reader might make it string
    mismatchRow.getAs[Int]("amount_target") should be (200)

    val mismatchDetailsJson = mismatchRow.getAs[String]("mismatch_details")
    mismatchDetailsJson should include ("\"name_vs_full_name_match_status\":\"false\"")
    mismatchDetailsJson should include ("\"name_source_value\":\"Bob\"")
    mismatchDetailsJson should include ("\"full_name_target_value\":\"Robert\"")
    mismatchDetailsJson should include ("\"value_vs_amount_match_status\":\"true\"") // value 200 vs amount 200 should match
  }

  it should "handle cases with no column mappings (only PKs)" in {
    import spark.implicits._
    val sourceCsvPath = createTempCsvFile("source_pk_only.csv", "id\n1\n2")
    val targetSchema = StructType(Seq(StructField("id", IntegerType, false)))
    val targetData = Seq(Row(1), Row(3))
    createTargetTable("test_db.target_table_s2t_pk_only", targetData, targetSchema)

    val config = ReconJobConfig(
      jobName = "s2t_pk_only_job", sourceType = "FILE", sourcePath = sourceCsvPath,
      targetTable = "test_db.target_table_s2t_pk_only", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "Y", businessRuleFlag = "N", columnMappingString = "", // No explicit mappings
      businessRuleSQL = None, outputPath = tempDir.resolve("output_s2t_pk").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new SourceToTargetReconJob()
    val result = job.run(spark, config)
    result.status should be ("SUCCESS")
    val summaryMap = result.summary.as[(String, Long, String, java.sql.Timestamp)].collect().map(r => r._1 -> r._2).toMap

    summaryMap.getOrElse("MATCHED", 0L) should be (1)      // id=1
    summaryMap.getOrElse("SOURCE_ONLY", 0L) should be (1)  // id=2
    summaryMap.getOrElse("TARGET_ONLY", 0L) should be (1)  // id=3
    result.detailedMismatches.get.count() should be (0) // No mismatches as no columns were compared
  }

  it should "return FAILURE if source file does not exist" in {
    val config = ReconJobConfig(
      jobName = "s2t_no_src_file", sourceType = "FILE", sourcePath = "/tmp/non_existent_file.csv",
      targetTable = "test_db.target_table_s2t", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "Y", businessRuleFlag = "N", columnMappingString = "id:id",
      businessRuleSQL = None, outputPath = tempDir.resolve("output_s2t_fail").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new SourceToTargetReconJob()
    val result = job.run(spark, config)
    result.status should be ("FAILURE")
    result.message.get should include ("Path does not exist") // Or similar Spark message
  }

  it should "return FAILURE if target table does not exist" in {
    val sourceCsvPath = createTempCsvFile("source_no_tgt.csv", "id\n1")
    val config = ReconJobConfig(
      jobName = "s2t_no_tgt_tbl", sourceType = "FILE", sourcePath = sourceCsvPath,
      targetTable = "test_db.non_existent_target_table", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "Y", businessRuleFlag = "N", columnMappingString = "id:id",
      businessRuleSQL = None, outputPath = tempDir.resolve("output_s2t_fail2").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new SourceToTargetReconJob()
    val result = job.run(spark, config)
    result.status should be ("FAILURE")
    result.message.get.toLowerCase should (include ("not found") or include("non_existent_target_table"))
  }

  it should "return FAILURE if PK columns are missing in source" in {
    val sourceCsvPath = createTempCsvFile("source_missing_pk.csv", "name,value\nAlice,100") // 'id' PK is missing
    val targetSchema = StructType(Seq(StructField("id", IntegerType, false)))
    createTargetTable("test_db.target_table_s2t_pk_missing", Seq(Row(1)), targetSchema)

    val config = ReconJobConfig(
      jobName = "s2t_missing_src_pk", sourceType = "FILE", sourcePath = sourceCsvPath,
      targetTable = "test_db.target_table_s2t_pk_missing", primaryKeyColumns = List("id"),
      sourceToTargetFlag = "Y", businessRuleFlag = "N", columnMappingString = "value:value_col",
      businessRuleSQL = None, outputPath = tempDir.resolve("output_s2t_fail3").toString,
      fileFormat = Some("csv"), fileDelimiter = Some(","), fileHasHeader = Some(true)
    )
    val job = new SourceToTargetReconJob()
    val result = job.run(spark, config)
    result.status should be ("FAILURE")
    result.message.get should include ("Source DataFrame for job s2t_missing_src_pk is missing required columns: id")
  }
}
