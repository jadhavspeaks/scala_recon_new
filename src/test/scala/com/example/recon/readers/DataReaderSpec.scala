package com.example.recon.readers

import com.example.recon.config.ReconJobConfig
import com.example.recon.utils.SparkSessionUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import scala.reflect.io.Directory
import scala.util.Try

class DataReaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient var spark: SparkSession = _
  val tempDir: Path = Files.createTempDirectory("dataReaderTest")

  override def beforeAll(): Unit = {
    spark = SparkSessionUtil.getSparkSession(appName = "DataReaderSpec", master = "local[2]")
    // Create Hive metastore_db and warehouse for Hive tests if needed, or use Spark's default
    // For this basic test, we'll focus on file reads. Hive might need more setup.
  }

  override def afterAll(): Unit = {
    SparkSessionUtil.stopSparkSession()
    // Clean up temp directory
    Try(new Directory(tempDir.toFile).deleteRecursively())
  }

  override def beforeEach(): Unit = {
    // Clean up any files from previous tests within tempDir if necessary,
    // but typically tests create their own uniquely named files.
  }

  private def createTempFile(name: String, content: String): String = {
    val filePath = Paths.get(tempDir.toString, name)
    val writer = new PrintWriter(filePath.toFile)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    filePath.toString
  }

  "DataReader.readFileSource" should "read a CSV file with header and infer schema" in {
    val csvContent =
      """id,name,value
        |1,Alice,100
        |2,Bob,200
      """.stripMargin
    val filePath = createTempFile("test1.csv", csvContent)

    val dfTry = DataReader.readFileSource(spark, filePath, Some("csv"), Some(","), Some(true), None)
    dfTry.isSuccess should be (true)
    val df = dfTry.get
    df.count() should be (2)
    df.columns should contain theSameElementsAs Seq("id", "name", "value")
    // Spark might infer id and value as int or string depending on data. Let's check one.
    df.schema("id").dataType.simpleString should (be ("integer") or be ("string"))
  }

  it should "read a CSV file without header and use default column names" in {
    val csvContent =
      """1,Alice,100
        |2,Bob,200
      """.stripMargin
    val filePath = createTempFile("test2.csv", csvContent)

    val dfTry = DataReader.readFileSource(spark, filePath, Some("csv"), Some(","), Some(false), None)
    dfTry.isSuccess should be (true)
    val df = dfTry.get
    df.count() should be (2)
    df.columns should contain theSameElementsAs Seq("_c0", "_c1", "_c2") // Default column names
  }

  it should "read a JSON file" in {
    val jsonContent =
      """{"id": 1, "name": "Alice", "value": 100}
        |{"id": 2, "name": "Bob", "value": 200}
      """.stripMargin
    val filePath = createTempFile("test3.json", jsonContent)
    val dfTry = DataReader.readFileSource(spark, filePath, Some("json"), None, None, None)
    dfTry.isSuccess should be (true)
    val df = dfTry.get
    df.count() should be (2)
    df.columns should contain theSameElementsAs Seq("id", "name", "value")
    df.schema("id").dataType.simpleString should be ("long") // JSON numbers are typically long
  }

  it should "read a Parquet file" in {
    // Create a dummy DataFrame and save as Parquet
    import spark.implicits._
    val data = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val parquetPath = Paths.get(tempDir.toString, "test4.parquet").toString
    data.write.mode("overwrite").parquet(parquetPath)

    val dfTry = DataReader.readFileSource(spark, parquetPath, Some("parquet"), None, None, None)
    dfTry.isSuccess should be (true)
    val df = dfTry.get
    df.count() should be (2)
    df.columns should contain theSameElementsAs Seq("id", "name")
  }

  it should "read a text file into a single 'value' column" in {
    val textContent =
      """Line 1
        |Line 2
        |Another line
      """.stripMargin
    val filePath = createTempFile("test5.txt", textContent)
    val dfTry = DataReader.readFileSource(spark, filePath, Some("text"), None, None, None)
    dfTry.isSuccess should be(true)
    val df = dfTry.get
    df.count() should be (3)
    df.columns should contain theSameElementsAs Seq("value")
    df.select("value").first().getString(0) should be ("Line 1")
  }

  it should "fail for unsupported file format" in {
    val filePath = createTempFile("test.unsupported", "content")
    val dfTry = DataReader.readFileSource(spark, filePath, Some("unsupported"), None, None, None)
    dfTry.isFailure should be (true)
    dfTry.failed.get.getMessage should include ("Unsupported file format: 'unsupported'")
  }

  it should "fail if file format is not specified" in {
     val filePath = createTempFile("test.noformat", "content")
     val dfTry = DataReader.readFileSource(spark, filePath, None, None, None, None)
     dfTry.isFailure should be (true)
     dfTry.failed.get.getMessage should include ("File format must be specified")
  }

  it should "fail gracefully if file does not exist" in {
    val nonExistentPath = Paths.get(tempDir.toString, "nonexistent.csv").toString
    val dfTry = DataReader.readFileSource(spark, nonExistentPath, Some("csv"), None, None, None)
    dfTry.isFailure should be (true)
    // Spark's error message for missing path can vary. Just check it's a failure.
  }

  "DataReader.readSourceData" should "dispatch to readFileSource for FILE type" in {
    val csvContent = "id,name\n1,Test"
    val filePath = createTempFile("dispatch.csv", csvContent)
    val config = ReconJobConfig("jobFile", "FILE", filePath, "tgt", List("id"), "Y", "N", "id:id", None, "/out", Some("csv"), Some(","), Some(true))

    val dfTry = DataReader.readSourceData(spark, config)
    dfTry.isSuccess should be (true)
    dfTry.get.count() should be (1)
    dfTry.get.columns should contain theSameElementsAs Seq("id", "name")
  }

  it should "dispatch to readHiveTable for HIVE type (stub, actual Hive read not tested here)" in {
    // This test is more of a conceptual check as setting up a Hive instance for unit tests is complex.
    // We are checking that it attempts to call spark.table()
    val config = ReconJobConfig("jobHive", "HIVE", "fakedb.faketable", "tgt", List("id"), "Y", "N", "id:id", None, "/out", None, None, None)

    // We expect this to fail because fakedb.faketable doesn't exist, but it means it tried to read a Hive table.
    val dfTry = DataReader.readSourceData(spark, config)
    dfTry.isFailure should be (true)
    dfTry.failed.get.getMessage.toLowerCase should (include ("not found") or include("fakedb.faketable"))
  }

  it should "fail for unsupported source type in readSourceData" in {
    val config = ReconJobConfig("jobUnsupported", "UNKNOWN_TYPE", "path", "tgt", List("id"), "Y", "N", "id:id", None, "/out", None, None, None)
    val dfTry = DataReader.readSourceData(spark, config)
    dfTry.isFailure should be (true)
    dfTry.failed.get.getMessage should include ("Unsupported source type: 'UNKNOWN_TYPE'")
  }

  "DataReader.readTargetData" should "attempt to read a Hive table (stub)" in {
    val dfTry = DataReader.readTargetData(spark, "fakedb.faketarget")
    dfTry.isFailure should be (true) // Expected, table doesn't exist
    dfTry.failed.get.getMessage.toLowerCase should (include ("not found") or include("fakedb.faketarget"))
  }

}
