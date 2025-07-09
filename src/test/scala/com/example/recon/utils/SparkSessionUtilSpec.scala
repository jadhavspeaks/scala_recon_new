package com.example.recon.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

class SparkSessionUtilSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    // Ensure Spark session is stopped after all tests in this spec
    SparkSessionUtil.stopSparkSession()
  }

  "SparkSessionUtil" should "create a new SparkSession" in {
    val spark = SparkSessionUtil.getSparkSession("TestApp1", "local[1]")
    spark shouldNot be (null)
    spark.sparkContext.appName should be ("TestApp1")
    spark.sparkContext.master should be ("local[1]")
    // Hive support is enabled by default
    Try(spark.sql("SHOW DATABASES").collect()).isSuccess should be (true)
    SparkSessionUtil.stopSparkSession() // Stop it for the next test
  }

  it should "return an existing SparkSession if one is active" in {
    val spark1 = SparkSessionUtil.getSparkSession("TestApp2", "local[1]")
    val spark2 = SparkSessionUtil.getSparkSession("TestApp2Another", "local[1]") // AppName might change if using getOrCreate

    spark1 should be theSameInstanceAs spark2 // getOrCreate behavior
    spark1.sparkContext.appName should (be ("TestApp2") or be ("TestApp2Another")) // Depending on which getOrCreate call "won"
    SparkSessionUtil.stopSparkSession()
  }

  it should "create a new session if the previous one was stopped" in {
    val spark1 = SparkSessionUtil.getSparkSession("TestApp3", "local[1]")
    SparkSessionUtil.stopSparkSession()
    val spark2 = SparkSessionUtil.getSparkSession("TestApp4", "local[1]")

    spark1 shouldNot be theSameInstanceAs spark2
    spark2.sparkContext.appName should be ("TestApp4")
    SparkSessionUtil.stopSparkSession()
  }

  it should "use default appName and master if not provided" in {
    val spark = SparkSessionUtil.getSparkSession()
    spark shouldNot be (null)
    spark.sparkContext.appName should be ("ReconciliationFramework") // Default app name
    spark.sparkContext.master should startWith ("local[*]") // Default master
    SparkSessionUtil.stopSparkSession()
  }

  it should "allow stopping a session" in {
    val spark = SparkSessionUtil.getSparkSession("TestAppToStop", "local[1]")
    spark.sparkContext.isStopped should be (false)
    SparkSessionUtil.stopSparkSession()
    // Note: Accessing spark.sparkContext after stop might throw error or behave unexpectedly
    // The internal reference in SparkSessionUtil should be None.
    // A good check is that a subsequent call to getSparkSession creates a new one.
    val newSpark = SparkSessionUtil.getSparkSession("NewSessionAfterStop", "local[1]")
    newSpark shouldNot be theSameInstanceAs spark
    newSpark.sparkContext.isStopped should be (false)
    SparkSessionUtil.stopSparkSession()
  }

   it should "handle stopping when no session exists" in {
    // Ensure no session exists initially from other tests
    SparkSessionUtil.stopSparkSession()
    // Calling stop again should not throw an error
    noException should be thrownBy SparkSessionUtil.stopSparkSession()
  }

}
