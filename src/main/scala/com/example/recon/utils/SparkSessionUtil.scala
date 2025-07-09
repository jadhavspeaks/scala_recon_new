package com.example.recon.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkSessionUtil extends LazyLogging {

  @transient private var sparkSession: Option[SparkSession] = None

  /**
   * Gets an existing SparkSession or creates a new one if none exists.
   * The SparkSession is configured with Hive support enabled.
   *
   * @param appName The name for the Spark application.
   * @param master  The Spark master URL (e.g., "local[*]", "yarn").
   *                Defaults to "local[*]" if not provided or empty.
   * @return An instance of SparkSession.
   */
  def getSparkSession(appName: String = "ReconciliationFramework", master: String = "local[*]"): SparkSession = {
    sparkSession.filterNot(_.sparkContext.isStopped).getOrElse {
      logger.info(s"Creating new SparkSession for app: $appName with master: $master")
      try {
        val effectiveMaster = if (master == null || master.trim.isEmpty) "local[*]" else master

        val session = SparkSession.builder()
          .appName(appName)
          .master(effectiveMaster)
          .enableHiveSupport() // Enable Hive support for reading/writing Hive tables
          // Add any other Spark configurations needed globally
          // .config("spark.sql.shuffle.partitions", "200") // Example configuration
          // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // For performance
          .getOrCreate()

        logger.info(s"SparkSession created successfully. Spark version: ${session.version}")
        sparkSession = Some(session)
        session
      } catch {
        case e: Exception =>
          logger.error(s"Failed to create SparkSession: ${e.getMessage}", e)
          throw new RuntimeException("Failed to initialize SparkSession", e)
      }
    }
  }

  /**
   * Stops the current SparkSession if it exists and is active.
   */
  def stopSparkSession(): Unit = {
    sparkSession.foreach { session =>
      if (!session.sparkContext.isStopped) {
        logger.info("Stopping SparkSession.")
        session.stop()
        logger.info("SparkSession stopped.")
      }
      sparkSession = None
    }
  }

  // Example of how to add runtime configurations if needed
  // def setSparkConf(spark: SparkSession, key: String, value: String): Unit = {
  //   spark.conf.set(key, value)
  //   logger.info(s"Spark configuration set: $key = $value")
  // }
}
