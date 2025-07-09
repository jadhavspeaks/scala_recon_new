package com.example.recon.jobs

import com.example.recon.config.ReconJobConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Represents the result of a reconciliation step.
 *
 * @param summary A DataFrame containing summary statistics (e.g., matched, mismatched, missing counts).
 * @param detailedMismatches An optional DataFrame containing row-level mismatch details.
 *                           This could be None if no mismatches are found or if details are not generated.
 * @param status A string indicating the status of this reconciliation step (e.g., "SUCCESS", "FAILURE", "PARTIAL_SUCCESS").
 * @param message An optional message providing more details about the execution or any issues encountered.
 */
case class ReconResult(
  summary: DataFrame,
  detailedMismatches: Option[DataFrame],
  status: String,
  message: Option[String] = None
)


/**
 * Base trait for all reconciliation jobs.
 * Each specific reconciliation type (e.g., Source-to-Target, Business Rule)
 * will implement this trait.
 */
trait ReconciliationJob {

  /**
   * Executes the reconciliation job.
   *
   * @param spark The active SparkSession.
   * @param config The configuration for this specific reconciliation job.
   * @return A ReconResult encapsulating the outcome of the job.
   *         The implementation should handle exceptions and ensure a ReconResult
   *         is always returned, indicating success or failure appropriately.
   */
  def run(spark: SparkSession, config: ReconJobConfig): ReconResult
}
