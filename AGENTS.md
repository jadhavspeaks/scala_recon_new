# AGENTS.md - Guidelines for AI Agent Development

This document provides guidelines for AI agents working on the Spark Data Reconciliation Framework.

## Project Overview

This framework is designed to perform data reconciliation between various sources (files, Hive tables) and target Hive tables. All reconciliation logic is driven by configurations stored in an Oracle database table.

Key components:
- `com.example.recon.config`: Configuration models (e.g., `ReconJobConfig`).
- `com.example.recon.utils`: Utilities for JDBC, SparkSession.
- `com.example.recon.readers`: Data reading logic (`DataReader`).
- `com.example.recon.writers`: Data writing logic (`DataWriter`).
- `com.example.recon.jobs`: Core reconciliation job implementations (`SourceToTargetReconJob`, `BusinessRuleReconJob`) and the base `ReconciliationJob` trait.
- `com.example.recon.MainApp`: The main entry point and orchestrator.

## Development Conventions

1.  **Scala Version**: Scala 2.13.6
2.  **Spark Version**: Spark 2.4.x (specifically 2.4.8 as per `pom.xml`)
3.  **Logging**:
    *   Use `com.typesafe.scalalogging.LazyLogging` for all classes requiring logging.
    *   Log meaningful messages, including job names or other contextual information where possible.
    *   Default log levels are configured in `src/main/resources/logback.xml`. Modify this file for changes to logging behavior.
4.  **Exception Handling**:
    *   Use `scala.util.Try`, `Success`, `Failure` for operations that can fail (especially I/O like database access, file reads/writes, Spark actions).
    *   Catch specific exceptions where appropriate.
    *   Ensure that individual job failures do not bring down the entire application; the `MainApp` should log the error and attempt to proceed with other jobs.
5.  **Immutability**: Prefer immutable data structures where practical.
6.  **Modularity**: Keep components focused on specific responsibilities. For example, `DataReader` only reads, `DataWriter` only writes.
7.  **Configuration Driven**:
    *   Avoid hardcoding values that can be externalized to the Oracle configuration table.
    *   When adding new features, consider if they can be controlled via new columns in the `ReconJobConfig` and the Oracle table.
8.  **Spark Usage**:
    *   Use `SparkSessionUtil` to obtain Spark sessions.
    *   Alias DataFrame columns clearly, especially before joins, to avoid ambiguity (e.g., `_src_`, `_tgt_`, `_br_`).
    *   Persist DataFrames that are used multiple times and unpersist them when no longer needed.
    *   Use null-safe Spark SQL functions (e.g., `<=>` for comparison, `coalesce`).
9.  **Column Mapping**: The `columnMappingString` in `ReconJobConfig` is parsed into `List[ColumnMapping]`. This mapping is critical for comparisons.
    *   In `SourceToTargetReconJob`: `sourceColumn` maps to a column in the source data, `targetColumn` to a column in the target Hive table.
    *   In `BusinessRuleReconJob`: `sourceColumn` maps to a column in the *output of the `businessRuleSQL`*, `targetColumn` to a column in the target Hive table.
10. **Primary Keys**: `primaryKeyColumns` in `ReconJobConfig` are crucial for joining and identifying records. These must be present in all data sources being compared for a given job.
11. **Output Paths**: `outputPath` in `ReconJobConfig` is a base path. The framework will create subdirectories for each job and job type (e.g., `<outputPath>/<jobName>/s2t_summary/`, `<outputPath>/<jobName>/br_details/`).
12. **Testing**: (Further development needed here)
    *   Add unit tests for utility functions and individual components.
    *   Add integration tests that simulate running jobs with sample data and configurations.
    *   Use an in-memory database like H2 for testing JDBC interactions if possible.

## Future Enhancements / Considerations

*   **Connection Pooling**: The current `JdbcUtil` uses a very basic connection reuse strategy. For robust production use, integrate a proper connection pool like HikariCP.
*   **Execution Metadata Persistence**: The `JobExecutionLog` in `MainApp` is currently just logged to the console. Enhance this to write to a dedicated Oracle table for audit and tracking.
*   **Schema Evolution**: While the framework attempts some generic comparisons (e.g., casting to string), more sophisticated schema drift handling and type compatibility checks could be added.
*   **Dependency Management**: Ensure `pom.xml` is kept clean and dependencies are updated cautiously.
*   **Security**: Database credentials are passed as command-line arguments. For production, consider more secure ways to manage secrets (e.g., environment variables, secrets management tools, Spark's Hadoop credential provider API).

## Before Committing Changes

1.  Ensure the code compiles (`mvn clean compile`).
2.  Run any available tests (`mvn test`). (Currently minimal, needs expansion)
3.  Ensure adherence to the guidelines above.
4.  Update this `AGENTS.md` if new conventions are established or major architectural changes are made.

This file is a living document. Please update it as the project evolves.
