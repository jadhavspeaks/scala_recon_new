-- Oracle DDL for the Reconciliation Job Configuration Table

CREATE TABLE RECON_JOB_CONFIG (
    JOB_NAME                VARCHAR2(100) NOT NULL PRIMARY KEY,
    SOURCE_TYPE             VARCHAR2(20)  NOT NULL, -- e.g., 'FILE' or 'TABLE'
    SOURCE_PATH             VARCHAR2(1000),         -- For file-based sources
    SOURCE_TABLE            VARCHAR2(100),          -- For table-based sources
    TARGET_TABLE            VARCHAR2(100) NOT NULL,
    PRIMARY_KEY_COLUMNS     VARCHAR2(500) NOT NULL, -- Comma-separated list, e.g., 'id,region_code'
    COLUMN_MAPPING_STRING   VARCHAR2(4000),         -- Comma-separated pairs, e.g., 'src_col:tgt_col,src_col2:tgt_col2'
    SOURCE_TO_TARGET_FLAG   CHAR(1) DEFAULT 'N' NOT NULL, -- 'Y' or 'N'
    BUSINESS_RULE_FLAG      CHAR(1) DEFAULT 'N' NOT NULL, -- 'Y' or 'N'
    BUSINESS_RULE_SQL       CLOB,                   -- For storing potentially large SQL queries
    OUTPUT_PATH             VARCHAR2(1000),         -- HDFS or S3 path for results
    FILE_FORMAT             VARCHAR2(20),           -- e.g., 'csv', 'parquet', 'json'
    FILE_DELIMITER          VARCHAR2(5),            -- e.g., ',' or '|'
    FILE_HAS_HEADER         CHAR(1) DEFAULT 'Y',    -- 'Y' or 'N'
    IS_ACTIVE               CHAR(1) DEFAULT 'Y' NOT NULL, -- To enable or disable jobs
    CREATED_TIMESTAMP       TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    LAST_UPDATED_TIMESTAMP  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,

    CONSTRAINT CHK_SOURCE_TYPE CHECK (SOURCE_TYPE IN ('FILE', 'TABLE')),
    CONSTRAINT CHK_S2T_FLAG CHECK (SOURCE_TO_TARGET_FLAG IN ('Y', 'N')),
    CONSTRAINT CHK_BR_FLAG CHECK (BUSINESS_RULE_FLAG IN ('Y', 'N')),
    CONSTRAINT CHK_IS_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N'))
);

-- Add some comments for clarity
COMMENT ON COLUMN RECON_JOB_CONFIG.JOB_NAME IS 'Unique name for the reconciliation job.';
COMMENT ON COLUMN RECON_JOB_CONFIG.PRIMARY_KEY_COLUMNS IS 'Comma-separated list of primary key column names.';
COMMENT ON COLUMN RECON_JOB_CONFIG.COLUMN_MAPPING_STRING IS 'Comma-separated source:target column mappings for comparison.';
COMMENT ON COLUMN RECON_JOB_CONFIG.BUSINESS_RULE_SQL IS 'The SQL query to be executed for a business rule reconciliation.';
COMMENT ON COLUMN RECON_JOB_CONFIG.IS_ACTIVE IS 'Flag to determine if the job is active and should be run.';

-- Optional: Create a trigger to automatically update the LAST_UPDATED_TIMESTAMP
CREATE OR REPLACE TRIGGER TRG_RECON_JOB_CONFIG_UPD
BEFORE UPDATE ON RECON_JOB_CONFIG
FOR EACH ROW
BEGIN
    :NEW.LAST_UPDATED_TIMESTAMP := CURRENT_TIMESTAMP;
END;
/
