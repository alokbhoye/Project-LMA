CREATE DATABASE DW_LMA;
CREATE SCHEMA D2_SCHEMA_LMA;

CREATE OR REPLACE TABLE DIM_BORROWER (
    SK_BORROWER      NUMBER ,
    BORROWER_ID      VARCHAR(20),    
    FIRST_NAME       VARCHAR(50),
    LAST_NAME        VARCHAR(50),
    EMAIL            VARCHAR(100),
    PHONE            VARCHAR(20),
    SEGMENT          VARCHAR(20),
    CITY             VARCHAR(50),
    STATE            VARCHAR(2),
    COUNTRY          VARCHAR(50),
    DOB              DATE,
    EMPLOYMENT_TYPE  VARCHAR(50),    
    EFF_START_DATE   TIMESTAMP_NTZ,       
    EFF_END_DATE     TIMESTAMP_NTZ,       
    IS_CURRENT       CHAR(1)               
);

SELECT * FROM DIM_BORROWER;
truncate table dim_borrower;
truncate table dim_branch;
truncate table dim_date;
truncate table dim_loan;
truncate table fact_loan_txn;

CREATE OR REPLACE TABLE DIM_BRANCH(
    SK_BRANCH       NUMBER,    
    BRANCH_ID       VARCHAR(20),    
    BRANCH_NAME     VARCHAR(100),
    REGION          VARCHAR(20),
    CITY            VARCHAR(50),
    STATE           VARCHAR(2),     
    COUNTRY         VARCHAR(50),       
    OPEN_DATE       DATE,
    STATUS          VARCHAR(20)
);

SELECT * FROM DIM_BRANCH;

---------------------------------------------------

CREATE OR REPLACE TABLE DIM_LOAN(
    SK_LOAN INT PRIMARY KEY,
    LOAN_ID            STRING,
    LOAN_TYPE          STRING,
    PRINCIPAL_AMOUNT   NUMBER(10,2),
    INTEREST_RATE      NUMBER(10,2),
    TENURE_MONTHS      NUMBER(10),
    DISBURSE_DATE      DATE,
    LOAN_STATUS        STRING,
    SECURITY_TYPE      STRING,
    EFF_START         TIMESTAMP_NTZ,
    EFF_END            TIMESTAMP_LTZ,
    IS_CURRENT   VARCHAR(5)
);

SELECT * FROM DIM_LOAN;

------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_DATE (
  SK_DATE       NUMBER(8,0),
  DATE_VALUE    DATE,
  YEAR          NUMBER(4,0),
  QUARTER       NUMBER(1,0),
  MONTH         NUMBER(2,0),
  WEEK_OF_YEAR  NUMBER(2,0),
  IS_WEEKEND    STRING
);

SELECT * FROM DIM_DATE;

---------------------------------------------------------------------

CREATE OR REPLACE TABLE FACT_LOAN_TXN (
    TXN_ID              VARCHAR(50),
    TXN_LINE_ID         VARCHAR(10),    
    SK_DATE             NUMBER(38,0),  -- Links to DIM_DATE
    SK_BORROWER         NUMBER(38,0),  -- Links to DIM_BORROWER
    SK_LOAN             NUMBER(38,0),  -- Links to DIM_LOAN
    SK_BRANCH           NUMBER(38,0),  -- Links to DIM_BRANCH    
    AMOUNT              NUMBER(18,2),
    INTEREST_COMPONENT  NUMBER(18,2),
    FEE_COMPONENT       NUMBER(18,2),    
    TXN_TYPE            VARCHAR(50),  -- e.g., 'Disbursement', 'Repayment'
    PAYMENT_MODE        VARCHAR(50),  -- e.g., 'UPI', 'NEFT'
    TXN_STATUS          VARCHAR(20)
);

SELECT * FROM FACT_LOAN_TXN;

truncate table dim_borrower;
truncate table dim_branch;
truncate table dim_date;
truncate table dim_loan;

truncate table fact_loan_txn;



-------------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE V_LOAN_ANALYTICS_BASE
TARGET_LAG = '1 hour'
warehouse = 'COMPUTE_WH'
AS
SELECT
    d.DATE_VALUE AS TXN_DATE,   -- The code now looks for TXN_DATE or DATE_VALUE
    d.YEAR,
    d.MONTH,
    d.QUARTER,
    br.REGION,
    br.BRANCH_NAME,
    br.STATE AS BRANCH_STATE,
    l.LOAN_TYPE,
    l.SECURITY_TYPE,
    l.INTEREST_RATE,
    b.SEGMENT AS BORROWER_SEGMENT,
    b.EMPLOYMENT_TYPE,
    b.CITY AS BORROWER_CITY,
    f.TXN_ID,
    f.TXN_TYPE,
    f.PAYMENT_MODE,
    f.TXN_STATUS,
    f.AMOUNT,
    f.INTEREST_COMPONENT,
    f.FEE_COMPONENT
FROM FACT_LOAN_TXN f
JOIN DIM_DATE d ON f.SK_DATE = d.SK_DATE
JOIN DIM_BRANCH br ON f.SK_BRANCH = br.SK_BRANCH
JOIN DIM_LOAN l ON f.SK_LOAN = l.SK_LOAN
JOIN DIM_BORROWER b ON f.SK_BORROWER = b.SK_BORROWER;

select * from V_LOAN_ANALYTICS_BASE;


---------------------------------------------------------------------------------------

-- cortex

CREATE TABLE IF NOT EXISTS DOC_KNOWLEDGE_BASE (
    CHUNK_ID VARCHAR(50),
    DOC_TYPE VARCHAR(50),      -- e.g., 'Credit Policy', 'SOP'
    TITLE VARCHAR(100),        -- Section Title
    CHUNK_TEXT VARCHAR,        -- The actual content (500-1000 tokens)
    CREATED_ON TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create the Cortex Search Service (The "Indexer")
-- Note: Requires Cortex Search privilege.
CREATE OR REPLACE CORTEX SEARCH SERVICE LOAN_POLICY_SEARCH
ON CHUNK_TEXT
ATTRIBUTES DOC_TYPE, TITLE
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT CHUNK_TEXT, DOC_TYPE, TITLE
    FROM DOC_KNOWLEDGE_BASE
);

-- ==========================================
-- 2. SETUP FOR CORTEX ANALYST (NL2SQL) [cite: 17]
-- ==========================================
CREATE OR REPLACE VIEW V_DISBURSEMENTS_DAILY AS
SELECT 
    TXN_DATE AS DATE_VALUE, 
    BRANCH_NAME, 
    LOAN_TYPE, 
    SUM(AMOUNT) as TOTAL_DISBURSED
FROM V_LOAN_ANALYTICS_BASE
WHERE TXN_TYPE = 'DISBURSEMENT'
GROUP BY 1, 2, 3;
select * from v_disbursements_daily;

-- Fix: Select TXN_DATE and alias it as DATE_VALUE
CREATE OR REPLACE VIEW V_COLLECTIONS_DAILY AS
SELECT 
    TXN_DATE AS DATE_VALUE, 
    BRANCH_NAME, 
    PAYMENT_MODE, 
    SUM(AMOUNT) as TOTAL_COLLECTED
FROM V_LOAN_ANALYTICS_BASE
WHERE TXN_TYPE = 'EMI_PAYMENT' AND TXN_STATUS = 'Success'
GROUP BY 1, 2, 3;
select * from v_loan_analytics_base;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- 1. Enable Cortex Functions for your role
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;

-- 2. Grant usage on the specific search service you created
GRANT USAGE ON CORTEX SEARCH SERVICE LOAN_POLICY_SEARCH TO ROLE ACCOUNTADMIN;

-- 3. Grant access to the underlying knowledge base table
GRANT SELECT ON TABLE DOC_KNOWLEDGE_BASE TO ROLE ACCOUNTADMIN;


---------------------------------------------------------------------

-- MASKING

-- 1. Create the Masked Role (General Access)
CREATE ROLE IF NOT EXISTS LOAN_ANALYST_MASKED;

-- 2. Create the Unmasked Role (Privileged Access)
CREATE ROLE IF NOT EXISTS LOAN_ANALYST_FULL;

-- 3. Grant usage on your database/schema to BOTH roles
GRANT USAGE ON DATABASE DW_LMA TO ROLE LOAN_ANALYST_MASKED;
GRANT USAGE ON DATABASE DW_LMA TO ROLE LOAN_ANALYST_FULL;
GRANT USAGE ON SCHEMA D2_SCHEMA_LMA TO ROLE LOAN_ANALYST_MASKED;
GRANT USAGE ON SCHEMA D2_SCHEMA_LMA TO ROLE LOAN_ANALYST_FULL;

-- 4. Grant Select access to BOTH roles (Masking happens on top of this)
GRANT SELECT ON TABLE DIM_BORROWER TO ROLE LOAN_ANALYST_MASKED;
GRANT SELECT ON TABLE DIM_BORROWER TO ROLE LOAN_ANALYST_FULL;

-- Create a policy for String columns (Names, Emails)
CREATE OR REPLACE MASKING POLICY PII_MASK_STRING AS (val string) RETURNS string ->
  CASE
    -- Whitelist the FULL role to see real data
    WHEN CURRENT_ROLE() IN ('LOAN_ANALYST_FULL', 'ACCOUNTADMIN', 'SYSADMIN') THEN val
    
    -- Everyone else (including LOAN_ANALYST_MASKED) sees this:
    ELSE '***MASKED***'
  END;

-- Create a policy for Phone Numbers (Partial Masking)
CREATE OR REPLACE MASKING POLICY PII_MASK_PHONE AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('LOAN_ANALYST_FULL', 'ACCOUNTADMIN', 'SYSADMIN') THEN val
    
    -- Mask everything except the last 4 digits
    ELSE CONCAT('******', RIGHT(val, 4))
  END;

SELECT * FROM DIM_BORROWER;
-- Apply Full Mask to Name
ALTER TABLE DIM_BORROWER 
MODIFY COLUMN FIRST_NAME SET MASKING POLICY PII_MASK_STRING;

ALTER TABLE DIM_BORROWER 
MODIFY COLUMN LAST_NAME SET MASKING POLICY PII_MASK_STRING;


-- Apply Full Mask to Email
ALTER TABLE DIM_BORROWER 
MODIFY COLUMN EMAIL SET MASKING POLICY PII_MASK_STRING;

-- Apply Partial Mask to Phone
ALTER TABLE DIM_BORROWER 
MODIFY COLUMN PHONE SET MASKING POLICY PII_MASK_PHONE;

GRANT ROLE LOAN_ANALYST_FULL TO USER alokbhoye23;
GRANT ROLE LOAN_ANALYST_MASKED TO USER alokbhoye23;
USE ROLE LOAN_ANALYST_MASKED;

SELECT * FROM DIM_BORROWER;

USE ROLE ACCOUNTADMIN;

SELECT * FROM DIM_BORROWER;
