CREATE OR REPLACE DATABASE CURATED_DB_LMS;
CREATE OR REPLACE SCHEMA CURATED_SCHEMA_LMS;

CREATE OR REPLACE TABLE BORROWERS (
    BORROWER_ID        STRING,
    FIRST_NAME         STRING,
    LAST_NAME          STRING,
    EMAIL              STRING,
    PHONE              STRING,
    SEGMENT            STRING,
    CITY               STRING,
    STATE              STRING,
    EMPLOYMENT_TYPE    STRING,
    COUNTRY            STRING,
    DOB                DATE,
    UPDATED_AT         TIMESTAMP,
    LOAD_DATE          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE BORROWERS_REJECTS (
    REJECT_ID          NUMBER AUTOINCREMENT,
    BORROWER_ID        STRING,
    FIRST_NAME         STRING,
    LAST_NAME          STRING,
    EMAIL              STRING,
    PHONE              STRING,
    SEGMENT            STRING,
    CITY               STRING,
    STATE              STRING,
    EMPLOYMENT_TYPE    STRING,
    COUNTRY            STRING,
    DOB_RAW            STRING,
    UPDATED_AT      STRING,
    REJECT_REASON      STRING,
    STATUS             NUMBER(1),
    LOAD_DATE          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

select * from borrowers_rejects;
TRUNCATE TABLE BORROWERS_REJECTS;
SELECT * FROM BORROWERS;
truncate table borrowers;

----------------------------------------------------------------------

CREATE OR REPLACE TABLE BRANCHES (
    BRANCH_ID       VARCHAR(20) PRIMARY KEY,
    BRANCH_NAME     VARCHAR(100),
    REGION          VARCHAR(20),       -- Standardized (e.g., 'SOUTH')
    CITY            VARCHAR(50),
    STATE           VARCHAR(2),        -- Standardized ISO Code (e.g., 'TN')
    COUNTRY         VARCHAR(50),       -- Standardized 'India'
    OPEN_DATE       DATE,              -- Real Date Object
    STATUS          VARCHAR(20),       -- Standardized 'ACTIVE'
    ETL_LOAD_DATE   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE BRANCHES_REJECTS (
    BRANCH_ID       VARCHAR(50),       -- Looser length for bad IDs
    BRANCH_NAME     VARCHAR(255),
    REGION          VARCHAR(100),      -- Store raw bad value (e.g. 'sth')
    CITY            VARCHAR(100),
    STATE           VARCHAR(100),      -- Store raw bad value (e.g. 'Tamilnadu')
    COUNTRY         VARCHAR(100),      -- Store raw bad value (e.g. 'IND')
    OPEN_DATE_RAW   VARCHAR(50),       -- Store raw bad date string
    STATUS          VARCHAR(50),
    REJECT_REASON   VARCHAR(2000),     -- Large column for concatenated errors
    ETL_LOAD_DATE   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

select * from branches;
TRUNCATE TABLE BRANCHES;
select * from branches_rejects;
TRUNCATE TABLE BRANCHES_REJECTS;

select * from borrowers;
select * from borrowers_rejects;

update borrowers set FIRST_NAME = 'Sunita' where BORROWER_ID = 'B0003';

UPDATE BRANCHES SET BRANCH_NAME = 'Bombay' where Branch_id = 'BR004';
--------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE CLEANED_LOANS (
    LOAN_ID            STRING,
    BORROWER_ID        STRING,
    BRANCH_ID          STRING,
    LOAN_TYPE          STRING,
    PRINCIPAL_AMOUNT   NUMBER(10,2),
    INTEREST_RATE      NUMBER(10,2),
    TENURE_MONTHS      NUMBER(10),
    DISBURSE_DATE      DATE,
    LOAN_STATUS        STRING,
    SECURITY_TYPE      STRING,
    UPDATED_AT         TIMESTAMP_NTZ,
    LOAD_TS            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE_NAME   STRING
);

CREATE OR REPLACE TABLE REJ_LOANS (
    LOAN_ID            STRING,
    BORROWER_ID        STRING,
    BRANCH_ID          STRING,
    LOAN_TYPE          STRING,
    PRINCIPAL_AMOUNT   STRING,
    INTEREST_RATE      STRING,
    TENURE_MONTHS      STRING,
    DISBURSE_DATE      STRING,
    LOAN_STATUS        STRING,
    SECURITY_TYPE      STRING,
    UPDATED_AT         STRING,

    ERROR_FLAG         NUMBER(1),      -- 0 = invalid
    ERROR_REASON       STRING,         -- reason text

    LOAD_TS            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE_NAME   STRING
);

select * from cleaned_loans;
TRUNCATE TABLE CLEANED_LOANS;
select * from rej_loans;
TRUNCATE TABLE REJ_LOANS;
update cleaned_loans set interest_rate = 10.00 where borrower_id = 'B0144';

-------------------------------------------------------------------------

CREATE OR REPLACE TABLE TRANSACTIONS (
    TXN_ID              VARCHAR(50),
    TXN_LINE_ID         VARCHAR(10),
    TXN_DATE            DATE,              -- Changed to DATE
    LOAN_ID             VARCHAR(20),
    BORROWER_ID         VARCHAR(20),
    BRANCH_ID           VARCHAR(20),
    TXN_TYPE            VARCHAR(50),
    AMOUNT              NUMBER(18,2),      -- Changed to NUMBER for calculations
    INTEREST_COMPONENT  NUMBER(18,2),      -- Changed to NUMBER
    FEE_COMPONENT       NUMBER(18,2),      -- Changed to NUMBER
    PAYMENT_MODE        VARCHAR(50),
    TXN_STATUS          VARCHAR(50),
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE_NAME    VARCHAR(255)
    );

    CREATE OR REPLACE TABLE REJ_TRANSACTIONS (
    TXN_ID              STRING,
    TXN_LINE_ID         STRING,
    TXN_DATE            STRING,       
    LOAN_ID             STRING,
    BORROWER_ID         STRING,
    BRANCH_ID           STRING,
    TXN_TYPE            STRING,
    AMOUNT              STRING,       
    INTEREST_COMPONENT  STRING,
    FEE_COMPONENT       STRING,
    PAYMENT_MODE        STRING,
    TXN_STATUS          STRING,    
    LOAD_TS             TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE_NAME    STRING,
    REJECT_REASON       STRING,
    FLAG                INTEGER 
);

SELECT * FROM TRANSACTIONS;
TRUNCATE TABLE TRANSACTIONS;

SELECT * FROM REJ_TRANSACTIONS;
TRUNCATE TABLE REJ_TRANSACTIONS;
----------------------------------------------------------------------------------


select borrower_id from transactions minus

select borrower_id from dw_lma.d2_schema_lma.dim_borrower;

create or replace table dummy_trigger_table(
    DUMMY_ID NUMBER
);

CREATE TABLE IF NOT EXISTS VALID_BORROWERS LIKE BORROWERS;
CREATE TABLE IF NOT EXISTS VALID_BRANCHES LIKE BRANCHES;
CREATE TABLE IF NOT EXISTS VALID_LOANS LIKE CLEANED_LOANS;
CREATE TABLE IF NOT EXISTS VALID_TRANSACTIONS LIKE TRANSACTIONS;

-- ==========================================================
-- 2. Create the Streams (The "Cameras")
-- ==========================================================
-- These MUST exist before you load data into the Raw tables
CREATE OR REPLACE STREAM STR_BORROWERS ON TABLE BORROWERS;
CREATE OR REPLACE STREAM STR_BRANCHES ON TABLE BRANCHES;
CREATE OR REPLACE STREAM STR_LOANS ON TABLE CLEANED_LOANS;
CREATE OR REPLACE STREAM STR_TRANSACTIONS ON TABLE TRANSACTIONS;