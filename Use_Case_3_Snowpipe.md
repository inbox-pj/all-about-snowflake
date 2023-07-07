## Snowpipe Setup Stages
- Create Table
    - Create Storage Integration
        - Create Stage
            - Create Pipe
                - Setup Notification

```sql
-----------------------------------------------------------------
-- SNOWPIPE
-----------------------------------------------------------------


CREATE OR REPLACE DATABASE GCP_INTEGRATION_DB;

CREATE OR REPLACE SCHEMA GCP_INTEGRATION_DB.GCP;

CREATE OR REPLACE STORAGE INTEGRATION GCP_INTEGRATION
        TYPE=EXTERNAL_STAGE
        STORAGE_PROVIDER = GCS
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('gcs://pjaiswal-snowflake-bucket-gcp/1/json/');

DESC STORAGE INTEGRATION GCP_INTEGRATION;

---
-- Take STORAGE_GCP_SERVICE_ACCOUNT
-- In GCP IAM, Create Role and for bucket, allow principle to this role with STORAGE_GCP_SERVICE_ACCOUNT under Buckets
-- Further create Topics under Pub/Sub or using $ gsutil notification create -t pjaiswal_snowflake_notification -f json gs://pjaiswal-snowflake-bucket-gcp
-- copy the subscription id for GCP_PUBSUB_SUBSCRIPTION_NAME
---

CREATE OR REPLACE NOTIFICATION INTEGRATION pjaiswal_snowflake_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/peerless-robot-332007/subscriptions/pjaiswal_snowflake_notification-subscription';


DESC NOTIFICATION INTEGRATION pjaiswal_snowflake_notification;

---
-- Take GCP_PUBSUB_SERVICE_ACCOUNT
-- Add Principle as Pub/Sub Subscriber for GCP_PUBSUB_SERVICE_ACCOUNT
---


CREATE OR REPLACE FILE FORMAT GCP_INTEGRATION_DB.GCP.GCP_FILE_FORMAT
        TYPE=JSON;
        
CREATE OR REPLACE STAGE GCP_INTEGRATION_STAGE_LOAD
  URL = 'gcs://pjaiswal-snowflake-bucket-gcp/1/json/'
  STORAGE_INTEGRATION = GCP_INTEGRATION
  FILE_FORMAT = GCP_INTEGRATION_DB.GCP.GCP_FILE_FORMAT;

LIST @GCP_INTEGRATION_STAGE_LOAD;

SHOW stages;

CREATE or replace TABLE HEALTHCARE (
Average_Covered_Charges VARCHAR(50),
Average_Total_Payments VARCHAR(50),
Total_Discharges VARCHAR(50),
Bachelor_or_Higher VARCHAR(50),
HS_Grad_or_Higher VARCHAR(50),
Reimbursement VARCHAR(50),
DRG_Definition VARCHAR(50),
INCOME_PER_CAPITA VARCHAR(50),
MEDIAN_EARNINGS_BACHELORS VARCHAR(50),
MEDIAN_EARNINGS_GRADUATE VARCHAR(50),
MEDIAN_EARNINGS_HS_GRAD VARCHAR(50),
MEDIAN_EARNINGS_LESS_THAN_HS VARCHAR(50),
MEDIAN_FAMILY_INCOME VARCHAR(50),
Number_of_Records VARCHAR(50),
POP_25_OVER VARCHAR(50),
Provider_City VARCHAR(50),
Provider_Id VARCHAR(50),
Provider_Name VARCHAR(50),
Provider_State VARCHAR(50),
Provider_Street_Address VARCHAR(50),
Provider_Zip_Code VARCHAR(50),
Referral_Region VARCHAR(50),
Referral_Region_Provider_Name VARCHAR(50),
ReimbursementPercentage VARCHAR(50),
Total_covered_charges VARCHAR(50),
Total_payments VARCHAR(50),
id VARCHAR(50),
filename    VARCHAR(16777216)
,file_row_number VARCHAR(16777216)
,load_timestamp timestamp default TO_TIMESTAMP_NTZ(current_timestamp));


CREATE OR REPLACE pipe GCP_PIPE
auto_ingest = TRUE
INTEGRATION=pjaiswal_snowflake_notification
AS
COPY INTO HEALTHCARE
FROM (
    SELECT
    $1:" Average Covered Charges "::STRING,
    $1:" Average Total Payments "::String,
    $1:" Total Discharges "::String,
    $1:"% Bachelor's or Higher"::String,
    $1:"% HS Grad or Higher"::String,
    $1:"% Reimbursement"::String,
    $1:"DRG Definition"::String,
    $1:"INCOME_PER_CAPITA"::String,
    $1:"MEDIAN EARNINGS - BACHELORS"::String,
    $1:"MEDIAN EARNINGS - GRADUATE"::String,
    $1:"MEDIAN EARNINGS - HS GRAD"::String,
    $1:"MEDIAN EARNINGS- LESS THAN HS"::String,
    $1:"MEDIAN_FAMILY_INCOME"::String,
    $1:"Number of Records"::String,
    $1:"POP_25_OVER"::String,
    $1:"Provider City"::String,
    $1:"Provider Id"::String,
    $1:"Provider Name"::String,
    $1:"Provider State"::String,
    $1:"Provider Street Address"::String,
    $1:"Provider Zip Code"::String,
    $1:"Referral Region"::String,
    $1:"Referral Region Provider Name"::String,
    $1:"ReimbursementPercentage"::String,
    $1:"Total covered charges"::String,
    $1:"Total payments"::String,
    $1:"_id"::String,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    TO_TIMESTAMP_NTZ(current_timestamp) 
    from @GCP_INTEGRATION_STAGE_LOAD
)
ON_ERROR=CONTINUE
TRUNCATECOLUMNS=TRUE;

show pipes;
desc Pipe GCP_PIPE;


SELECT SYSTEM$PIPE_STATUS('GCP_PIPE');

-- Load backlog files
ALTER PIPE GCP_PIPE refresh;

select * from HEALTHCARE;


SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name=>  'HEALTHCARE',
   start_time=> dateadd(hours, -1, current_timestamp())));



-- Pause pipe
ALTER PIPE GCP_PIPE SET PIPE_EXECUTION_PAUSED = true;

-- Resume pipe
ALTER PIPE GCP_PIPE SET PIPE_EXECUTION_PAUSED = false

-- Verify pipe is paused and has pendingFileCount 0 
SELECT SYSTEM$PIPE_STATUS('GCP_PIPE');


SHOW PIPES like '%GCP_PIPE%';

SHOW PIPES in database GCP_INTEGRATION_DB;

SHOW PIPES in schema GCP;

SHOW PIPES like '%GCP%' in Database GCP_INTEGRATION_DB;


```
