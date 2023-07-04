-- Create dedicated warehouse for different groups and assigned users per role

```sql
--Create virtual warehouse
CREATE OR REPLACE WAREHOUSE GL_WH
WITH
WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

ALTER WAREHOUSE GL_WH SET WAREHOUSE_SIZE = 'XSMALL';

CREATE OR REPLACE WAREHOUSE EDW_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

--Create role
CREATE ROLE GL_ROLE;
CREATE ROLE EDW_ROLE;

--Grant role on Warehouse
GRANT USAGE ON WAREHOUSE GL_WH TO ROLE GL_ROLE;
GRANT USAGE ON WAREHOUSE EDW_WH TO ROLE EDW_ROLE;

--Create Users
CREATE USER GL_USER PASSWORD = 'GL_USER' LOGIN_NAME = 'GL_USER' DEFAULT_ROLE='GL_ROLE' DEFAULT_WAREHOUSE = 'GL_WH'  MUST_CHANGE_PASSWORD = FALSE;
CREATE USER EDW_USER PASSWORD = 'EDW_USER' LOGIN_NAME = 'EDW_USER' DEFAULT_ROLE='EDW_ROLE' DEFAULT_WAREHOUSE = 'EDW_WH'  MUST_CHANGE_PASSWORD = FALSE;

--Grant Roles to Users
GRANT ROLE GL_ROLE TO USER GL_USER;
GRANT ROLE EDW_ROLE TO USER EDW_USER;

--Drop objects
DROP USER IF EXISTS GL_USER;
DROP USER IF EXISTS EDW_USER;

DROP ROLE IF EXISTS GL_ROLE;
DROP ROLE IF EXISTS EDW_ROLE;

DROP WAREHOUSE IF EXISTS GL_WH;
DROP WAREHOUSE IF EXISTS EDW_WH;
```

```sql
--=================================================================
-- Cloud Integration - GCP
--=================================================================

--create integration object
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket/path', 'gcs://bucket/path2');

ALTER STORAGE INTEGRATION gcp_integration
  SET
  STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket/path0', 'gcs://bucket/path1');

-- Describe integration object to provide access
DESC STORAGE integration gcp_integration;


--Unload data from Snoflake to GCP
-- create stage object
create or replace stage demo_db.public.stage_gcp
    STORAGE_INTEGRATION = gcp_integration
    URL = 'gcs://bucket/<new_unload_path>'
    FILE_FORMAT = fileformat_gcp
    ;

COPY INTO @stage_gcp FROM <table_name>;

```







