## Time Travel, Backup and Restore

```sql

-- Setting up UTC time for convenience
ALTER SESSION SET TIMEZONE ='UTC'
SELECT DATEADD(DAY, 1, CURRENT_TIMESTAMP)

-- Using time travel: Method 1 - 2 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*1.5)

-- Using time travel: Method 2 - before timestamp
SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2021-04-15 17:47:50.581'::timestamp)

-- Using time travel: Method 3 - before Query ID
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9ee5-0500-8473-0043-4d8300073062')

-- Restoring the Table data
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test_backup as
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9ef0-0500-8473-0043-4d830007309a')

TRUNCATE OUR_FIRST_DB.public.test

INSERT INTO OUR_FIRST_DB.public.test
SELECT * FROM OUR_FIRST_DB.public.test_backup


-- UNDROP command - Tables
DROP TABLE OUR_FIRST_DB.public.customers;
UNDROP TABLE OUR_FIRST_DB.public.customers;


-- UNDROP command - Database
DROP DATABASE OUR_FIRST_DB;
UNDROP DATABASE OUR_FIRST_DB;

-- Undroping a with a name that already exists
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.customers as
SELECT * FROM OUR_FIRST_DB.public.customers before (statement => '019b9f7c-0500-851b-0043-4d83000762be')

UNDROP table OUR_FIRST_DB.public.customers;

ALTER TABLE OUR_FIRST_DB.public.customers
RENAME TO OUR_FIRST_DB.public.customers_wrong;

```
## Create/Alter table/DATABASE with DATA_RETENTION_TIME_IN_DAYS

```sql

ALTER DATABASE OUR_FIRST_DB SET DATA_RETENTION_TIME_IN_DAYS = <x>

-- x could be 0-90 days based on snowflake edition used, default to 1 day only, contribute to storage cost
Create Table <table name> (
-- 
)
DATA_RETENTION_TIME_IN_DAYS = <x>


Create Table <table name> SET
DATA_RETENTION_TIME_IN_DAYS = <x>

```


## Time Travel Cost

```sql

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Query time travel storage
SELECT 	ID, 
		TABLE_NAME, 
		TABLE_SCHEMA,
        TABLE_CATALOG,
		ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
		TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;
```
