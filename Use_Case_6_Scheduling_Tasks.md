## Scheduling Tasks

```sql
-- Create task (Parent Task)
CREATE OR REPLACE TASK CUSTOMER_CREATE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    --SCHEDULE = 'USING CRON 0 7,10 * * 5L UTC'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);

-- Create task (Child Task)
CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    AFTER CUSTOMER_CREATE     -- this CUSTOMER_INSERT task will execute only after CUSTOMER_CREATE task will be finished
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);    

SHOW TASKS;

-- Task starting and suspending (first root task)
ALTER TASK CUSTOMER_CREATE RESUME;
ALTER TASK CUSTOMER_INSERT RESUME;

ALTER TASK CUSTOMER_CREATE SUSPEND;
ALTER TASK CUSTOMER_INSERT SUSPEND;


-- __________ minute (0-59)
-- | ________ hour (0-23)
-- | | ______ day of month (1-31, or L)
-- | | | ____ month (1-12, JAN-DEC)
-- | | | | __ day of week (0-6, SUN-SAT, or L)
-- | | | | |
-- | | | | |
-- * * * * *

-- Every minute
SCHEDULE = 'USING CRON * * * * * UTC'

-- Every day at 6am UTC timezone
SCHEDULE = 'USING CRON 0 6 * * * UTC'

-- Every hour starting at 9 AM and ending at 5 PM on Sundays 
SCHEDULE = 'USING CRON 0 9-17 * * SUN America/Los_Angeles'


-- 7.00 am at last Friday of the month
SCHEDULE = 'USING CRON 0 7 * * 5L UTC'

-- Create task with stored procedure
CREATE OR REPLACE PROCEDURE CUSTOMERS_INSERT_PROCEDURE (CREATE_DATE varchar)
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var sql_command = 'INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(:1);'
        snowflake.execute(
            {
            sqlText: sql_command,
            binds: [CREATE_DATE]
            });
        return "Successfully executed.";
        $$;

CREATE OR REPLACE TASK CUSTOMER_TAKS_PROCEDURE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS CALL CUSTOMERS_INSERT_PROCEDURE (CURRENT_TIMESTAMP);


-- Use the table function "TASK_HISTORY()"
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;
  
  
-- See results for a specific Task in a given time
select *
from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-4,current_timestamp()),
    result_limit => 5,
    task_name=>'CUSTOMER_INSERT'));
  
  
-- See results for a given time period
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>to_timestamp_ltz('2021-04-22 11:28:32.776 -0700'),
    scheduled_time_range_end=>to_timestamp_ltz('2021-04-22 11:35:32.776 -0700')));  
  

-- Task with Condition
CREATE OR REPLACE TASK CUSTOMER_CREATE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN 1==1
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);

CREATE OR REPLACE TASK CUSTOMER_CREATE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('<stream_name>')                -- only SYSTEM$STREAM_HAS_DATA function can be used in condition
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);

```
