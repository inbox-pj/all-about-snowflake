1. Create a table called employees with the following columns and data types:
    - customer_id int,
    - first_name varchar(50),
    - last_name varchar(50),
    - email varchar(50),
    - age int,
    - department varchar(50)
3. Create a stage object pointing to 's3://snowflake-assignments-mc/copyoptions/example1'
4. Create a file format object with the specification
    - TYPE = CSV
    - FIELD_DELIMITER=','
    - SKIP_HEADER=1;
5. Use the copy option to only validate if there are errors and if yes what errors.
6. Load the data anyways regardless of the error using the ON_ERROR option. How many rows have been loaded?



```sql
CREATE OR REPLACE WAREHOUSE COMPUTE_WAREHOUSE 
WITH 
WAREHOUSE_SIZE=XSMALL
MAX_CLUSTER_COUNT=3
MIN_CLUSTER_COUNT=1
AUTO_SUSPEND=600      --automatically suspend the virtual warehouse after 10 minutes of not being used
AUTO_RESUME=TRUE
INITIALLY_SUSPENDED=TRUE
COMMENT='This is First Warehouse';


DROP DATABASE IF EXISTS EXERCISE_DB;
DROP WAREHOUSE IF EXISTS COMPUTE_WAREHOUSE;

ALTER DATABASE <old_database_name> RENAME to <new_database_name>;

CREATE OR REPLACE DATABASE EXERCISE_DB;

USE DATABASE EXERCISE_DB;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_SCHEMA;

CREATE OR REPLACE TABLE EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE (
    customer_id integer,
    first_name varchar(50),
    last_name varchar(50),
    email varchar(50),
    age integer,
    department varchar(50)
);

// Truncate table
TRUNCATE table EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE;

DESC TABLE EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_STAGE_SCHEMA;

CREATE OR REPLACE STAGE EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE 
--url='s3://snowflake-assignments-mc/copyoptions/example1';
url='s3://snowflake-assignments-mc/copyoptions/example2';

DESC STAGE EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE;

LIST @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA;

CREATE OR REPLACE FILE FORMAT EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT 
TYPE='CSV',
FIELD_DELIMITER=',',
SKIP_HEADER=1;

// Altering file format object
ALTER file format EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT
    SET SKIP_HEADER = 1;

// See properties of file format object
DESC file format EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT;

COPY INTO EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
VALIDATION_MODE=RETURN_ERRORS;


COPY into EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
VALIDATION_MODE=RETURN_1_ROWS;

SELECT * FROM EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE;


COPY into EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
ON_ERROR=CONTINUE
SIZE_LIMIT=20000    -- size limit
RETURN_FAILED_ONLY=TRUE    -- focus only on failed/partially loaded record, with ON_ERROR=CONTINUE only
TRUNCATECOLUMNS=TRUE    -- truncate the value with defined column length, if exceeded
FORCE=TRUE    -- reload forcefullt even if file is loaded previously, even the files have not changed, may load to duplicate records
PURGE=TRUE;    -- deletes the files from staging once loaded into table


SELECT rejected_record FROM TABLE(result_scan('01ad4fbc-0001-2a90-0002-1ac20002619a'));    -- associated with VALIDATION_MODE=RETURN_ERRORS


SELECT rejected_record FROM TABLE (validate(exercise_db.exercise_schema.employee, job_id => '01ad4a4c-0001-2b27-0002-1ac20002a0ee')); -- associated with ON_ERROR=CONTINUE

SELECT split_part(rejected_record, ',', 1),
split_part(rejected_record, ',', 2),
split_part(rejected_record, ',', 3),
split_part(rejected_record, ',', 4),
split_part(rejected_record, ',', 5),
split_part(rejected_record, ',', 6)
FROM (SELECT rejected_record FROM TABLE (validate(exercise_db.exercise_schema.employee, job_id => '01ad4a4c-0001-2b27-0002-1ac20002a0ee')));


-- Load History
SELECT * FROM EXERCISE_DB.INFORMATION_SCHEMA.LOAD_HISTORY;

-- Global Load history
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;



```


=============================================================================



1. Create a stage object that is pointing to 's3://snowflake-assignments-mc/unstructureddata/'
2. Create a file format object that is using TYPE = JSON
3. Create a table called JSON_RAW with one column
    - Column name: Raw
    - Column type: Variant
4. Copy the raw data in the JSON_RAW table using the file format object and stage object
5. Select the below attributes and query these columns-
    - first_name
    - last_name
    - skills
6. The skills column contains an array. Query the first two values in the skills attribute for every record in a separate column:
    - first_name
    - last_name
    - skills_1
    - skills_2
7. Create a table and insert the data for these 4 columns in that table.


```sql
create or replace database EXERCISE_DB;

create or replace schema EXERCISE_DB.EXERCISE_SCHEMA;

create or replace stage EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_STAGE
url = 's3://snowflake-assignments-mc/unstructureddata/';


list @EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_STAGE;

desc stage EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_STAGE;

create or replace file format EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_FILE_FORMAT_SCHEMA
type=JSON;

desc file format EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_FILE_FORMAT_SCHEMA;


create or replace table EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW (
raw variant
);


select * from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW;

copy into EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW
from @EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_STAGE
file_format = EXERCISE_DB.EXERCISE_SCHEMA.EXERCISE_FILE_FORMAT_SCHEMA;


select 
$1:id::int as id,
$1:first_name::varchar as first_name,
$1:last_name::varchar as last_name,
$1:age::int as age,
$1:department::varchar as department,
raw:Skills::STRING as Skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW;


select 
$1:id::int as id,
$1:first_name::varchar as first_name,
$1:last_name::varchar as last_name,
$1:age::int as age,
$1:department::varchar as department,
raw:Skills[0]::STRING as Skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW
UNION ALL
select 
$1:id::int as id,
$1:first_name::varchar as first_name,
$1:last_name::varchar as last_name,
$1:age::int as age,
$1:department::varchar as department,
raw:Skills[1]::STRING as Skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW
order by id;

select $1:id::int as id, array_size(raw:Skills) from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW order by id asc;


select 
$1:id::int as id,
$1:first_name::varchar as first_name,
$1:last_name::varchar as last_name,
$1:age::int as age,
$1:department::varchar as department,
raw:Skills[0]::STRING as Skills1,
raw:Skills[1]::STRING as Skills2
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW;

select 
raw:id::int as id,
raw:first_name::varchar as first_name,
raw:last_name::varchar as last_name,
raw:age::int as age,
raw:department::varchar as department,
f.value as skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW, table(flatten(raw:Skills)) f
order by id asc;


select f.* from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW, table(flatten(raw:Skills)) f;

create or replace table skills_set as
select raw:id::int as id,
raw:first_name::varchar as first_name,
raw:last_name::varchar as last_name,
raw:age::int as age,
raw:department::varchar as department,
f.value as skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW, table(flatten(raw:Skills)) f
order by id asc;


create or replace table skills_set as
select 
$1:id::int as id,
$1:first_name::varchar as first_name,
$1:last_name::varchar as last_name,
$1:age::int as age,
$1:department::varchar as department,
raw:Skills[0]::STRING as Skills1,
raw:Skills[1]::STRING as Skills2,
raw:Skills[2]::STRING as Skills3
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW
order by id asc;

select * from skills_set;

select skills1 from skills_set where first_name='Florina';
```
