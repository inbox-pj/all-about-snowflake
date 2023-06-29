1. Create a table called employees with the following columns and data types:
  customer_id int,
  first_name varchar(50),
  last_name varchar(50),
  email varchar(50),
  age int,
  department varchar(50)

2. Create a stage object pointing to 's3://snowflake-assignments-mc/copyoptions/example1'

3. Create a file format object with the specification
TYPE = CSV
FIELD_DELIMITER=','
SKIP_HEADER=1;

4. Use the copy option to only validate if there are errors and if yes what errors.

5. Load the data anyways regardless of the error using the ON_ERROR option. How many rows have been loaded?



```
CREATE OR REPLACE DATABASE EXERCISE_DB;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_SCHEMA;

CREATE OR REPLACE TABLE EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE (
    customer_id integer,
    first_name varchar(50),
    last_name varchar(50),
    email varchar(50),
    age integer,
    department varchar(50)
);

DESC TABLE EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_STAGE_SCHEMA;

CREATE OR REPLACE STAGE EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE 
url='s3://snowflake-assignments-mc/copyoptions/example1';

DESC STAGE EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE;

LIST @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE;

CREATE OR REPLACE SCHEMA EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA;

CREATE OR REPLACE FILE FORMAT EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT 
TYPE='CSV',
FIELD_DELIMITER=',',
SKIP_HEADER=1;


COPY INTO EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
VALIDATION_MODE=RETURN_ERRORS;


COPY INTO EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
VALIDATION_MODE=RETURN_1_ROWS;

SELECT * FROM EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE;


COPY INTO EXERCISE_DB.EXERCISE_SCHEMA.EMPLOYEE
FROM @EXERCISE_DB.EXERCISE_STAGE_SCHEMA.EXERCISE_STAGE
file_format = (FORMAT_NAME=EXERCISE_DB.EXERCISE_FILE_FORMAT_SCHEMA.EXERCISE_FILE_FORMAT)
ON_ERROR=CONTINUE;

```


========================================================================================================================================================================================================
