# snowflake-all-in-one

## Summary
```sql

-------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CLUSTER Data
-------------------------------------------------------------------------------------------------------------------------------------------------------------
// Adding Cluster Key a.k.a Index
CREATE TABLE <table_name> ... CLUSTER BY (COLUMN1, COLUMN2);

CREATE TABLE <table_name> ... CLUSTER BY (Expressions);

ALTER TABLE <table_name> CLUSTER BY ( COLUMN1 );

ALTER TABLE <table_name> DROP CLUSTERING KEY

-------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Validate Data
-------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE save_copy_errors AS SELECT * FROM TABLE(VALIDATE(LOAN_PAYMENT, JOB_ID=>'<query_id>'));

SELECT * FROM save_copy_errors;

REMOVE@my_csv_stagePATTERN='.*.csv.gz';

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage s
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv')
    VALIDATION_MODE = RETURN_ERRORS | RETURN_n_ROWS;   -- Validate the data instead of loading them

-------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Transformation
-------------------------------------------------------------------------------------------------------------------------------------------------------------

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM (select 
            s.$1,
            s.$2, 
            s.$3,
	    CASE WHEN CAST(s.$3 as int) < 0 THEN 'not profitable' ELSE 'profitable' END ,
            substring(s.$5,1,5) 
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

-- Using subset of columns
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
);
    
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX (ORDER_ID,PROFIT)
    FROM (select 
            s.$1,
            s.$3
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv')
    --ON_ERROR=<>;


--ON_ERROR Options:
* CONTINUE
* ABORT_STATEMENT (default)
* SKIP_FILE
* SKIP_FILE_<total_error_limit_count_threshold>
* SKIP_FILE_<percentage_limit_threshold>%

// Using file format object in Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

  
// Using file format object in Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

-- Truncate table
TRUNCATE TABLE <<table_name>>
```


# SnowSQL Client

```sql
snowsql -a xkshnvm-xz97051 -u pjaiswal
Pj9891862653@10

use DATABASE OUR_FIRST_DB;

-- Creating external stage
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);

-- Description of external stage

DESC STAGE my_csv_stage; 


PUT file:///Users/pjaiswal/Downloads/202
                                        3-06-20-3_12pm.csv @my_csv_stage AUTO_CO
                                        MPRESS=TRUE;

-- Alter external stage   

ALTER STAGE aws_stage
    SET credentials=(aws_key_id='XYZ_DUMMY_ID' aws_secret_key='987xyz');

-- Publicly accessible staging area    

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3';



LIST @my_csv_stage;


copy into our_first_db.public.LOAN_PAYMENT
from @my_csv_stage/2023-06-20-3_12pm.csv.gz
file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1)
	       --files = ('OrderDetails.csv')
	       --pattern='.*Order.*'
 



```


