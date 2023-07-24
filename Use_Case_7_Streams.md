## STREAMS

### Metadata
- METADATA$ACTION
- METADATA$ISUPDATE
- METADATA$ROW_ID
------------------------------------ 

### INSERT

```sql
-- Create a stream object
create or replace stream Customer_Stream on table Customer;

SHOW STREAMS;

DESC STREAM Customer_Stream;

-- Get changes on data using stream (INSERTS)
select * from Customer_Stream;

-- Consume stream object
INSERT INTO Customer_Report 
    SELECT 
    CS.id,
    C.department,
    CS.price,
    CS.amount,
    CS.STORE_ID,
    C.LOCATION, 
    C.EMPLOYEES 
    FROM Customer_Stream CS
    JOIN CUSTOMER C ON CS.ID=C.ID ;


-- ofset check.

select SYSTEM$STREAM_GET_TABLE_TIMESTAMP('Customer_Stream')
```


### UPDATE

```sql
merge into Customer_Report CR      		-- Target table to merge changes from source table
using Customer_Stream CS                -- Stream that has captured the changes
   on  CR.id = CS.id                 
when matched 
    and S.METADATA$ACTION ='INSERT'
    and S.METADATA$ISUPDATE ='TRUE'        -- Indicates the record has been updated 
    then update 
    set 
        CR.price = CS.price,
        CR.amount= CS.amount;
```

### DELETE

```sql
merge into Customer_Report CR      		-- Target table to merge changes from source table
using Customer_Stream CS                	-- Stream that has captured the changes
   on  CR.id = CS.id            
when matched 
    and S.METADATA$ACTION ='DELETE' 
    and S.METADATA$ISUPDATE = 'FALSE'
    then delete    
```

### Process UPDATE,INSERT & DELETE simultaneously

```sql
merge into SALES_FINAL_TABLE F      				-- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id=S.id
when matched                        		-- DELETE condition
    and S.METADATA$ACTION ='DELETE' 
    and S.METADATA$ISUPDATE = 'FALSE'
    then delete                   
when matched                        		-- UPDATE condition
    and S.METADATA$ACTION ='INSERT' 
    and S.METADATA$ISUPDATE  = 'TRUE'       
    then update 
    set f.product = s.product,
        f.price = s.price,
        f.amount= s.amount,
        f.store_id=s.store_id
when not matched 							-- INSERT
    and S.METADATA$ACTION ='INSERT'
    then insert 
    (id,product,price,store_id,amount,employees,location)
    values
    (s.id, s.product,s.price,s.store_id,s.amount,s.employees,s.location)
```

### Automatate the STREAMS using TASKS

```sql
CREATE OR REPLACE TASK all_data_changes
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('SALES_STREAM')
    AS 
merge into SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id=S.id
when matched                        -- DELETE condition
    and S.METADATA$ACTION ='DELETE' 
    and S.METADATA$ISUPDATE = 'FALSE'
    then delete                   
when matched                        -- UPDATE condition
    and S.METADATA$ACTION ='INSERT' 
    and S.METADATA$ISUPDATE  = 'TRUE'       
    then update 
    set f.product = s.product,
        f.price = s.price,
        f.amount= s.amount,
        f.store_id=s.store_id
when not matched 
    and S.METADATA$ACTION ='INSERT'
    then insert 
    (id,product,price,store_id,amount,employees,location)
    values
    (s.id, s.product,s.price,s.store_id,s.amount,s.employees,s.location)

ALTER TASK all_data_changes RESUME;

-- Verify the history
select *
from table(information_schema.task_history())
order by name asc,scheduled_time desc;
```

### STREAM Type Append-only (CDC for INSERT)

```sql
-- Create stream with append-only
CREATE OR REPLACE STREAM SALES_STREAM_APPEND
ON TABLE SALES_RAW_STAGING 
APPEND_ONLY = TRUE;
```


### Change clause, alternative of Stream

```sql
ALTER TABLE sales_raw
SET CHANGE_TRACKING = TRUE;

SELECT * FROM SALES_RAW
CHANGES(information => default)
AT (offset => -0.5*60);

SELECT * FROM SALES_RAW
CHANGES(information  => default)
AT (timestamp => 'your-timestamp'::timestamp_tz)

SELECT * FROM SALES_RAW
CHANGES(information  => append_only)
AT (timestamp => 'your-timestamp'::timestamp_tz)


CREATE OR REPLACE TABLE PRODUCTS 
AS
SELECT * FROM SALES_RAW
CHANGES(information  => append_only)
AT (timestamp => 'your-timestamp'::timestamp_tz)

```


# Snowpipe-Stream-Task collaboration

<img width="854" alt="image" src="https://github.com/inbox-pj/snowflake-all-in-one/assets/53929164/0a837e61-4e56-4344-87fe-c2bf0ca394ca">

```sql
-- Create integration object
create or replace storage integration Snowflake_Obj
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::917077600714:role/snowflake'
storage_allowed_locations = ('s3://snowpipe-demo2/');

desc integration Snowflake_Obj;

-- Create file format
create or replace file format csv_file_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true;

-- Create stage
create or replace stage snow_stage
storage_integration = Snowflake_Obj
url = 's3://snowpipe-demo2/'
file_format = csv_file_format;

-- Create snowflake staging table

create or replace transient table parking
(
Summons_Number	 Number	,
Plate_ID	Varchar	,
Registration_State	 Varchar	,
Plate_Type	 Varchar	,
Issue_Date	DATE	
);


-- Create snowflake tables to load data for LC and NJ cities.
create or replace transient table LC_parking_t like PARKING;
create or replace transient table NJ_parking_t like PARKING;

-- Create snowpipe to continuously load data.
create or replace pipe demo_db.public.snowpipe auto_ingest=true as
    copy into demo_db.public.parking
    from @demo_db.public.snow_stage/parking_data/
    ON_ERROR='CONTINUE'
    file_format = (type = 'csv',error_on_column_count_mismatch=false);
       
show pipes; 
show tasks;

alter pipe SNOWPIPE refresh;

-- Create stream object to capture changes in NY table. 
create or replace  stream LC_parking on table demo_db.public.parking; 
create or replace  stream NJ_parking on table demo_db.public.parking; 

-- Create task to capture only LC city data
CREATE OR REPLACE TASK DEMO_DB.PUBLIC.LC_parking
  WAREHOUSE = compute_wh
  SCHEDULE = '1 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('LC_parking')
AS
INSERT INTO snowpipe.public.LC_parking_t
SELECT * FROM DEMO_DB.PUBLIC.parking WHERE Registration_State='LC';

ALTER TASK LC_parking RESUME;
-- Create task to capture only NJ city data

CREATE OR REPLACE TASK DEMO_DB.PUBLIC.NJ_parking
  WAREHOUSE = compute_wh
  SCHEDULE = '1 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('NJ_parking')
AS
INSERT INTO demo_db.public.NJ_parking_t
SELECT * FROM parking WHERE Registration_State='NJ';

ALTER TASK NJ_PARKING RESUME;

CREATE OR REPLACE TASK DEMO_DB.PUBLIC.REFRESH_PIPE
  WAREHOUSE = compute_wh
  SCHEDULE = '1 minute'
AS
alter pipe demo_db.public.SNOWPIPE refresh;

ALTER TASK DEMO_DB.PUBLIC.REFRESH_PIPE RESUME;


```










