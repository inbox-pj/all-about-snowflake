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


















