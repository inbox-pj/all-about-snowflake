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
PURGE=TRUE;    -- deletes the files from external staging (s3, gcs etc) once loaded into table


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
f.value.name as skills
from EXERCISE_DB.EXERCISE_SCHEMA.JSON_RAW, table(flatten(raw:Skills)) f
order by id asc;

-- Parse array within array.

select
cl.value:cityName::string as city_name,
yl.value::string as year_lived
from json_demo,
table(flatten(v:citiesLived)) cl,        -- Higher array
table(flatten(cl.value:yearsLived)) yl;  -- Nested array


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

---========================Processing XML===============================

```sql
create or replace table xml_demo (v variant);

insert into xml_demo
select
parse_xml('<bpd:AuctionData xmlns:bpd="http://www.treasurydirect.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.treasurydirect.gov/ http://www.treasurydirect.gov/xsd/Auction_v1_0_0.xsd">
<AuctionAnnouncement>
<SecurityTermWeekYear>26-WEEK</SecurityTermWeekYear>
<SecurityTermDayMonth>182-DAY</SecurityTermDayMonth>
<SecurityType>BILL</SecurityType>
<CUSIP>912795G96</CUSIP>
<AnnouncementDate>2008-04-03</AnnouncementDate>
<AuctionDate>2008-04-07</AuctionDate>
<IssueDate>2008-04-10</IssueDate>
<MaturityDate>2008-10-09</MaturityDate>
<OfferingAmount>21.0</OfferingAmount>
<CompetitiveTenderAccepted>Y</CompetitiveTenderAccepted>
<NonCompetitiveTenderAccepted>Y</NonCompetitiveTenderAccepted>
<TreasuryDirectTenderAccepted>Y</TreasuryDirectTenderAccepted>
<AllTenderAccepted>Y</AllTenderAccepted>
<TypeOfAuction>SINGLE PRICE</TypeOfAuction>
<CompetitiveClosingTime>13:00</CompetitiveClosingTime>
<NonCompetitiveClosingTime>12:00</NonCompetitiveClosingTime>
<NetLongPositionReport>7350000000.0</NetLongPositionReport>
<MaxAward>7350000000</MaxAward>
<MaxSingleBid>7350000000</MaxSingleBid>
<CompetitiveBidDecimals>3</CompetitiveBidDecimals>
<CompetitiveBidIncrement>0.005</CompetitiveBidIncrement>
<AllocationPercentageDecimals>2</AllocationPercentageDecimals>
<MinBidAmount>100</MinBidAmount>
<MultiplesToBid>100</MultiplesToBid>
<MinToIssue>100</MinToIssue>
<MultiplesToIssue>100</MultiplesToIssue>
<MatureSecurityAmount>65002000000.0</MatureSecurityAmount>
<CurrentlyOutstanding/>
<SOMAIncluded>N</SOMAIncluded>
<SOMAHoldings>11511000000.0</SOMAHoldings>
<FIMAIncluded>Y</FIMAIncluded>
<Series/>
<InterestRate/>
<FirstInterestPaymentDate/>
<StandardInterestPayment/>
<FrequencyInterestPayment>NONE</FrequencyInterestPayment>
<StrippableIndicator/>
<MinStripAmount/>
<CorpusCUSIP/>
<TINTCUSIP1/>
<TINTCUSIP2/>
<ReOpeningIndicator>N</ReOpeningIndicator>
<OriginalIssueDate/>
<BackDated/>
<BackDatedDate/>
<LongShortNormalCoupon/>
<LongShortCouponFirstIntPmt/>
<Callable/>
<CallDate/>
<InflationIndexSecurity>N</InflationIndexSecurity>
<RefCPIDatedDate/>
<IndexRatioOnIssueDate/>
<CPIBasePeriod/>
<TIINConversionFactor/>
<AccruedInterest/>
<DatedDate/>
<AnnouncedCUSIP/>
<UnadjustedPrice/>
<UnadjustedAccruedInterest/>
<ScheduledPurchasesInTD>772000000.0</ScheduledPurchasesInTD>
<AnnouncementPDFName>A_20080403_1.pdf</AnnouncementPDFName>
<OriginalDatedDate/>
<AdjustedAmountCurrentlyOutstanding/>
<NLPExclusionAmount>0.0</NLPExclusionAmount>
<MaximumNonCompAward>5000000.0</MaximumNonCompAward>
<AdjustedAccruedInterest/>
</AuctionAnnouncement>
</bpd:AuctionData>')


SELECT v FROM xml_demo;


SELECT v:"@" FROM xml_demo;



-- query root elements.

SELECT v:"$" FROM xml_demo;

-- another way
SELECT XMLGET(v, 'AuctionAnnouncement', 0) FROM xml_demo;


-- like json
SELECT XMLGET(v, 'AuctionAnnouncement', 0):"$" FROM xml_demo;



SELECT
auction_announcement.index as auction_contents_index,
auction_announcement.value as auction_contents_value
FROM xml_demo,
LATERAL FLATTEN(to_array(xml_demo.v:"$" )) xml_doc,
LATERAL FLATTEN(to_array(xml_doc.VALUE:"$" )) auction_announcement;


-- Recomended method

SELECT 
XMLGET(value, 'SecurityType' ):"$" as "Security Type",
XMLGET( value, 'MaturityDate' ):"$" as "Maturity Date",
XMLGET( value, 'OfferingAmount' ):"$" as "Offering Amount",
XMLGET( value, 'MatureSecurityAmount' ):"$" as "Mature Security Amount"
FROM xml_demo,
LATERAL FLATTEN(to_array(xml_demo.v:"$" )) auction_announcement;


```
