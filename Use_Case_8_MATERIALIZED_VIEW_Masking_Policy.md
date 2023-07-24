


## MATERIALIZED VIEW
------------------------
```sql
-- Create materialized view
CREATE OR REPLACE [SECURE] MATERIALIZED VIEW [IF NOT EXIST] ORDERS_MV
AS 
SELECT
YEAR(O_ORDERDATE) AS YEAR,
MAX(O_COMMENT) AS MAX_COMMENT,
MIN(O_COMMENT) AS MIN_COMMENT,
MAX(O_CLERK) AS MAX_CLERK,
MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE);


SHOW MATERIALIZED VIEWS;

DESC MATERIALIZED VIEWS ORDERS_MV;

-- check materialized_view_refresh_history
select * from table(information_schema.materialized_view_refresh_history())
```


## DYNAMIC DATA MASKING (COLUMN LEVEL SECURITY) - DAP (Data Access Policy)
### Compliance with data protection regulations: GDPR, HIPAA, or CCPA
--------------------------------------
```sql
-- Set up masking policy
create or replace masking policy phone 
    as (val varchar) returns varchar ->
        case        
            when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
            else '##-###-##'
        end;

create or replace masking policy phone as (val varchar) returns varchar ->
        case
            when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
            else CONCAT(LEFT(val,2),'*******')
        end;

create or replace masking policy names as (val varchar) returns varchar ->
        case
            when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
            else CONCAT(LEFT(val,2),'*******')
        end;

create or replace masking policy emails as (val varchar) returns varchar ->
		case
			  when current_role() in ('ANALYST_FULL') then val
			  when current_role() in ('ANALYST_MASKED') then regexp_replace(val,'.+\@','*****@') -- leave email domain unmasked
			  else '********'
		end;


create or replace masking policy sha2 as (val varchar) returns varchar ->
		case
			  when current_role() in ('ANALYST_FULL') then val
			  else sha2(val) -- return hash of the column value
		end;



-- Apply policy on a specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone 
SET MASKING POLICY PHONE;

-- Apply Masking policy in Table defination
CREATE or REPLACE TABLE CUSTOMERS (id int, phone varchar masking policy phone, email varchar masking policy emails, name varchar);


-- List and describe policies
DESC MASKING POLICY phone;
SHOW MASKING POLICIES;

-- Show columns with applied policies
SELECT * FROM table(information_schema.policy_references(policy_name=>'phone'));


-- Remove policy before replacing/dropping 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN email
UNSET MASKING POLICY;


-- Alter existing policies 
alter masking policy phone set body ->
	case        
 		when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
 		else '**-**-**'
	end;
```


## ROW ACCESS SECURITY

```sql

create table patient
(
Name varchar,
age integer,
icdcode varchar,
zip_code varchar,
city varchar,
provider_name varchar
)

create or replace row access policy patient_policy as (icdcode varchar) returns boolean ->
           CASE WHEN icdcode='F70' THEN FALSE ELSE TRUE END
;

create or replace row access policy patient_policy as (icdcode varchar) returns boolean ->
      CASE WHEN 'SYSADMIN'=current_role() THEN TRUE ELSE
                                                    CASE WHEN icdcode='F70' THEN FALSE ELSE TRUE END
                                                    END
;

alter table claims.pharmacy.patient add row access policy patient_policy on (icdcode);
alter table claims.pharmacy.patient add row access policy governance.row_access_policy.patient_policy on (icdcode,zip_code);

drop row access policy  governance.row_access_policy.patient_policy;

select *
from table(information_schema.policy_references(policy_name => 'patient_policy'));


select *
  from table(information_schema.policy_references(ref_entity_name => 'claims.pharmacy.patient', ref_entity_domain => 'table'));



```

