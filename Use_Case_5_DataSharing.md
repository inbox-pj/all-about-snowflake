## Data Sharing


### * Share Data with Another Snowflake User



#### On Producer Account Side
---------------------------
```sql
-- Create a share object
CREATE OR REPLACE SHARE ORDERS_SHARE;

---- Setup Grants ----
-- Grant usage on database
GRANT USAGE ON DATABASE DATA_S TO SHARE ORDERS_SHARE; 

-- Grant usage on schema
GRANT USAGE ON SCHEMA DATA_S.PUBLIC TO SHARE ORDERS_SHARE; 

-- Grant SELECT on table
GRANT SELECT ON TABLE DATA_S.PUBLIC.ORDERS TO SHARE ORDERS_SHARE; 

-- Grant SELECT on all tables in schema/database
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_S.PUBLIC TO SHARE ORDERS_SHARE;
GRANT SELECT ON ALL TABLES IN DATABASE DATA_S TO SHARE ORDERS_SHARE;


-- Create VIEW -- 
CREATE OR REPLACE VIEW DATA_S.PUBLIC.ORDERS_VIEW AS
SELECT 
FIRST_NAME,
LAST_NAME,
EMAIL
FROM DATA_S.PUBLIC.ORDERS
WHERE JOB != 'DATA SCIENTIST'; 


-- Grant usage & SELECT (Non-Secure Way) --
GRANT USAGE ON DATABASE DATA_S TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA DATA_S.PUBLIC TO ROLE PUBLIC;
GRANT SELECT ON TABLE DATA_S.PUBLIC.ORDERS TO ROLE PUBLIC;
GRANT SELECT ON VIEW DATA_S.PUBLIC.ORDERS_VIEW TO ROLE PUBLIC;


-- Create SECURE VIEW (Secure Way) -- 
CREATE OR REPLACE SECURE VIEW DATA_S.PUBLIC.ORDERS_VIEW_SECURE AS
SELECT 
FIRST_NAME,
LAST_NAME,
EMAIL
FROM DATA_S.PUBLIC.ORDERS
WHERE JOB != 'DATA SCIENTIST' 

GRANT SELECT ON VIEW DATA_S.PUBLIC.ORDERS_VIEW_SECURE TO ROLE PUBLIC;


-- Grant select on view
GRANT SELECT ON VIEW  DATA_S.PUBLIC.ORDERS_VIEW TO SHARE ORDERS_SHARE;
GRANT SELECT ON VIEW  DATA_S.PUBLIC.ORDERS_VIEW_SECURE TO SHARE ORDERS_SHARE;


-- Validate Grants
SHOW GRANTS TO SHARE ORDERS_SHARE;

---- Add Consumer Account ----
ALTER SHARE ORDERS_SHARE ADD ACCOUNT=<consumer-account-id>;

```


#### On Consumer Account Side
---------------------------
```sql
-- Show all shares (consumers and producers)
SHOW SHARES;

-- See details on Share
DESC <producer-account-id>.ORDERS_SHARE;


-- Create a database in Consumer using share
CREATE DATABASE DATA_S FROM SHARE <producer-account-id>.ORDERS_SHARE;

-- Validate Table Access
SELECT * FROM DATA_S.PUBLIC.ORDERS;
```


###  * Share Data with Reader's Account

#### On Producer Account Side
---------------------------
```sql
-- Create Reader Account
CREATE MANAGED ACCOUNT pjaiswal_account
ADMIN_NAME = pjaiswal_admin,
ADMIN_PASSWORD = 'password',
TYPE = READER;

-- Show accounts
SHOW MANAGED ACCOUNTS;

-- Share the data
ALTER SHARE ORDERS_SHARE ADD ACCOUNT = <reader-account-id>;
-- override the Business Critical to non-business critical account's share restriction
ALTER SHARE ORDERS_SHARE ADD ACCOUNT =  <reader-account-id> SHARE_RESTRICTIONS=false;
```


#### On Consumer Account Side (if consumer want to share the shared database with another user)
---------------------------
```sql
-- Create and set up users --

-- Create user
CREATE USER MYRIAM PASSWORD = 'difficult_passw@ord=123'

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE READ_WH TO ROLE PUBLIC;

-- Grating privileges on a Shared Database for other users
GRANT IMPORTED PRIVILEGES ON DATABASE DATA_SHARE_DB TO REOLE PUBLIC;
```
