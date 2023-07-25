/*** create role and user ********************/
/**** execute using security admin role ******/

create role sandbox;

create user developer password='abc123' default_role = sandbox must_change_password = true;

/**********************/

grant role sandbox to user developer;
grant usage on warehouse compute_wh to role sandbox;


GRANT USAGE ON DATABASE DEMO_DB_OWNER TO ROLE sandbox;
GRANT USAGE ON SCHEMA DEMO_DB_OWNER.PUBLIC TO ROLE sandbox;

grant usage on procedure clone_table_owner(VARCHAR,VARCHAR) to role sandbox

grant usage on procedure create_sample_tbl(STRING,  STRING ,  STRING ,  STRING, STRING, STRING) to role sandbox

grant usage on procedure clone_table_caller(VARCHAR,VARCHAR) to role sandbox


show procedures 

desc procedure clone_table_owner(VARCHAR,VARCHAR)

create table test_clone ( name string, age number)
insert into test_clone
select 'john','15'

insert into test_clone
select 'mike','22'

select * from test_clone

insert into test_clone
select 'mike','22'


CREATE or replace PROCEDURE clone_table_owner(SRC_TABLE_NAME STRING,CLONE_TABLE_NAME STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS $$
 
    var sql_stmt = "CREATE TRANSIENT TABLE DEMO_DB.PUBLIC."+CLONE_TABLE_NAME+" CLONE"+" DEMO_DB_OWNER.PUBLIC."+SRC_TABLE_NAME
    var clone_stmt = snowflake.createStatement( {sqlText: sql_stmt} );
    clone_stmt.execute();
    
    var grant = "GRANT SELECT ON TABLE DEMO_DB.PUBLIC."+CLONE_TABLE_NAME +" TO ROLE SANDBOX"
    //return grant
    var grant_usage = snowflake.createStatement( {sqlText: grant} );
    grant_usage.execute();
 $$
;

call clone_table_caller_1('TEST_CLONE' ,'CLONE10_TEST_CLONE')

CREATE or replace PROCEDURE clone_table_caller(SRC_TABLE_NAME STRING,CLONE_TABLE_NAME STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS $$
 
    var sql_stmt = "CREATE TRANSIENT TABLE DEMO_DB.PUBLIC."+CLONE_TABLE_NAME+" CLONE"+" DEMO_DB_OWNER.PUBLIC."+SRC_TABLE_NAME
    var clone_stmt = snowflake.createStatement( {sqlText: sql_stmt} );
    clone_stmt.execute();
    
    var grant = "GRANT ALL ON TABLE DEMO_DB.PUBLIC."+CLONE_TABLE_NAME +" TO ROLE SANDBOX"
    //return grant
    var grant_usage = snowflake.createStatement( {sqlText: grant} );
    grant_usage.execute();
 $$
;

show procedures

call demo_db_owner.public.clone_table_owner('TEST_CLONE' ,'CLONE_TEST_CLONE')

call demo_db_owner.public.clone_table_caller('TEST_CLONE' ,'CLONE1_TEST_CLONE')

