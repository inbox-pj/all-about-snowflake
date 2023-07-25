/**************Level-1****************/

-- Pass table name to procedure and get fill rate of all columns.

--COLUMN_NAME	FILL_RATE
--    ABC	    0.98
--    DEF	    0.81
--    GHK	    0.27

-- To calculate fill rate first we divide the total record count from column count negating null values.

-- (Column not null count)/(Total record count)

-- In the following procedure we will pass table name. Get total record count and return the record count.

-- We will consider table CREATE TRANSIENT TABLE CUSTOMER as select * from  "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER";



create or replace procedure column_fill_rate_1(TABLE_NAME varchar)
  returns varchar
  language javascript
  as     
  $$  
    var my_sql_command = "select count(*) from "+ TABLE_NAME +";"
    
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next()
    
    row_count = result_set1.getColumnValue(1);
    
       
  return row_count; 
  $$
  ;
  
 call column_fill_rate_1('CUSTOMER')
 select count(*) from CUSTOMER
  
create or replace procedure column_fill_rate_1(TABLE_NAME varchar)
  returns FLOAT
  language javascript
  as     
  $$  
    var my_sql_command = "select count(*) from "+ TABLE_NAME +";"
    
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next()
    
    row_count = result_set1.getColumnValue(1);
    
       
  return row_count; 
  $$
  ;
  
call column_fill_rate_1('CUSTOMER')

-- We got our result. But we need to understand about snowflake.createstatement , 
-- execute and result_set objects which are helpig us to perform this operation.

-- Let's check how the statement object looks like.


create or replace procedure column_fill_rate_stmt_obj(TABLE_NAME varchar)
  -- returns varchar
 returns VARIANT NOT NULL
language javascript
as     
$$  
  var my_sql_command = "select count(*) from "+ TABLE_NAME +";"
  
  var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
  var result_set1 = statement1.execute();
  result_set1.next()
  
  row_count = result_set1.getColumnValue(1);
  
     
return statement1; 
$$
;
  
call column_fill_rate_stmt_obj('CUSTOMER')

-- Now let's see how our result object looks like


create or replace procedure column_fill_rate_result_obj(TABLE_NAME varchar)
-- returns varchar
returns VARIANT NOT NULL
language javascript
as     
$$  
  var my_sql_command = "select count(*) from "+ TABLE_NAME +";"
  
  var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
  var result_set1 = statement1.execute();
  result_set1.next()
  
  row_count = result_set1.getColumnValue(1);
  
     
return result_set1; 
$$
;

call column_fill_rate_result_obj('CUSTOMER')


#Argument names are case-insensitive in the SQL portion of the stored procedure code, but are case-sensitive in the JavaScript portion.

call column_fill_rate_result_obj('Customer')

#Using uppercase identifiers (especially argument names) consistently across your SQL statements and JavaScript code tends to reduce silent errors

CREATE OR REPLACE PROCEDURE f(argument1 VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$

var local_variable2 = ARGUMENT1;  // Correct

var local_variable1 = ARGUMENT1;  // Incorrect

return local_variable2
$$;

call f('prad')
