/************************************** Level 5 ********************************************/

--COLUMN_NAME	FILL_RATE
--    ABC	    0.98
--    DEF	    0.81
--    GHK	    0.27

call column_fill_rate('CUSTOMER_1')

select * from CUSTOMER_1

select * from Table_fill_rate

CREATE OR REPLACE TABLE TABLE_FILL_RATE
(
COLUMN_NAME VARCHAR,
FILL_RATE NUMBER(38,2)
)

truncate table Table_fill_rate

create transient table CUSTOMER_1 CLONE CUSTOMER

select * from CUSTOMER_1

update CUSTOMER_1
SET C_ADDRESS=NULL
WHERE C_MKTSEGMENT='AUTOMOBILE'

create or replace procedure column_fill_rate(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
    var my_sql_command = "select COUNT(*) CNT from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next();
    
    var my_sql_command2 = "select * from "+ TABLE_NAME +" LIMIT 10 ;"
    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
    var result_set2 = statement2.execute();
    
    var cnt = result_set1.getColumnValue(1);
    var array_of_rows = [];
    
    
    var row_num = 0;
    row_as_json = {};
    
    var column_name;
    
      while (result_set2.next())  {
        // Put each row in a variable of type JSON.
         
        // For each column in the row...
        for (var col_num = 0; col_num < result_set2.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set2.getColumnName(col_num+1);
          
         var my_sql_command3 = "select round(count(*)/"+cnt+",2) RW_CNT from "+ TABLE_NAME +" where "+col_name+" IS NOT NULL;"
         
         // return cnt;
         var statement3 = snowflake.createStatement( {sqlText: my_sql_command3} );
      
           result_set3 = statement3.execute();
          
           result_set3.next();
           var col_value = result_set3.getColumnValue(1);
          
           row_as_json = { ColumnName : col_name ,column_value : col_value}
          
           array_of_rows.push(row_as_json)
          }
          
        }   
        
      table_as_json = { "key1" : array_of_rows };
      
         for (var col_num = 0; col_num < result_set2.getColumnCount(); col_num = col_num + 1) {
         
         var my_sql_command4 = "insert into Table_fill_rate values (:1 , :2 ) "
      
         var statement4 = snowflake.createStatement( {sqlText: my_sql_command4,binds: [table_as_json.key1[col_num].ColumnName,table_as_json.key1[col_num].column_value]} );
      
         statement4.execute();
         
         }
 
  return table_as_json; 
  $$
  ;


-- Let's put some scenarios for our procedure and test it

select * from customer

-- What if my table is empty

call column_fill_rate('CUSTOMER_2')

-- What if full column of table is null.

call column_fill_rate('CUSTOMER_3')

truncate table Table_fill_rate

select * from Table_fill_rate


-- What if you pass table which do not exist

call column_fill_rate('CUSTOMER_5')


-- What if the column data type is varient or json

truncate table Table_fill_rate

select * from Table_fill_rate

call column_fill_rate('json_tbl')


select * from json_tbl


-- What if my procedure fails inbetween due to some external reason.

call column_fill_rate('CUSTOMER')

truncate table Table_fill_rate

select * from Table_fill_rate










create transient table CUSTOMER_2 CLONE CUSTOMER

create transient table CUSTOMER_3 CLONE CUSTOMER

update CUSTOMER_3
SET C_ADDRESS=NULL

select 0/10




truncate table CUSTOMER_2
call column_fill_rate('json_tbl')

-- json_tbl

select * from json_tbl

truncate table Table_fill_rate

select * from Table_fill_rate

select count(*) from CUSTOMER_3

select count(C_ADDRESS) from CUSTOMER_1

desc table CUSTOMER_3


create or replace procedure column_fill_rate(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
    var my_sql_command = "select COUNT(*) CNT from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next();
    
    var my_sql_command2 = "select * from "+ TABLE_NAME +" LIMIT 10 ;"
    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
    var result_set2 = statement2.execute();
    
    return result_set2.getColumnType(1);
    
    var cnt = result_set1.getColumnValue(1);
    var array_of_rows = [];
    
    
    var row_num = 0;
    row_as_json = {};
    
    var column_name;
    
      while (result_set2.next())  {
        // Put each row in a variable of type JSON.
         
        // For each column in the row...
        for (var col_num = 0; col_num < result_set2.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set2.getColumnName(col_num+1);
          
         var my_sql_command3 = "select round(count(*)/"+cnt+",2) RW_CNT from "+ TABLE_NAME +" where "+col_name+" IS NOT NULL;"
         
         // return cnt;
         var statement3 = snowflake.createStatement( {sqlText: my_sql_command3} );
      
           result_set3 = statement3.execute();
          
           result_set3.next();
           var col_value = result_set3.getColumnValue(1);
          
           row_as_json = { ColumnName : col_name ,column_value : col_value}
          
           array_of_rows.push(row_as_json)
          }
          
        }   
        
      // return array_of_rows
        
      table_as_json = { "key1" : array_of_rows };
      
      // return  table_as_json;
      
         for (var col_num = 0; col_num < result_set2.getColumnCount(); col_num = col_num + 1) {
         
         var my_sql_command4 = "insert into Table_fill_rate values (:1 , :2 ) "
      
         var statement4 = snowflake.createStatement( {sqlText: my_sql_command4,binds: [table_as_json.key1[col_num].ColumnName,table_as_json.key1[col_num].column_value]} );
      
         statement4.execute();
         
         }
 
  // return table_as_json; 
  $$
  ;
