/************************************** Level 4 ********************************************/


create transient table CUSTOMER_TRANSPOSED (COLUMN_NAME varchar ,COLUMN_VALUE varchar)

truncate table CUSTOMER_TRANSPOSED

call column_fill_rate_bind_var('CUSTOMER')

select * from CUSTOMER_TRANSPOSED

create or replace procedure column_fill_rate_bind_var(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select * from "+ TABLE_NAME +" LIMIT 10;"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    
    var insert_cmd = "INSERT INTO CUSTOMER_TRANSPOSED VALUES(:1,:2)"
    
      while (result_set1.next())  {
        // Put each row in a variable of type JSON.
 
        // For each column in the row...
        for (var col_num = 0; col_num < result_set1.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set1.getColumnName(col_num+1);
          var col_value = result_set1.getColumnValue(col_num+1);
          
          if (col_name=='C_NAME') {
          
          col_value='JOHN'
          }
          
          else
          {
          
          col_value
          
          }
          
          row_as_json = { ColumnName : col_name ,column_value : col_value}
          
          array_of_rows.push(row_as_json)
          
          snowflake.execute(
        { 
        sqlText: insert_cmd, 
        binds: [ col_name,col_value] 
        }
        ); 
          
          
          
          }
          
        }   
        
  table_as_json = { "key1" : array_of_rows };
 
   return table_as_json; 
  $$
  ;

call column_fill_rate_bind_var('CUSTOMER')

select * from CUSTOMER_TRANSPOSED


TRUNCATE TABLE CUSTOMER_TRANSPOSED



create or replace procedure column_fill_rate_bind_var(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select * from "+ TABLE_NAME +" LIMIT 10;"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    
    var insert_cmd = "INSERT INTO CUSTOMER_TRANSPOSED VALUES(:1,:2)"
    
      while (result_set1.next())  {
        // Put each row in a variable of type JSON.
 
        // For each column in the row...
        for (var col_num = 0; col_num < result_set1.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set1.getColumnName(col_num+1);
          var col_value = result_set1.getColumnValue(col_num+1);
          
          if (col_name=='C_NAME') {
          
          col_value='JOHN'
          }
          
          else
          {
          
          col_value
          
          }
          
          row_as_json = { ColumnName : col_name ,column_value : col_value}
          
          array_of_rows.push(row_as_json)
          
          snowflake.execute(
        { 
        sqlText: "INSERT INTO CUSTOMER_TRANSPOSED VALUES(?,?)", 
        binds: [ col_name,col_value] 
        }
        ); 
             
          }
          
        }   
        
  table_as_json = { "key1" : array_of_rows };
 
   return table_as_json; 
  $$
  ;
