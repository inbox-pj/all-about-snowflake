/************************** LEVEL 3 **********************************/


create or replace procedure column_fill_rate_loops(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select count(*) ABC,count(*) DEF from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
//    result_set1.next()
    
//    ColumnName =  result_set1.getColumnName(1);
//    column_value = result_set1.getColumnValue(1);
//    
//    row_as_json = { ColumnName : ColumnName ,column_value : 9/10}
//          
//    array_of_rows.push(row_as_json)
//           
//    ColumnName =  result_set1.getColumnName(2);
//    column_value = result_set1.getColumnValue(2);
//    
//    row_as_json = { ColumnName : ColumnName ,column_value : 8/10}
//          
//    array_of_rows.push(row_as_json)
//    
//    table_as_json = { "key1" : array_of_rows };

      while (result_set1.next())  {
        // Put each row in a variable of type JSON.
 
        // For each column in the row...
        for (var col_num = 0; col_num < result_set1.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set1.getColumnName(col_num+1);
          var col_value = result_set1.getColumnValue(col_num+1);
          
          row_as_json = { ColumnName : col_name ,column_value : col_value}
          
          array_of_rows.push(row_as_json)
          }
          
        }   
        
  table_as_json = { "key1" : array_of_rows };
 
  // return table_as_json; 
  return result_set1.next();
  $$
  ;

call column_fill_rate_loops('CUSTOMER')


/************** Add if else clause **************************/
/************** Converting table to json format *************/

create or replace procedure column_fill_rate_loops_if_else(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select * from "+ TABLE_NAME +" LIMIT 10;"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    
      while (result_set1.next())  {
        // Put each row in a variable of type JSON.
 
        // For each column in the row...
        for (var col_num = 0; col_num < result_set1.getColumnCount(); col_num = col_num + 1) {
          var col_name =result_set1.getColumnName(col_num+1);
          var col_value = result_set1.getColumnValue(col_num+1);
          
          row_as_json = { ColumnName : col_name ,column_value : col_value}
          
          array_of_rows.push(row_as_json)
          }
          
        }   
        
  table_as_json = { "key1" : array_of_rows };
 
   return table_as_json; 
  $$
  ;

call column_fill_rate_loops_if_else('CUSTOMER')



create or replace procedure column_fill_rate_loops_if_else(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select * from "+ TABLE_NAME +" LIMIT 10;"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    
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
          }
          
        }   
        
  table_as_json = { "key1" : array_of_rows };
 
   return table_as_json; 
  $$
  ;

call column_fill_rate_loops_if_else('CUSTOMER')
