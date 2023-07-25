
CREATE OR REPLACE TABLE error_log (error_code string, error_state string, error_message string, stack_trace string);


set do_log = true;  -- true to enable logging, false (or undefined) to disable
set log_table = 'my_log_table';  -- The name of the temp table where log messages go.


CREATE or replace PROCEDURE do_log(MSG STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS $$

    // See if we should log - checks for session variable do_log = true.
    try {
       var stmt = snowflake.createStatement( { sqlText: `select $do_log` } ).execute();
    } catch (ERROR){
       return ''; //swallow the error, variable not set so don't log
    }
    stmt.next();
    if (stmt.getColumnValue(1)==true){ //if the value is anything other than true, don't log
       try {
           snowflake.createStatement( { sqlText: `create temp table identifier ($log_table) if not exists (ts number, msg string)`} ).execute();
           snowflake.createStatement( { sqlText: `insert into identifier ($log_table) values (:1, :2)`, binds:[Date.now(), MSG] } ).execute();
       } catch (ERROR){
           throw ERROR;
       }
    }
 $$
;





create or replace procedure column_fill_rate(TABLE_NAME varchar)
  returns VARIANT --NOT NULL
  language javascript
  EXECUTE AS CALLER
  as     
  $$  

     var accumulated_log_messages = '';

     function log(msg) {
        snowflake.createStatement( { sqlText: `call do_log(:1)`, binds:[msg] } ).execute();
        }
  
  // Input check
  
    var input_pattern = "select rlike('" +TABLE_NAME +"','[a-zA-Z0-9_]+')"
    var statement0 = snowflake.createStatement( {sqlText: input_pattern} );
    //return input_pattern;
    var result_set0 = statement0.execute();
    result_set0.next();
    
    reg_status = result_set0.getColumnValue(1)

    accumulated_log_messages += 'regular expression result: '+reg_status+ '\n';
    //return reg_status;
    
  
    if (reg_status == false){
     throw  TABLE_NAME +" is not a table "}
  
  
  try {
    var my_sql_command = "select COUNT(*) CNT from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next();
   
     } 
   catch (err) 
     {
     
     snowflake.execute({
      sqlText: `insert into error_log VALUES (?,?,?,?)`
      ,binds: [err.code, err.state, err.message, err.stackTraceTxt]
      });
 
      if (accumulated_log_messages != '') {
        log(accumulated_log_messages)
        }
      throw err.message;

     }

    

     var cnt = result_set1.getColumnValue(1);
     if (cnt == 0){
     throw  TABLE_NAME +" is empty "} 
     
     accumulated_log_messages += 'count of records: '+cnt+ '\n';
     

    try { 
    var my_sql_command2 = "select * from "+ TABLE_NAME +" LIMIT 10 ;"
    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
    var result_set2 = statement2.execute();
    }
    
    catch (err)
    
    {
    
    snowflake.execute({
      sqlText: `insert into error_log VALUES (?,?,?,?)`
      ,binds: [err.code, err.state, err.message, err.stackTraceTxt]
      });
    
    if (accumulated_log_messages != '') {
        log(accumulated_log_messages)
        }

    throw "Failed: when trying to get schema of the table" ; 
    
    }



    
    accumulated_log_messages += 'column type of result set 2: '+result_set2.getColumnType(1)+ '\n';
    //return result_set2.getColumnType(1);
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
         
         //accumulated_log_messages += 'count of records: '+cnt;
         // return cnt;
         var statement3 = snowflake.createStatement( {sqlText: my_sql_command3} );
      
           result_set3 = statement3.execute();
          
           result_set3.next();
           var col_value = result_set3.getColumnValue(1);
          
           row_as_json = { ColumnName : col_name ,column_value : col_value}
          
           array_of_rows.push(row_as_json)
          }
          
        }   
       
      //accumulated_log_messages += 'array of records: '+array_of_rows+ '\n'; 
      // return array_of_rows
        
      table_as_json = { "key1" : array_of_rows };
      
      //return  table_as_json;
       
       try{
         for (var col_num = 0; col_num < result_set2.getColumnCount(); col_num = col_num + 1) {
         
         var my_sql_command4 = "insert into Table_fill_rate values (:1 , :2 ) "
      
         var statement4 = snowflake.createStatement( {sqlText: my_sql_command4,binds: [table_as_json.key1[col_num].ColumnName,table_as_json.key1[col_num].column_value]} );
      
          statement4.execute();
         
         }
       }
    catch (err)
    {
    
    throw "Failed: " + err;
    
    }

   finally {

           if (accumulated_log_messages != '') {
        log(accumulated_log_messages)
        }
}



 
  // return table_as_json; 
  $$
  ;
