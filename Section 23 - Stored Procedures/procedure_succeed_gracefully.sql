create or replace procedure column_fill_rate(TABLE_NAME varchar)
  returns VARIANT --NOT NULL
  language javascript
  as     
  $$  
  
    // Input check
  
    var input_pattern = "select rlike('" +TABLE_NAME +"','[a-zA-Z0-9_]+')"
    var statement0 = snowflake.createStatement( {sqlText: input_pattern} );

    //return input_pattern;
    var result_set0 = statement0.execute();
    result_set0.next();
    reg_status = result_set0.getColumnValue(1)

    //return reg_status;
    
     
    if (reg_status == false){
     return  TABLE_NAME +" is not a table "}
     
      
  
  try {
    var my_sql_command = "select COUNT(*) CNT from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    result_set1.next();
     } 
   catch (err) 
     {
     
     return "Failed: " + err; 
     
     }
    var cnt = result_set1.getColumnValue(1);
    
    
    if (cnt == 0){
     return  TABLE_NAME +" is empty "}
     

    try { 
    var my_sql_command2 = "select * from "+ TABLE_NAME +" LIMIT 10 ;"
    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
    var result_set2 = statement2.execute();
    }
    
    catch (err)
    
    {
    
    return "Failed: " + err;
    
    }
    
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
    
    return "Failed: " + err;
    
    }
 
  // return table_as_json; 
  $$
  ;
