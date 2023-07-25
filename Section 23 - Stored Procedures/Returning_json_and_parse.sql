/**************Level-2****************/


-- Before calculating the fill rate let's fix our out put structure.

-- We are expecting output something like below,

--COLUMN_NAME	FILL_RATE
--    ABC	    0.98
--    DEF	    0.81
--    GHK	    0.27

-- Now let's see how to mock this output.


/*{
  "key1": [
    {
      "ColumnName": "ABC",
      "column_value": 0.98
    },
    {
      "ColumnName": "DEF",
      "column_value": 0.81
    }
    ]
}*/

create or replace transient table json_tbl ( fill_rate VARIANT) 

insert into json_tbl
select
 parse_json(
'{
  "key1": [
    {
      "ColumnName": "ABC",
      "column_value": 0.98
    },
    {
      "ColumnName": "DEF",
      "column_value": 0.81
    }
    ]
}')


select 
f.value:ColumnName,f.value:column_value
from json_tbl, table(flatten(fill_rate:key1)) f

create or replace procedure column_fill_rate_output_sturcture(TABLE_NAME varchar)
  returns VARIANT NOT NULL
  language javascript
  as     
  $$  
  
    var array_of_rows = [];
    row_as_json = {};
    
    var my_sql_command = "select floor(9/10) ABC,floor(8/10) DEF from "+ TABLE_NAME +";"
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    // result_set1.next()
    
    ColumnName =   result_set1.getColumnName(1);
    column_value = result_set1.getColumnValue(1);
    
    row_as_json = { ColumnName : ColumnName ,column_value : column_value}
          
    array_of_rows.push(row_as_json)
           
    ColumnName =  result_set1.getColumnName(2);
    column_value = result_set1.getColumnValue(2);
    
    row_as_json = { ColumnName : ColumnName ,column_value : column_value}
          
    array_of_rows.push(row_as_json)
    
    table_as_json = { "key1" : array_of_rows };
 
  return table_as_json; 
  $$
  ;

call column_fill_rate_output_sturcture('CUSTOMER')


select 
f.value:ColumnName,f.value:column_value
from json_tbl, table(flatten(fill_rate:key1)) f

select 
f.value:ColumnName,f.value:column_value
from TABLE(RESULT_SCAN(LAST_QUERY_ID())) AS res, table(flatten(COLUMN_FILL_RATE_OUTPUT_STURCTURE:key1)) f



