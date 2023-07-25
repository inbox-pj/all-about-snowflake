/********************************  Set session variables ******************************/


CREATE PROCEDURE set_variable_caller()
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
        var rs = snowflake.execute( {sqlText: "SET SESSION_VAR_caller = 51"} );
        rs.next();

    $$
    ;

call set_variable_caller()
    
select $SESSION_VAR_caller
    
CREATE or replace PROCEDURE set_variable_owner()
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS
    $$
        var rs = snowflake.execute( {sqlText: "SET SESSION_VAR_owner = 51"} );
        rs.next();

    $$
    ;
    
call set_variable_owner()

select $SESSION_VAR_owner

UNSET SESSION_VAR_ZYXW



/*********** Check for procedure details ****************/

desc procedure  clone_table_owner(varchar,varchar)

/************ Check procedure execution details ************/

grant monitor on warehouse compute_wh to role sandbox

revoke monitor on warehouse compute_wh from role sandbox
