

```sql

		-- External tables in snowflake

		CREATE OR REPLACE EXTERNAL TABLE ext_table
		 WITH LOCATION = @control_db.external_stages.my_s3_stage
		 FILE_FORMAT = (TYPE = CSV);

		desc table ext_table

		-- Get metadata information
		select *
		from table(information_schema.external_table_files(TABLE_NAME=>'emp_ext_table'));

		select *
		from table(information_schema.external_table_file_registration_history(TABLE_NAME=>'emp_ext_table'));


		-- Manual Refresh
		ALTER EXTERNAL TABLE emp_ext_table REFRESH;


		-- Partition in external tables.(Filter data)
		create or replace external table emp_ext_table
		(
		file_name_part varchar AS SUBSTR(metadata$filename,5,11),
		first_name string as  (value:c1::string), 
		last_name string(20) as ( value:c2::string), 
		email string as (value:c3::string))
		PARTITION BY (file_name_part)
		WITH LOCATION =  @control_db.external_stages.my_s3_stage
		FILE_FORMAT = (TYPE = CSV);

		ALTER EXTERNAL TABLE emp_ext_table_partitions ADD PARTITION (customer_type='Gold_customer') location 'Gold_customer/'

		ALTER EXTERNAL TABLE emp_ext_table_partitions ADD PARTITION (customer_type='platinum_customer') location 'platinum_customer/'


		-- Auto refresh metadata(Auto refresh) 

		ALTER EXTERNAL TABLE emp_ext_table REFRESH;

		create or replace external table emp_ext_table
		(
		file_name_part varchar AS SUBSTR(metadata$filename,5,11),
		first_name string as  (value:c1::string), 
		last_name string(20) as ( value:c2::string), 
		email string as (value:c3::string))
		PARTITION BY (file_name_part)
		WITH LOCATION =  @control_db.external_stages.my_s3_stage
		FILE_FORMAT = (TYPE = CSV)
		auto_refresh=true;

```
