--- Try to execute these commands in the Gitpod-- Please check Module-2 intro for more information

create or replace table demo_db.public.emp_basic_1 (
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);

put file:///workspace/snowflake/Module-2/Snowflake-stages/Data/Employee/employees0*.csv 
@demo_db.public.%emp_basic_1;


select * from demo_db.public.emp_basic_1;

copy into demo_db.public.emp_basic_1
from @demo_db.public.%emp_basic_1
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*employees0[1-5].csv.gz'
on_error = 'skip_file';

create database control_db;
create schema file_formats;
create or replace file format control_db.file_formats.my_csv_format
type = csv field_optionally_enclosed_by='"' field_delimiter = ',' 
null_if = ('NULL', 'null') empty_field_as_null = true compression = gzip;

DESC FILE FORMAT control_db.file_formats.my_csv_format;

copy into demo_db.public.emp_basic_1
from @demo_db.public.%emp_basic_1
file_format = control_db.file_formats.my_csv_format
pattern = '.*employees0[1-5].csv.gz'
on_error = 'skip_file';

-- Load data to table sence 2

create or replace table demo_db.public.emp_basic_2 (
         first_name string ,
         last_name string ,
         email string 
);

put file:///workspace/snowflake/Module-2/Snowflake-stages/Data/Employee/employees0*.csv @demo_db.public.%emp_basic_2;

copy into demo_db.public.emp_basic_2
from (select  t.$1 , t.$2 , t.$3 from @demo_db.public.%emp_basic_2 t)
file_format = control_db.file_formats.my_csv_format
pattern = '.*employees0[1-5].csv.gz'
on_error = 'skip_file';

select * from demo_db.public.emp_basic_2 ;

-- Loading data scene 3

# Create staging area.

create or replace table demo_db.public.emp_basic_local (
         file_name string,
         fie_row_number string,
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);


# Upload data to stagig area.

put file:///workspace/snowflake/Module-2/Snowflake-stages/Data/Employee/employees0*.csv 
@demo_db.public.%emp_basic_local;

# Copy data from staging area to snowflake.

copy into demo_db.public.emp_basic_local
from (select metadata$filename, metadata$file_row_number, t.$1 , t.$2 , t.$3 , t.$4 , t.$5 , t.$6 
      from @demo_db.public.%emp_basic_local t)
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*employees0[1-5].csv.gz'
on_error = 'skip_file';


select * from demo_db.public.emp_basic_local;
select count(*) from @demo_db.public.%emp_basic_local
 (file_format => control_db.file_formats.my_csv_format)


select metadata$filename, metadata$file_row_number,$1 , $2 , $3 , $4 , $5 , $6 
      from @demo_db.public.%emp_basic_local 
      (file_format => control_db.file_formats.my_csv_format)
minus
select * from demo_db.public.emp_basic_local


select count(*) 
from  (select metadata$filename, metadata$file_row_number,$1 , $2 , $3 , $4 , $5 , $6 
      from @demo_db.public.%emp_basic_local 
      (file_format => control_db.file_formats.my_csv_format  ))
      

select split_part(file_name,'/',2) from demo_db.public.emp_basic_local;