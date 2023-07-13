
-- Please try to execute these commands from GITPOD. Refer Module2 introduction for more information.
-- Unload data

-- Unload data scene 1

SELECT * FROM demo_db.public.emp_basic_local

copy into @demo_db.public.%emp_basic_local
from demo_db.public.emp_basic_local
file_format = (type = csv field_optionally_enclosed_by='"')
--on_error = 'skip_file';

get @demo_db.public.%emp_basic_local file:///workspace/snowflake/Module-2/Snowflake-stages/Data/Employee/unload/;

-- Unload data scene 2

copy into @demo_db.public.%emp_basic_local/test_folder/test_
from (select first_name, last_name,email from demo_db.public.emp_basic_local)
file_format = (type = csv field_optionally_enclosed_by='"')
OVERWRITE=TRUE;
--on_error = 'skip_file';