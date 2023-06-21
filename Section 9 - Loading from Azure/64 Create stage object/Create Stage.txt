  
---- Create file format & stage objects ----

-- create file format
create or replace file format demo_db.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage demo_db.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://storageaccountsnow.blob.core.windows.net/snowflakecsv'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @demo_db.public.stage_azure;
