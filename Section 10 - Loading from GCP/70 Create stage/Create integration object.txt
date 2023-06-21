-- create integration object that contains the access information
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket/path', 'gcs://bucket/path2');

  
-- Describe integration object to provide access
DESC STORAGE integration gcp_integration;
