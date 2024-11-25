CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = <'AWS_ROLE_ARN'>
  STORAGE_ALLOWED_LOCATIONS = ('*')


DESC INTEGRATION my_s3_integration;

CREATE STAGE my_s3_stage
    URL = 's3://customer-booking-data-pipeline/renamed_processed_data/'
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
    STORAGE_INTEGRATION = my_s3_integration;


CREATE PIPE my_snowpipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO CUSTOMER_BOOKINGS_STAGING
    FROM @my_s3_stage
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';


SHOW PIPES;

select SYSTEM$PIPE_STATUS('my_snowpipe');
