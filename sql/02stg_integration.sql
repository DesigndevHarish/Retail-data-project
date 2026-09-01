use database e_com_s3;



CREATE OR REPLACE STORAGE INTEGRATION S3_STREAMING_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::480749289287:role/SnowflakeS3StreamingRole'
STORAGE_ALLOWED_LOCATIONS = (
    's3://de-streaming-json-lake-2026-480749289287-ap-south-1-an/raw/orders/',
    's3://de-streaming-json-lake-2026-480749289287-ap-south-1-an/historical/retail_events/'
);


CREATE OR REPLACE STAGE bronze.STG_RETAIL_HISTORICAL
    URL = 's3://de-streaming-json-lake-2026-480749289287-ap-south-1-an/historical/retail_events/'
    STORAGE_INTEGRATION = S3_STREAMING_INTEGRATION
    FILE_FORMAT = (TYPE = JSON);