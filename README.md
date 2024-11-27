# s3tosnowflake

This project uses the s3 , glue , lambda in aws and snowpipe, external stages, tasks , stored procedure concepts in snowflake

first , I have created customer-booking-data-pipeline repository in s3 , then  added the raw data files (booking_details_raw and customer_details_raw)

then to add both file I have created the glue job

then to execute it I have created lambda function , this lambda function execute the glue job 
