
# **S3 to Snowflake Data Pipeline Project**

Welcome to my **Customer Booking Data Pipeline** project! 🚀 This project is designed to automate the processing and loading of customer booking data from AWS S3 to Snowflake. By using AWS Glue, Lambda, and Snowpipe, I’ve built an end-to-end solution to ensure smooth data transformation and loading 

## **Project Overview**

The goal of this project is to create a seamless data pipeline that processes raw customer booking data stored in AWS S3, transforms it using AWS Glue, and then loads the processed data into Snowflake for analysis. Here’s a breakdown of how everything works:

### **Key AWS Services Used:**
1. **S3 (Simple Storage Service)** - For storing raw data files and processed files.
2. **AWS Glue** - For transforming and cleaning the raw data.
3. **AWS Lambda** - For automating the Glue job execution and handling file renaming.
   
**Snowflake** - To store the final processed data and enable easy analysis.


## **The Flow of Data**

### 1. **Data Ingestion (Raw Data in S3)**

The pipeline starts with raw customer booking data stored in an S3 bucket (customer-booking-data-pipeline), in the **raw-data** folder. The files include:

- `customer_details_raw.csv` (Contains customer information)
- `booking_details_raw.csv` (Contains booking information)

In this setup, there are two main components: the Lambda function (CustomerBookingFunction.py) and the AWS Glue job (CustomerBooking.py). Both components work together to automate the data processing pipeline, which involves extracting data from raw sources, transforming it, and then renaming and storing the processed results in a different S3 location.

1. Lambda Function (CustomerBookingFunction.py)
The Lambda function is responsible for triggering the AWS Glue job and monitoring its status. Here's a breakdown of how it works:

Triggering the Glue Job: The function starts by initializing a connection to the AWS Glue service using the boto3 client. It then triggers the Glue job named CustomerBooking via the start_job_run API call. The function captures the JobRunId, which uniquely identifies this job run.

Job Status Monitoring: The Lambda function enters a loop where it repeatedly checks the status of the Glue job using the get_job_run API call. It checks the job's state every 30 seconds, printing updates on the job status. If the job state is SUCCEEDED, it moves to the next step of renaming the processed file. If the job fails or is stopped, the Lambda function raises an error and stops the process.

Renaming the Processed File: After the Glue job successfully completes, the Lambda function proceeds to call a helper function rename_processed_file(). This function is responsible for renaming the processed data file that has been written to the S3 bucket. The new file name is generated with a timestamp to make it unique and easily identifiable.

2. AWS Glue Job (CustomerBooking.py)
The Glue job handles the data transformation, primarily focusing on joining and processing customer and booking data. Here’s how it works:

Loading Raw Data: The Glue job starts by reading raw data files from an S3 bucket using AWS Glue's create_dynamic_frame.from_options. The data includes customer details (customer_details_raw.csv) and booking details (booking_details_raw.csv). These raw files are loaded into Glue's DynamicFrames, which are schema-aware representations of the data, allowing for more flexible transformations.

Renaming Columns: Before performing any joins, the Glue job renames a column in the bookings data (CUSTOMER_ID to BOOKINGS_CUSTOMER_ID). This step avoids column name conflicts during the join operation between the customers and bookings data.

Joining Data: The two datasets are then joined using the Join.apply method. The join is performed on the CUSTOMER_ID field from the customers data and the renamed BOOKINGS_CUSTOMER_ID from the bookings data. This join combines the customer details with their respective booking details.

Data Transformation: After the join, the job further processes the data. It converts the resulting DynamicFrame into a Spark DataFrame for more advanced transformations. In this case, it drops the BOOKINGS_CUSTOMER_ID column, as it is no longer needed after the join.

Repartitioning and Writing Data: The job then repartitions the data into a single file (using .coalesce(1)) to make the output easier to handle, ensuring that only one file is produced. The transformed data is written back to the S3 bucket in the processed-data folder, in CSV format with headers included.

Workflow Overview
Triggering the Process: The Lambda function starts the Glue job, waits for its completion, and monitors the status.
Data Processing: The Glue job loads raw data, performs necessary transformations (including joins and column renaming), and writes the processed data back to S3.
File Renaming: Once the job completes, the Lambda function renames the processed data file to include a timestamp, ensuring that each file has a unique name. and load it to the renamed_processed_file folder.




Snowflake -  I have created the TRAVEL_DB database and raw schema , then I have create the CUSTOMER_BOOKINGS_DATA and CUSTOMER_BOOKINGS_STAGING table , 


1. Cloud Storage Integration in Snowflake
To begin with, you needed a secure connection between Snowflake and your Amazon S3 storage where your customer booking data is stored. This is achieved through a storage integration. Think of this integration as Snowflake's way of "talking" to AWS S3 securely, using an AWS IAM role that grants the necessary permissions.

By setting up this integration, you've ensured that Snowflake can access the data stored in your S3 bucket and work with it directly without needing manual intervention. You allowed Snowflake to access any data location in your S3 bucket (STORAGE_ALLOWED_LOCATIONS = '*'), which is flexible but could be adjusted later if needed for security or organizational reasons.

2. Creating the Stage for Data Loading
   Once the integration was set up, the next step was creating a stage in Snowflake. A stage is essentially a reference to a data storage location, in this case, your S3 bucket. By creating this stage, you've told Snowflake exactly where to find the data, and you've also defined how the data is structured (CSV format with specific delimiters, and an instruction to skip the header row).

This is like preparing a "loading dock" for your data, where Snowflake knows exactly where to pick up the files from S3 and how to interpret them.
to configure this , I have gone through the below snowflake documentation -
https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-3-create-a-cloud-storage-integration-in-snowflake

3. Setting Up Snowpipe for Continuous Data Loading
I have used below snowflake documentation to setup it ( I have used option 1)
https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3
To automate the process of loading data into Snowflake, you've used Snowpipe, which is Snowflake's continuous data ingestion service. With Snowpipe enabled, the system automatically detects new files that arrive in your S3 bucket and loads them into Snowflake without you having to manually trigger the process.

This "auto-ingest" functionality saves a lot of time and effort. You don’t have to worry about manually loading each new file; Snowpipe does it for you. If any errors occur while loading, Snowflake will continue processing the rest of the data, so you don't lose out on the rest of the files. This makes your process more resilient and automated
4. Processing the Data with a Stored Procedure
After Snowpipe loads the data into a staging table (a temporary place to hold data before it’s processed), you wrote a stored procedure to handle the actual processing of that data. The stored procedure performs a MERGE operation to check whether the customer data already exists in your main table (the CUSTOMER_BOOKINGS_DATA table).

If a matching record exists (based on CUSTOMER_ID), it updates the record with new values.
If no match is found, it inserts a new record into the main table.
This step ensures that your main table is always up-to-date with the latest customer booking data, whether it’s new data or updates to existing data.

Additionally, after the merge, you truncate the staging table to clear out processed data, ensuring that the staging table is ready for the next batch of data.

5. Automating the Process with Tasks
Now, because you don’t want to manually run the stored procedure every time, you've automated the process using tasks. Tasks in Snowflake are used to schedule and automate SQL operations (like calling the stored procedure) to run at specific intervals.

You set your task to execute the stored procedure every 5 minutes, which means that Snowflake will automatically process new data from the staging table into the main table at regular intervals. The task will run continuously as long as it’s active, so you don’t have to worry about setting it up again after the first time.

