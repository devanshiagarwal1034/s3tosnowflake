
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
File Renaming: Once the job completes, the Lambda function renames the processed data file to include a timestamp, ensuring that each file has a unique name.



