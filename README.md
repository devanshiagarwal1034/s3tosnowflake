# **S3 to Snowflake Data Pipeline Project**

Welcome to my Customer Booking Data Pipeline project! 🚀 This project demonstrates an end-to-end, automated solution to process and load customer booking data from AWS S3 into Snowflake, leveraging key AWS services and Snowflake features for seamless data transformation and analysis.

This project aims to provide a scalable and efficient data pipeline that automates data ingestion, transformation, and loading while ensuring the processed data is analysis-ready.

### **Project Overview**

The project focuses on building a pipeline that extracts raw customer booking data from AWS S3, processes it using AWS Glue and Lambda, and loads it into Snowflake for analysis. By utilizing services like Snowpipe, storage integration, and Snowflake tasks, the pipeline ensures automation and operational efficiency.

### **Key AWS Services and Snowflake Features:**

1. **S3**: Stores raw and processed data files.  
2. **AWS Glue**: Transforms, cleans, and joins datasets.  
3. **AWS Lambda**: Automates Glue job execution and renames processed files.  

**Snowflake**: Enables efficient data storage and analysis with:  
- **Stage** for defining S3 data locations.  
- **Storage Integration** for secure S3 access.  
- **Snowpipe** for automated data ingestion.  
- **Stored Procedures** for data processing.  
- **Tasks** for scheduled automation of stored procedures.  

### **The Flow of Data**

### 1. **Data Ingestion (Raw Data in S3)**

The pipeline begins with the raw customer booking data, which is stored in an S3 bucket (named `customer-booking-data-pipeline`) within the **raw-data** folder. The files in this folder are:

- `customer_details_raw.csv` (Contains customer information)
- `booking_details_raw.csv` (Contains booking information)

To automate the processing of these files, I used two main components: a Lambda function (named `CustomerBookingFunction.py`) and an AWS Glue job (`CustomerBooking.py`). Both of these components work together to manage the data pipeline, from extraction to transformation and renaming.

#### 2. **Lambda Function (CustomerBookingFunction.py)**
The Lambda function serves multiple purposes:
- **Triggering the Glue Job**: It begins by establishing a connection to AWS Glue via the boto3 client. The function then triggers the Glue job named `CustomerBooking` using the `start_job_run` API call. It also captures the `JobRunId` to track this specific job run.
- **Job Status Monitoring**: After triggering the Glue job, the Lambda function monitors its status. It checks the job state every 30 seconds using the `get_job_run` API call. If the job state returns `SUCCEEDED`, it proceeds to the next step. If there’s an error or if the job fails, the Lambda function raises an error and halts the pipeline.
- **Renaming the Processed File**: Once the Glue job completes successfully, the Lambda function renames the processed file using a helper function, `rename_processed_file()`. This function adds a timestamp to the file name, ensuring each processed file has a unique identifier.

#### 3. **AWS Glue Job (CustomerBooking.py)**
The Glue job handles the heavy lifting of transforming and processing the data. Here's how it works:
- **Loading Raw Data**: The job begins by loading the raw data files from the S3 bucket using Glue's `create_dynamic_frame.from_options`. This step pulls the customer data (`customer_details_raw.csv`) and booking data (`booking_details_raw.csv`) into Glue's DynamicFrames, which are schema-aware representations that make data transformation easier.
- **Renaming Columns**: Before joining the two datasets, the Glue job renames a column in the bookings data (`CUSTOMER_ID` to `BOOKINGS_CUSTOMER_ID`) to avoid conflicts during the join operation.
- **Joining Data**: The two datasets are then merged using the `Join.apply` method, with the join performed on the `CUSTOMER_ID` field from the customer data and the `BOOKINGS_CUSTOMER_ID` from the booking data.
- **Data Transformation**: After the join, the Glue job performs additional transformations. It converts the resulting DynamicFrame into a Spark DataFrame, which allows for advanced operations like dropping unnecessary columns, such as `BOOKINGS_CUSTOMER_ID`.
- **Repartitioning and Writing Data**: Finally, the job repartitions the data into a single file (using `.coalesce(1)`) to simplify output handling. It then writes the processed data back to S3 in the **processed-data** folder in CSV format, ensuring the output includes headers.

#### **Workflow Overview**
- **Triggering the Process**: The Lambda function triggers the Glue job, waits for its completion, and monitors the status.
- **Data Processing**: The Glue job loads, transforms, and processes the data, and then writes it back to the S3 bucket in a processed-data folder.
- **File Renaming**: After the job finishes, the Lambda function renames the processed file to include a timestamp, ensuring that each file is uniquely identifiable and easily traceable and load it to renamed_processed_data folder.

 **4.Cloud Storage Integration in Snowflake**
The first step in Snowflake is to establish a secure connection between Snowflake and AWS S3, where the customer booking data is stored. This is accomplished by creating a **storage integration**, which enables Snowflake to securely "talk" to S3. This integration uses an AWS IAM role that grants the necessary permissions, allowing Snowflake to access the S3 data without manual intervention. 

**5. Creating the Stage for Data Loading**
Next, I created a **stage** in Snowflake, which acts as a reference to the S3 bucket where the data resides. This step is crucial because it tells Snowflake where to find the files and how to interpret them. The files are in CSV format, with a specific delimiter and an instruction to skip the header row, making it clear how Snowflake should load the data.

   I followed the Snowflake documentation to configure this stage and set up the connection with S3. Here’s the reference I used:
   - [Snowflake Data Load S3 Configuration Documentation](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-3-create-a-cloud-storage-integration-in-snowflake)

**6. Setting Up Snowpipe for Continuous Data Loading**
To automate the process of loading data into Snowflake, I used **Snowpipe**, Snowflake’s continuous data ingestion service. Snowpipe automatically detects when new files arrive in the S3 bucket and loads them into Snowflake without any manual intervention. 

   By setting up this auto-ingestion process, I eliminate the need to trigger the data load process manually. Snowpipe runs in the background, automatically loading new files as they arrive, and if any errors occur during the loading process, Snowflake continues processing the remaining data.

   I used the following Snowflake documentation to set up Snowpipe:
   - [Snowflake Snowpipe Auto Ingest Configuration](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)

**7. Processing the Data with a Stored Procedure**
Once Snowpipe loads the data into a staging table, a stored procedure is used to process the data further. The stored procedure performs a **MERGE** operation on the staging table, checking whether the customer data already exists in the main table (`CUSTOMER_BOOKINGS_DATA`).

   - If a matching record exists (based on `CUSTOMER_ID`), the procedure updates the existing record with new values.
   - If no match is found, the procedure inserts the new record into the main table.

   After the merge operation, the procedure truncates the staging table to ensure that it is empty and ready for the next batch of incoming data.

**8. Automating the Process with Tasks**
To eliminate the need for manual intervention, I used **Snowflake Tasks** to schedule the stored procedure to run automatically at regular intervals (every 5 minutes). Tasks in Snowflake are used to automate SQL operations, ensuring that new data is processed continuously and that the main table remains up-to-date without requiring any manual triggers. This automated scheduling ensures the process runs smoothly without any manual effort.

Conclusion
This project demonstrates a robust, automated data pipeline using AWS and Snowflake. By combining the strengths of these platforms, the pipeline ensures reliable data processing and analysis.
