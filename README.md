
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

### 2. **AWS Glue (Data Transformation)**

- It extracts data from the raw files in S3, performing transformations such as cleaning, renaming columns, and joining the `customer_details_raw.csv` file with the `booking_details_raw.csv` on the `CUSTOMER_ID` field.
- It then writes the transformed data back into the **processed-data** folder in S3.

### 3. **AWS Lambda (Orchestration and File Renaming)**

Here’s how it works:

#### **Triggering the Glue Job**
- When new raw data arrives in S3, the Lambda function execute the Glue job. It uses the AWS SDK to start the Glue job and monitors its progress.
- The Lambda function checks the status of the Glue job at regular intervals, ensuring that the job runs to completion.

#### **Renaming the Processed File**
- After the Glue job completes successfully, the Lambda function automatically renames the processed file and moves it to the **renamed_processed_data** folder in S3.
- This step ensures that the processed file has a unique timestamped name, making it easier to track and manage different versions of the processed data.

The Lambda function plays a key role in automating both the execution of the Glue job and the post-processing steps, allowing me to focus on higher-level tasks without worrying about manual triggers or file handling.

### 4. **Snowflake Setup (Data Loading)**

Once the processed data is available in S3, the next step is loading it into **Snowflake**. Here's how that part of the pipeline works:

1. **External Stage**: Snowflake is set up with an external stage that points to the S3 bucket where the processed data files are stored.
2. **Snowpipe**: I’ve set up **Snowpipe** to automatically load data from S3 into Snowflake as soon as the file is available. Snowpipe continuously monitors the S3 bucket and ensures that the data is ingested as soon as it’s ready.
3. **Tasks and Stored Procedures**: To make the data loading process fully automated, I created tasks and stored procedures in Snowflake. These automate the process of transforming and loading data as soon as new files are available.


## **Why This Matters**

This project is a great example of how cloud technologies like AWS and Snowflake can work together to automate and streamline data workflows. By using Glue, Lambda, and Snowflake, I’ve been able to set up an end-to-end solution for processing, transforming, and loading data with minimal manual effort. Whether you're working with customer booking data like in this example or any other large dataset, this approach makes data management easier and more efficient.


## **Conclusion**

Building this automated pipeline was a fun and challenging experience! It helped me dive deeper into AWS Glue, Lambda, and Snowflake, and the end result is a fully automated solution that can scale and adapt as the dataset grows. If you’re interested in automating your own data workflows, I highly recommend exploring how AWS services like Glue, Lambda, and Snowpipe can be used to build a seamless data pipeline.

Feel free to explore the repository, and let me know if you have any questions or suggestions!

---

This version has a bit more personality and a conversational tone to make it feel more engaging, while still keeping the technical explanations clear. Let me know if you need any further adjustments or more details! 😊
