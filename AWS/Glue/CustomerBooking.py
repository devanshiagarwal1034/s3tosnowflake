import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import Join
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize the Glue context and parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
raw_bucket = "s3://customer-booking-data-pipeline/raw-data/"
processed_bucket = "s3://customer-booking-data-pipeline/processed-data/"

# Load raw data from S3
customers = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_bucket + "customer_details_raw.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

bookings = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_bucket + "booking_details_raw.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

# Rename columns in the 'bookings' dataframe to avoid conflicts
bookings_renamed = bookings.rename_field('CUSTOMER_ID', 'BOOKINGS_CUSTOMER_ID')  # Rename CUSTOMER_ID in bookings


# Perform the join (customers are not renamed, bookings columns are renamed)
joined_data = Join.apply(customers, bookings_renamed, 'CUSTOMER_ID', 'BOOKINGS_CUSTOMER_ID')

# Convert to DataFrame for further transformations
joined_df = joined_data.toDF()

# Drop the renamed columns (like 'BOOKINGS_CUSTOMER_ID') if you don't need them
joined_df = joined_df.drop('BOOKINGS_CUSTOMER_ID')

# Repartition to 1 file
single_file_df = joined_df.coalesce(1)

# Write the output as a single CSV
output_path = processed_bucket  # Ensure this path points to the processed folder
single_file_df.write.mode("append").option("header", "true").csv(output_path)

# Commit the job
job.commit()
