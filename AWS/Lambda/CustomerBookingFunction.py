import boto3
import time

# Initialize AWS Glue and S3 clients
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    # Define your Glue job name
    glue_job_name = "CustomerBooking"
    
    # Start the Glue job
    response = glue_client.start_job_run(JobName=glue_job_name)
    job_run_id = response['JobRunId']
   
    
    print(f"Started Glue job: {glue_job_name} with JobRunId: {job_run_id}")
    
    # Wait for the Glue job to complete
    while True:
        job_status = glue_client.get_job_run(JobName=glue_job_name, RunId=job_run_id)
        state = job_status['JobRun']['JobRunState']
        print(job_status)
        print(f"Current job state: {state}")
        
        if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(30)  # Wait 30 seconds before checking again

    if state == 'SUCCEEDED':
        print("Glue job completed successfully. Proceeding to rename the file.")
        rename_processed_file()
    else:
        print(f"Glue job failed or stopped. State: {state}")
        raise Exception(f"Glue job did not succeed. Final state: {state}")

def rename_processed_file():
    import datetime
    s3 = boto3.client('s3')
    
    # S3 bucket and folder details
    bucket_name = "customer-booking-data-pipeline"
    source_folder = "processed-data/"
    target_folder = "renamed_processed_data/"
    
    # Generate current timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{target_folder}processeddata_{timestamp}.csv"
    
    try:
        # List objects in the source folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
        
        if "Contents" in response:
            for obj in response["Contents"]:
                print(obj)
                source_file_key = obj["Key"]
                print(f"Found object: {source_file_key}")
                
                # Process only specific files (e.g., CSV files)
                if source_file_key.endswith(".csv"):
                    print(f"Processing file: {source_file_key}")
                    
                    # Copy the file with the new name
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={"Bucket": bucket_name, "Key": source_file_key},
                        Key=new_file_name
                    )
                    
                    # Delete only files, not the folder itself
                    s3.delete_object(Bucket=bucket_name, Key=source_file_key)
                    print(f"Deleted file: {source_file_key}")
        else:
            print("No files found in the source folder.")
    except Exception as e:
        print(f"Error renaming the file: {str(e)}")
        raise e

