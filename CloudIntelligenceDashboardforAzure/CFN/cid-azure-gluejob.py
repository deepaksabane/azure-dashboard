# Cloud Intelligence Dashboard for Azure Glue Script - Standard Cost Export

### Glue base
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

### Parameters fetched from Glue Job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'var_raw_path', 'var_parquet_path', 'var_processed_path',
    'var_glue_database', 'var_glue_table', 'var_bucket', 'var_raw_folder',
    'var_processed_folder', 'var_parquet_folder', 'var_date_format',
    'var_folderpath', 'var_azuretags', 'var_account_type', 'var_bulk_run_ssm_name', 
    'var_error_folder', 'var_lambda01_name'
])
var_raw_path = args['var_raw_path']
var_parquet_path = args['var_parquet_path']
var_processed_path = args['var_processed_path']
var_glue_database = args['var_glue_database']
var_glue_table = args['var_glue_table']
var_bucket = args['var_bucket']
var_raw_folder = args['var_raw_folder']
var_processed_folder = args['var_processed_folder']
var_parquet_folder = args['var_parquet_folder']
var_date_format = args['var_date_format']
var_folderpath = args['var_folderpath']
var_azuretags = args['var_azuretags']
var_account_type = args['var_account_type']
var_bulk_run_ssm_name = args['var_bulk_run_ssm_name']
var_error_folder = args['var_error_folder']
var_lambda01_name = args['var_lambda01_name']
var_raw_fullpath = var_raw_path + var_folderpath
SELECTED_TAGS = var_azuretags.split(", ")

### Copy Function
import concurrent.futures
def copy_s3_objects(source_bucket, source_folder, destination_bucket, destination_folder):
    s3_client = boto3.client('s3')
    def copy_object(obj):
        copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
        target_key = obj['Key'].replace(source_folder, destination_folder)
        s3_client.copy_object(Bucket=destination_bucket, Key=target_key, CopySource=copy_source, TaggingDirective='COPY')
    # Get list of files
    response = s3_client.list_objects(Bucket=source_bucket, Prefix=source_folder)
    objects = response.get('Contents', [])
    if objects:
        # Copy files using concurrent futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(copy_object, objects)
        print("INFO: Copy process complete")
    else:
        print(("INFO: No files in {}, copy process skipped.").format(source_folder))

### Delete Function
def delete_s3_folder(bucket, folder):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects(Bucket=bucket, Prefix=folder)
    objects = response.get('Contents', [])
    if objects:
        delete_keys = [{'Key': obj['Key']} for obj in objects]
        s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
        print("INFO: Delete process complete")
    else:
        print(("INFO: No files in {}, delete process skipped.").format(folder))

### Bulk Run - process latest object for each month
from datetime import datetime
ssm_client = boto3.client('ssm')
var_bulk_run = ssm_client.get_parameter(Name=var_bulk_run_ssm_name)['Parameter']['Value']
if var_bulk_run == 'true':
    print("INFO: Bulk run is set to {}, starting bulk run".format(var_bulk_run))
    # Delete manifest.json files from raw folder
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=var_bucket, Prefix=var_raw_folder)
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('manifest.json'):
            print(f"INFO: Deleting manifest file {key}")
            s3.delete_object(Bucket=var_bucket, Key=key)
    # Copy CSV from raw to processed
    copy_s3_objects(var_bucket, var_raw_folder, var_bucket, var_processed_folder)
    # Delete raw files
    delete_s3_folder(var_bucket, var_raw_folder)
    # Delete parquet files
    delete_s3_folder(var_bucket, var_parquet_folder)
    # Create dictionary to store latest modified file for each month
    s3 = boto3.client('s3')
    tag_key = 'lastmodified'
    response = s3.list_objects_v2(Bucket=var_bucket, Prefix=var_processed_folder)
    latest_files = {}
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('/'):
            continue
        parent_key = '/'.join(key.split('/')[:-1])
        tags = s3.get_object_tagging(Bucket=var_bucket, Key=key)['TagSet']
        for tag in tags:
            if tag['Key'] == tag_key:
                last_modified = tag['Value']
                # Convert last_modified datetime object
                dt_object = datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')
                # Convert datetime object to timestamp
                timestamp = dt_object.timestamp()
                # Check current file is the latest modified file for the month
                if parent_key in latest_files:
                    if timestamp > latest_files[parent_key]['timestamp']:
                        latest_files[parent_key] = {'key': key, 'timestamp': timestamp}
                else:
                    latest_files[parent_key] = {'key': key, 'timestamp': timestamp}
    # Copy latest modified file for each month to raw folder
    for parent_key, file_info in latest_files.items():
        key = file_info['key']
        new_key = key.replace(var_processed_folder, var_raw_folder)
        copy_s3_objects(var_bucket, key, var_bucket, new_key)
    # Print objects in the raw bucket, allows for file identification if bulk run fails
    s3 = boto3.client('s3')
    objects = s3.list_objects(Bucket=var_bucket).get('Contents', [])
    print("INFO: Bulk run complete, latest files for each month:")
    for obj in objects:
        if obj['Key'].startswith(var_raw_folder):
            print(obj['Key'])
    # Disable multipart upload Lambda functions
    lambda_client = boto3.client('lambda')
    try:
        # Retrieve current environment variables
        function_name = var_lambda01_name
        current_config = lambda_client.get_function_configuration(FunctionName=function_name)
        environment = current_config.get('Environment', {})
        variables = environment.get('Variables', {})

        # Update partitionSize
        variables['partitionSize'] = '10737418240'

        # Update function configuration with the modified environment variables
        response = lambda_client.update_function_configuration(
            FunctionName = function_name,
            Environment = {'Variables': variables}
        )
        print("INFO: Lambda function configuration updated successfully.")
    except Exception as e:
        print("ERROR: {}".format(e))
        pass
    # Change bulk_run ssm parameter to false
    ssm_client.put_parameter(Name=var_bulk_run_ssm_name, Value='false', Type='String', Overwrite=True)

else:
    print("INFO: Bulk run is set to {}, continuing with normal run".format(var_bulk_run))

### Read CSV and append file_path column
from pyspark.sql.functions import input_file_name

try:
    df1 = spark.read.option("header","true").option("delimiter",",").option("escape", "\"").csv(var_raw_fullpath)
    df1 = df1.withColumn("file_path", input_file_name())
except Exception as e:
    print("WARNING: Cannot read CSV file(s) in {}. Incorrect path or folder empty.".format(var_raw_fullpath))
    print("ERROR: {}".format(e))
    raise e

### MAPPING SECTION 1: To parse AWS Glue script complete the mapping below. Change the first value to match CSV headers. Do not change the second value [CASE SENSITIVE]
df1 = df1.withColumnRenamed("billingPeriodEndDate", "BillingPeriodEndDate") \
         .withColumnRenamed("billingPeriodStartDate", "BillingPeriodStartDate") \
         .withColumnRenamed("cost", "Cost") \
         .withColumnRenamed("costInBillingCurrency", "CostInBillingCurrency") \
         .withColumnRenamed("date", "Date") \
         .withColumnRenamed("effectivePrice", "EffectivePrice") \
         .withColumnRenamed("PayGPrice", "PayGPrice") \
         .withColumnRenamed("quantity", "Quantity") \
         .withColumnRenamed("tags", "Tags") \
         .withColumnRenamed("unitPrice", "UnitPrice")

### MAPPING SECTION 2: To render sample dashboard complete the mapping below. Change the first value to match CSV headers. Do not change the second value.
df1 = df1.withColumnRenamed("accountName", "AccountName") \
         .withColumnRenamed("billingAccountName", "BillingAccountName") \
         .withColumnRenamed("meterCategory", "MeterCategory") \
         .withColumnRenamed("meterSubCategory", "MeterSubCategory") \
         .withColumnRenamed("ProductName", "Product") \
         .withColumnRenamed("resourceLocation", "ResourceLocation") \
         .withColumnRenamed("resourceName", "ResourceName") \
         .withColumnRenamed("subscriptionId", "SubscriptionId") \
         .withColumnRenamed("resourceId", "ResourceId") \
         .withColumnRenamed("resourceGroupName", "ResourceGroupName")

### Write parquet file
output_path = var_parquet_path + var_date_format + "/"
df1.write.mode("overwrite").parquet(output_path)
print("INFO: Parquet file written to {}".format(output_path))

### Update Glue Catalog
glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [output_path]},
    format="parquet"
).toDF().createOrReplaceTempView("dataframe")
dataframe = spark.sql("SELECT * FROM dataframe")
dataframe.write.mode("overwrite").format("parquet").saveAsTable(var_glue_database + "." + var_glue_table)

### Write processed files
processed_path = var_processed_path + var_date_format + "/"
df1.write.mode("overwrite").csv(processed_path)
print("INFO: Processed files written to {}".format(processed_path))
