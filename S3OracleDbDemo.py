
# Import necessary modules
import csv
import boto3
from io import StringIO
import requests
import cx_Oracle
from moto import mock_aws

# Function to upload data from URL to S3 bucket
def upload_data_to_s3_from_url(url, bucket_name, object_key):
    # Fetch data from URL
    response = requests.get(url)
    data = response.content

    # Create an S3 client using boto3
    s3 = boto3.client('s3')

    # Create the S3 bucket
    s3.create_bucket(Bucket=bucket_name)

    # Upload data to S3 bucket
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data)

    print(f"Data from URL '{url}' uploaded to S3 bucket '{bucket_name}' with key '{object_key}'.")

# Function to read data from S3 bucket
def read_data_from_s3(bucket_name, object_key):
    # Create an S3 client using boto3
    s3 = boto3.client('s3')

    # Read data from S3 bucket
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read()
    return data

# Function to parse CSV data
def parse_csv_data(csv_data):
    decoded_data = csv_data.decode('utf-8')  # Decode the CSV data
    csv_reader = csv.reader(decoded_data.splitlines())  # Create CSV reader
    header = next(csv_reader)  # Get header row
    rows = list(csv_reader)  # Get data rows
    return header, rows

# Function to insert data into Oracle database
def insert_data_into_oracle(header, rows):
    # Database connection details - Replace with your actual credentials
    user = 'hr'
    password = 'hr'
    dsn = cx_Oracle.makedsn('localhost', 1521, service_name='orclpdb')

    try:
        # Connect to the database
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()

        # Prepare the INSERT statement
        insert_statement = f"INSERT INTO data14 ({', '.join(header)}) VALUES ({', '.join([':' + str(i + 1) for i in range(len(header))])})"

        # Execute the INSERT statement for each row
        cursor.executemany(insert_statement, rows)

        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        print("Data inserted into Oracle database.")

    except cx_Oracle.Error as error:
        print("Error inserting data into Oracle database:", error)

# URL of the CSV file
url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv'

# Name of the S3 bucket
bucket_name = 'my_bucket'

# Key (object key) under which the file will be stored in the S3 bucket
object_key = 'data.csv'

# Start the moto mock S3 server
with mock_aws():
    # Upload data to S3 from URL
    upload_data_to_s3_from_url(url, bucket_name, object_key)

    # Read the uploaded data from S3
    s3_data = read_data_from_s3(bucket_name, object_key)

    # Parse CSV data
    header, rows = parse_csv_data(s3_data)

    # Insert data into Oracle database
    insert_data_into_oracle(header, rows)


