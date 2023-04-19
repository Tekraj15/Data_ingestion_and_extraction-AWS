import psycopg2
import pandas as pd
import boto3
import io

# Define the function to read data from Aurora DB
def read_from_aurora():
    
    # Connect to the Aurora DB
    conn = psycopg2.connect(
        host='<your_host>',
        database='<your_database>',
        user='<your_username>',
        password='<your_password>'
    )


    query = 'SELECT col1, col2, col3, col4 FROM catalog.chewy_catalog;'

    # Execute the query and fetch the results
    cur = conn.cursor()    # create a cursor object that allows you to interact with the database
    cur.execute(query)
    results = cur.fetchall()   # fetches all the rows returned by the SELECT query as a list of tuples, each tuple represent a single row in query result.
    conn.close()

    # Convert the results to a pandas dataframe
    columns = ['col1', 'col2', 'col3', 'col4']  # make sure the columns are in same order as the columns in 
    df = pd.DataFrame(results, columns=columns) # replace with your column names
    return df

# Define the function to push data to S3
def push_to_s3(df, bucket_name, object_key):

    
    csv_buffer = io.StringIO()  # Convert the dataframe to a CSV file in memory
    df.to_csv(csv_buffer, index=False)
    
    s3 = boto3.resource('s3')    # Connect to S3 and upload the file
    s3.Object(bucket_name, object_key).put(Body=csv_buffer.getvalue())
    print(f"Data written to s3://{bucket_name}/{object_key}")


df = read_from_aurora()
push_to_s3(df, 'my-s3-bucket', 'my-object-key.csv')
