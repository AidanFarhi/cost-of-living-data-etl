import os
import boto3
import json
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

"""
# Set up connection to Snowflake
conn = snowflake.connector.connect(
    user='your_user',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'
)

# Create a Pandas DataFrame
df = pd.DataFrame({
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C'],
    'column3': [True, False, True]
})

# Create a Snowflake table
table_name = 'your_table'
cur = conn.cursor()
cur.execute(f"create table if not exists {table_name} (column1 int, column2 varchar, column3 boolean)")

# Load the data from Pandas DataFrame into Snowflake
query = f"insert into {table_name} values(%s, %s, %s)"
data = df.values.tolist()
cur.executemany(query, data)
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
"""

def main(event, context):
	return {'statusCode': 200}


if __name__ == '__main__':
	main(None, None)
