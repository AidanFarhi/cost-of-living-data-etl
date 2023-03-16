import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()


def get_expense_data():
	bucket_name = os.getenv('BUCKET_NAME')
	client = boto3.client(
		's3', 
		endpoint_url='https://s3.amazonaws.com',
		aws_access_key_id=os.getenv('ACCESS_KEY'),
		aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
	)
	objects_metadata = client.list_objects(
		Bucket=bucket_name, Prefix='real_estate/cost_of_living/2023-03-15'
	)
	keys = [obj['Key'] for obj in objects_metadata['Contents'] if 'expenses' in obj['Key']]
	objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
	expenses_df = pd.concat([pd.read_csv(StringIO(obj['Body'].read().decode('utf-8'))) for obj in objects])
	return expenses_df

def get_household_data(conn):
	household_df = pd.read_sql('SELECT * FROM HOUSEHOLD', conn)
	return household_df

def transform_expense_data(expense_df):
	expense_df.usd_amount = expense_df.usd_amount.apply(lambda x: x.replace(',', '')).astype(float)
	expense_df.num_children = expense_df.num_children.astype(int)
	expense_df['as_of_date'] = date.today()
	expense_df = expense_df.rename(columns={
		'num_children': 'CHILDREN', 'num_adults': 'ADULTS', 'num_working': 'WORKING_ADULTS',
		'expense_category': 'CATEGORY', 'usd_amount': 'AMOUNT', 'as_of_date': 'AS_OF_DATE', 
		'county': 'COUNTY'
	})
	return expense_df

def join_expense_and_household(expense_df, household_df):
	joined = expense_df.merge(household_df, on=['CHILDREN', 'ADULTS', 'WORKING_ADULTS'])
	joined = joined[['CATEGORY', 'AMOUNT', 'COUNTY', 'AS_OF_DATE', 'HOUSEHOLD_ID']]
	return joined

def load_expense_data_to_snowflake(conn, joined):
	write_pandas(conn, joined, 'ANNUAL_EXPENSE')

def main(event, context):
	conn = snowflake.connector.connect(
		user=os.getenv('SNOWFLAKE_USERNAME'),
		password=os.getenv('SNOWFLAKE_PASSWORD'),
		account=os.getenv('SNOWFLAKE_ACCOUNT'),
		warehouse=os.getenv('WAREHOUSE'),
		database=os.getenv('DATABASE'),
		schema=os.getenv('SCHEMA')
	)
	expense_df = get_expense_data()
	household_df = get_household_data(conn)
	expense_df = transform_expense_data(expense_df)
	joined = join_expense_and_household(expense_df, household_df)
	load_expense_data_to_snowflake(conn, joined)
	return {'statusCode': 200}


if __name__ == '__main__':
	main(None, None)
