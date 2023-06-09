import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def get_df_from_s3(client: boto3.client, bucket_name: str, category: str, extract_date: str) -> pd.DataFrame:
    """
    Retrieve DataFrame from S3 bucket for a given category and extract date.

    Args:
        client (boto3.client): Boto3 client for S3.
        bucket_name (str): Name of the S3 bucket.
        category (str): Category to filter the objects.
        extract_date (str): Extract date string.

    Returns:
        pd.DataFrame: DataFrame containing the retrieved data.
    """
    objects_metadata = client.list_objects(Bucket=bucket_name, Prefix=f"real_estate/cost_of_living/{extract_date}")
    keys = [obj["Key"] for obj in objects_metadata["Contents"] if category in obj["Key"]]
    objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
    df = pd.concat([pd.read_csv(StringIO(obj["Body"].read().decode("utf-8"))) for obj in objects])
    return df


def transform_living_wage_df(living_wage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the living wage DataFrame.

    Args:
        living_wage_df (pd.DataFrame): DataFrame containing living wage data.

    Returns:
        pd.DataFrame: Transformed living wage DataFrame.
    """
    living_wage_df = living_wage_df[living_wage_df["wage_level"].str.contains("LIVING")]
    living_wage_df = living_wage_df.rename(
        columns={
            "num_children": "NUMBER_OF_CHILDREN",
            "num_adults": "NUMBER_OF_ADULTS",
            "county": "COUNTY",
            "num_working": "NUMBER_OF_WORKING_ADULTS",
            "usd_amount": "HOURLY_WAGE",
        }
    )
    living_wage_df.NUMBER_OF_CHILDREN = living_wage_df.NUMBER_OF_CHILDREN.astype(int)
    living_wage_df.COUNTY = living_wage_df.COUNTY.apply(lambda x: x + " COUNTY")
    cols = ["COUNTY", "NUMBER_OF_ADULTS", "NUMBER_OF_CHILDREN", "NUMBER_OF_WORKING_ADULTS", "HOURLY_WAGE"]
    living_wage_df = living_wage_df[cols]
    living_wage_df["SNAPSHOT_DATE"] = date.today()
    return living_wage_df


def transform_annual_expense_df(annual_expense_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the annual expense DataFrame.

    Args:
        annual_expense_df (pd.DataFrame): DataFrame containing annual expense data.

    Returns:
        pd.DataFrame: Transformed annual expense DataFrame.
    """
    annual_expense_df.usd_amount = annual_expense_df.usd_amount.apply(lambda x: x.replace(",", "")).astype(float)
    annual_expense_df = annual_expense_df.rename(
        columns={
            "num_children": "NUMBER_OF_CHILDREN",
            "num_adults": "NUMBER_OF_ADULTS",
            "num_working": "NUMBER_OF_WORKING_ADULTS",
            "expense_category": "CATEGORY",
            "usd_amount": "AMOUNT",
            "county": "COUNTY",
        }
    )
    annual_expense_df.NUMBER_OF_CHILDREN = annual_expense_df.NUMBER_OF_CHILDREN.astype(int)
    annual_expense_df.COUNTY = annual_expense_df.COUNTY.apply(lambda x: x + " COUNTY")
    annual_expense_df["SNAPSHOT_DATE"] = date.today()
    return annual_expense_df


def transform_typical_annual_salary_df(typical_annual_salary_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the typical annual salary DataFrame.

    Args:
        typical_annual_salary_df (pd.DataFrame): DataFrame containing typical annual salary data.

    Returns:
        pd.DataFrame: Transformed typical annual salary DataFrame.
    """
    typical_annual_salary_df = typical_annual_salary_df.rename(
        columns={"occupational_area": "OCCUPATION", "typical_annual_salary": "SALARY", "county": "COUNTY"}
    )
    typical_annual_salary_df["SNAPSHOT_DATE"] = date.today()
    typical_annual_salary_df.COUNTY = typical_annual_salary_df.COUNTY.apply(lambda x: x + " COUNTY")
    return typical_annual_salary_df


def main(event: dict, context: object) -> dict:
    """
    Main function to process data extraction, transformation, and loading.

    Args:
        event (dict): Event data, in this case, the extract date.
        context (object): Context object, not used in this function.

    Returns:
        dict: A dictionary indicating the success status of the process.
    """
    bucket_name = os.getenv("BUCKET_NAME")
    client = boto3.client(
        "s3",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    )
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA"),
    )
    extract_date = event["extractDate"]

    # Get data from S3 and Snowflake
    living_wage_df = get_df_from_s3(client, bucket_name, "living_wage", extract_date)
    annual_expense_df = get_df_from_s3(client, bucket_name, "expenses", extract_date)
    typical_annual_salary_df = get_df_from_s3(client, bucket_name, "typical_salaries", extract_date)

    # Pull location data from Snowflake
    location_df = pd.read_sql("SELECT location_id, county FROM dim_location WHERE state = 'DE'", conn)

    # Get date_id from dim_date table
    dim_date_df = pd.read_sql(f"SELECT date_id, date FROM dim_date WHERE date = '{date.today()}'", conn)

    # Transform
    living_wage_df = transform_living_wage_df(living_wage_df)
    annual_expense_df = transform_annual_expense_df(annual_expense_df)
    typical_annual_salary_df = transform_typical_annual_salary_df(typical_annual_salary_df)

    # Add location_id to each dataset
    typical_annual_salary_merged_df = typical_annual_salary_df.merge(location_df, on="COUNTY", how="inner")
    living_wage_merged_df = living_wage_df.merge(location_df, on="COUNTY", how="inner")
    annual_expense_merged_df = annual_expense_df.merge(location_df, on="COUNTY", how="inner")

    # Add date_id to each dataset
    typical_annual_salary_final_df = typical_annual_salary_merged_df.merge(
        dim_date_df, left_on="SNAPSHOT_DATE", right_on="DATE", how="inner"
    )
    living_wage_final_df = living_wage_merged_df.merge(
        dim_date_df, left_on="SNAPSHOT_DATE", right_on="DATE", how="inner"
    )
    annual_expense_final_df = annual_expense_merged_df.merge(
        dim_date_df, left_on="SNAPSHOT_DATE", right_on="DATE", how="inner"
    )

    # Rename columns
    typical_annual_salary_final_df = typical_annual_salary_final_df.rename(columns={"DATE_ID": "SNAPSHOT_DATE_ID"})
    living_wage_final_df = living_wage_final_df.rename(columns={"DATE_ID": "SNAPSHOT_DATE_ID"})
    annual_expense_final_df = annual_expense_final_df.rename(columns={"DATE_ID": "SNAPSHOT_DATE_ID"})

    # Filter
    annual_expense_keep_cols = [
        "CATEGORY",
        "NUMBER_OF_CHILDREN",
        "AMOUNT",
        "NUMBER_OF_ADULTS",
        "NUMBER_OF_WORKING_ADULTS",
        "SNAPSHOT_DATE_ID",
        "LOCATION_ID",
    ]
    annual_expense_final_df = annual_expense_final_df[annual_expense_keep_cols]
    living_wage_keep_cols = [
        "NUMBER_OF_ADULTS",
        "NUMBER_OF_CHILDREN",
        "NUMBER_OF_WORKING_ADULTS",
        "HOURLY_WAGE",
        "SNAPSHOT_DATE_ID",
        "LOCATION_ID",
    ]
    living_wage_final_df = living_wage_final_df[living_wage_keep_cols]
    typical_annual_salary_keep_cols = ["OCCUPATION", "SALARY", "SNAPSHOT_DATE_ID", "LOCATION_ID"]
    typical_annual_salary_final_df = typical_annual_salary_final_df[typical_annual_salary_keep_cols]

    # Load to Snowflake
    write_pandas(conn, annual_expense_final_df, "FACT_ANNUAL_EXPENSE")
    write_pandas(conn, living_wage_final_df, "FACT_LIVING_WAGE")
    write_pandas(conn, typical_annual_salary_final_df, "FACT_TYPICAL_ANNUAL_SALARY")

    return {"statusCode": 200}


if __name__ == "__main__":
    from sys import argv

    main({"extractDate": argv[1]}, None)
