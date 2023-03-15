## This script loads cost of living data from S3 and loads it to Snowflake.

This code depends on a .env file placed in the root of the project with the following environment variables:

```
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
BUCKET_NAME
AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY
```

### Command to run script.

`docker compose up --build`
