## This script extracts crime data from an api and loads it to an S3 bucket.

This code depends on a .env file placed in the root of the project with the following environment variables:

```
RAPID_API_KEY
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
BUCKET_NAME
AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY
```

### Command to run script.

`docker compose up --build`
