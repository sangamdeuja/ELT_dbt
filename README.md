# Project Description : ELT_DBT: Integrating S3 with snowflake and perform CDC
This is a simple project deomnstrating ETL using dbt and Snowflake. The csv file from s3 bucket is staged to snowflake data object which is then transformed using dbt.
The transformed table is then stored into cleaned table to be used by other analytics tools. The whole process captures the CDC in the batch incremental data. 
