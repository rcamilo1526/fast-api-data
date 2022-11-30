COPY HIRED_EMPLOYEES FROM 
's3://company-data-{account_id}/data/hired_employees.parquet'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
PARQUET;
COPY JOBS FROM 
's3://company-data-{account_id}/data/jobs.parquet'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
PARQUET;
COPY DEPARTMENTS FROM 
's3://company-data-{account_id}/data/departments.parquet'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
PARQUET;