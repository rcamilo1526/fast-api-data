-- TABLE UPLOAD
COPY HIRED_EMPLOYEES FROM 
's3://company-data-{account_id}/data/hired_employees.csv'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
CSV;
COPY JOBS FROM 
's3://company-data-{account_id}/data/jobs.csv'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
CSV;
COPY DEPARTMENTS FROM 
's3://company-data-{account_id}/data/departments.csv'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
CSV;