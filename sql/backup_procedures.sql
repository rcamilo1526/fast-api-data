- FEATURE BACKUP
CREATE OR REPLACE PROCEDURE BACKUP_HIRED_EMPLOYEES()
AS $$
BEGIN
UNLOAD('SELECT * FROM COMPANY.PUBLIC.HIRED_EMPLOYEES')
to 's3://company-backup-{account_id}/backup-parquet/HIRED_EMPLOYEES'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
FORMAT PARQUET ALLOWOVERWRITE
PARALLEL OFF;
END;
$$
LANGUAGE plpgsql
;
CREATE OR REPLACE PROCEDURE BACKUP_JOBS()
AS $$
BEGIN
UNLOAD('SELECT * FROM COMPANY.PUBLIC.JOBS')
to 's3://company-backup-{account_id}/backup-parquet/JOBS'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
FORMAT PARQUET ALLOWOVERWRITE
PARALLEL OFF;
END;
$$
LANGUAGE plpgsql
;
CREATE OR REPLACE PROCEDURE BACKUP_DEPARTMENTS()
AS $$
BEGIN
UNLOAD('SELECT * FROM COMPANY.PUBLIC.DEPARTMENTS')
to 's3://company-backup-{account_id}/backup-parquet/DEPARTMENTS'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
FORMAT PARQUET ALLOWOVERWRITE
PARALLEL OFF;
END;
$$
LANGUAGE plpgsql
;