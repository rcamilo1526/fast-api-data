-- FEATURE RESTORE
CREATE OR REPLACE PROCEDURE RESTORE_HIRED_EMPLOYEES()
AS $$
BEGIN
DELETE FROM DEPARTMENTS;
COPY DEPARTMENTS FROM 
's3://company-backup-{account_id}/backup/HIRED_EMPLOYEES.avro'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
AVRO AS 'auto' IGNOREALLERRORS;
END;
$$
LANGUAGE plpgsql
;
CREATE OR REPLACE PROCEDURE RESTORE_JOBS()
AS $$
BEGIN
DELETE FROM DEPARTMENTS;
COPY DEPARTMENTS FROM 
's3://company-backup-{account_id}/backup/JOBS.avro'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
AVRO AS 'auto' IGNOREALLERRORS;
END;
$$
LANGUAGE plpgsql
;
CREATE OR REPLACE PROCEDURE RESTORE_DEPARTMENTS()
AS $$
BEGIN
DELETE FROM DEPARTMENTS;
COPY DEPARTMENTS FROM 
's3://company-backup-{account_id}/backup/DEPARTMENTS.avro'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
AVRO AS 'auto' IGNOREALLERRORS;
END;
$$
LANGUAGE plpgsql
;