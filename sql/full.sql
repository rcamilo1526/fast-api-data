-- TABLE CREATION
CREATE TABLE IF NOT EXISTS HIRED_EMPLOYEES
(
    ID            INT PRIMARY KEY,
    NAME          VARCHAR,
    DATETIME      varchar,
    DEPARTMENT_ID INT,
    JOB_ID        INTEGER
);
CREATE TABLE IF NOT EXISTS DEPARTMENTS
(
    ID         INT PRIMARY KEY,
    DEPARTMENT VARCHAR
);
CREATE TABLE IF NOT EXISTS JOBS
(
    ID  INT PRIMARY KEY,
    JOB VARCHAR
);
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
-- FEATURE BACKUP
CREATE OR REPLACE PROCEDURE BACKUP_HIRED_EMPLOYEES()
AS $$
BEGIN
UNLOAD('SELECT * FROM COMPANY.PUBLIC.HIRED_EMPLOYEES')
to 's3://company-backup-{account_id}/backup-parquet/HIRED_EMPLOYEES'
iam_role 'arn:aws:iam::{account_id}:role/SpectrumRole'
FORMAT PARQUET ALLOWOVERWRITE;
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
FORMAT PARQUET ALLOWOVERWRITE;
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
FORMAT PARQUET ALLOWOVERWRITE;
END;
$$
LANGUAGE plpgsql
;
CALL BACKUP_HIRED_EMPLOYEES();
CALL BACKUP_JOBS();
CALL BACKUP_DEPARTMENTS();
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


-- CHALLENGE #2
CREATE OR REPLACE VIEW HIRES_DEPARTMENT_JOB AS
WITH BY_QTR AS (SELECT DEPARTMENT,
                       JOB,
                       EXTRACT(QTR FROM TO_DATE("DATETIME", 'YYYY-MM-DD"T"HH24:MI:SSZ')) AS QTR,
                       COUNT(*)                                                          AS N_EMPLOYEES
                FROM HIRED_EMPLOYEES E
                         LEFT JOIN JOBS J ON E.JOB_ID = J.ID
                         LEFT JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.ID
                WHERE EXTRACT(Y FROM TO_DATE("DATETIME", 'YYYY-MM-DD"T"HH24:MI:SSZ')) = 2021
                GROUP BY DEPARTMENT, JOB, QTR)
SELECT DEPARTMENT,
       JOB,
       SUM(CASE WHEN QTR = 1 THEN N_EMPLOYEES ELSE 0 END) AS Q1,
       SUM(CASE WHEN QTR = 2 THEN N_EMPLOYEES ELSE 0 END) AS Q2,
       SUM(CASE WHEN QTR = 3 THEN N_EMPLOYEES ELSE 0 END) AS Q3,
       SUM(CASE WHEN QTR = 4 THEN N_EMPLOYEES ELSE 0 END) AS Q4
FROM BY_QTR
GROUP BY DEPARTMENT,JOB
ORDER BY DEPARTMENT,JOB;

CREATE OR REPLACE VIEW DEPARTMENTS_HIRED_ABOVE_MEAN AS
WITH HIRES_BY_DEPARTMENT AS (SELECT D.ID, DEPARTMENT, COUNT(*) HIRED
                             FROM HIRED_EMPLOYEES E
                                      LEFT JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.ID
                             WHERE EXTRACT(Y FROM TO_DATE("DATETIME", 'YYYY-MM-DD"T"HH24:MI:SSZ')) = 2021
                             GROUP BY 1, 2)
SELECT ID, DEPARTMENT, HIRED
FROM HIRES_BY_DEPARTMENT
WHERE HIRED > (SELECT AVG(HIRED) FROM HIRES_BY_DEPARTMENT)
ORDER BY 3 DESC;

