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