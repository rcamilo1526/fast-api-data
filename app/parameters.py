from fastapi import Query


table_description = """
Type table to do backup.

Current options:
- departments
- hired_employees
- jobs
"""


class TableParameters:
    def __init__(
        self,
        table: str = Query(..., description=table_description)
    ):
        self.table = table


views_description = """
View of insights.

Current options:
- HIRES_DEPARTMENT_JOB
- DEPARTMENTS_HIRED_ABOVE_MEAN
"""


class ViewsParameters:
    def __init__(
        self,
        table: str = Query(..., description=views_description),
        limit: int = 100
    ):
        self.table = table
        self.limit = limit
