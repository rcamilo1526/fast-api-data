"""Data dictionary."""

table_dict = {'hired_employees': {'id': 'INTEGER',
                                  'name': 'STRING',
                                  'datetime': 'ISO_DATE',
                                  'department_id': 'INTEGER',
                                  'job_id': 'INTEGER'},
              'departments': {'id': 'INTEGER',
                              'department': 'STRING'},
              'jobs': {'id': 'INTEGER',
                       'job': 'STRING'}
              }
