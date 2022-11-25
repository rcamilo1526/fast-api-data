"""Data dictionary."""

table_dict_p = {'hired_employees': ({'column': 'id',
                                     'type': 'INTEGER'},
                                    {'column': 'name',
                                     'type': 'STRING'},
                                    {'column': 'datetime',
                                     'type': 'ISO_DATE'},
                                    {'column': 'department_id',
                                     'type': 'INTEGER'},
                                    {'column': 'job_id',
                                     'type': 'INTEGER'}
                                    ),
                'departments': ({'column': 'id',
                                 'type': 'INTEGER'},
                                {'column': 'department',
                                 'type': 'STRING'}
                                ),
                'jobs': ({'column': 'id',
                          'type': 'INTEGER'},
                         {'column': 'job',
                          'type': 'STRING'}
                         )
                }
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
