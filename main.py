from fastapi import FastAPI, File, UploadFile, Header
import csv
import codecs

data_api = FastAPI()
# uvicorn main:data_api --reload

table_dict = {'hired_employees': ({'column': 'id'},
                                  {'column': 'name'},
                                  {'column': 'datetime'},
                                  {'column': 'department_id'},
                                  {'column': 'job_id'}
                                  ),
              'departments': ({'column': 'id'},
                              {'column': 'department'}
                              ),
              'jobs': ({'column': 'id'},
                       {'column': 'job'}
                       )
              }


@data_api.get("/")
async def root():
    return {"message": "Hello folks 😎."}


@data_api.post("/upload")
# def upload(table: str, file: UploadFile = File(...)):
def upload(file: UploadFile = File(...)):
    data = []
    ttype = file.filename.replace('.csv', '')
    tcolumns = [c['column'] for c in table_dict[ttype]]
    csvReader = csv.DictReader(codecs.iterdecode(file.file, 'utf-8'),
                               fieldnames=tcolumns)

    for r in csvReader:
        data.append(r)

    file.file.close()

    return {'file': file.filename,
            'type': ttype,
            'columns': tcolumns,
            'data': data}
