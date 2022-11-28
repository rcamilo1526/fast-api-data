from fastapi import FastAPI, File, UploadFile, Depends, Query
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse
import pandas as pd
from app.templates.data_dict import table_dict
from app.validations.tables import generate_filter_codes
from app.services.redshift import (upload_df_redshift, backup_table,
                                   restore_table, get_insights)
from app.parameters import TableParameters, ViewsParameters
from app.security.auth import authenticate
import re

description = """
DATA API to validate and upload csv company data 🏢

## Items

upload can validate and upload data **user and password required**.
"""

app = FastAPI(title="data api", description=description)

# commands to run
# uvicorn app.main:app --reload
# docker build -t data_api:0.1 .
# docker run -p 8000:8000 --name my-api data_api:0.1


@app.get("/")
async def root():
    return {"message": "Hello folks 😎."}


@app.post("/upload")
# def upload(table: str, file: UploadFile = File(...)):
async def upload(form_data: OAuth2PasswordRequestForm = Depends(),
                 file: UploadFile = File(...)):

    await authenticate(form_data)

    # ttype = file.filename.replace('.csv', '')
    ttype = (re.search('^(departments|hired_employees|jobs).*',
                       file.filename).group(1))

    if ttype not in table_dict:
        return JSONResponse(status_code=405,
                            content={'error': 'The data table is not availabe for upload'})

    tcolumns = list(table_dict[ttype].keys())
    df = pd.read_csv(file.file, names=tcolumns, dtype={
                     t: 'str' for t in tcolumns})
    file.file.close()
    nrows = len(df.index)

    content_reponse = {'file': file.filename,
                       'type': ttype,
                       'columns': tcolumns,
                       'rows': nrows}

    if nrows < 1 or nrows > 1000:
        return JSONResponse(status_code=405,
                            content={'error': 'The number of lines should be between 1 and 1000'})

    codeline = generate_filter_codes(df, ttype)
    loc = {'df': df}
    exec(codeline, globals(), loc)

    df_ok = loc['df_ok']
    df_failed = loc['df_failed']
    upload_df_redshift(df_ok, ttype)
    ok_inserts = len(df_ok.index)
    if len(df_failed.index) == 0:
        content_reponse['result'] = 'All the records have been uploaded'
        content_reponse['nrows_inserted'] = ok_inserts
    else:
        content_reponse[
            'result'] = f'Errors founded on records failed, successfully uploaded {ok_inserts} registers'
        if ok_inserts > 0:
            content_reponse['nrows_inserted'] = ok_inserts
        df_failed = df_failed.fillna('').to_dict(orient="records")
        content_reponse['rows_failed'] = df_failed

    return JSONResponse(status_code=200, content=content_reponse)


@app.post("/backup")
async def backup(form_data: OAuth2PasswordRequestForm = Depends(),
                 params: TableParameters = Depends()):

    await authenticate(form_data)
    ttype = params.table
    if ttype not in table_dict:
        return JSONResponse(status_code=405,
                            content={'error': f'The table {ttype} is not supported'})
    try:
        status = backup_table(ttype)
        return status
    except Exception as e:
        return e


@app.post("/restore")
async def restore(form_data: OAuth2PasswordRequestForm = Depends(),
                  params: TableParameters = Depends()):

    await authenticate(form_data)
    ttype = params.table
    if ttype not in table_dict:
        return JSONResponse(status_code=405,
                            content={'error': f'The table {ttype} is not supported'})
    try:
        status = restore_table(ttype)
        return status
    except Exception as e:
        return e


@app.get("/insights")
async def insights(params: ViewsParameters = Depends()):

    vtype = params.table
    vlimit = params.limit
    available_insights = ['HIRES_DEPARTMENT_JOB',
                          'DEPARTMENTS_HIRED_ABOVE_MEAN']
    if vtype not in available_insights:
        return JSONResponse(status_code=405,
                            content={'error': f'The insight {vtype} is not supported'})
    try:
        status = get_insights(vtype, vlimit)
        return status
    except Exception as e:
        return e
