from fastapi import FastAPI, File, UploadFile, Header
from fastapi.responses import JSONResponse
import pandas as pd
from app.templates.data_dict import table_dict
from app.validations.tables import generate_filter_codes
import csv
import codecs
import re

app = FastAPI()
# uvicorn app.main:app --reload


@app.get("/")
async def root():
    return {"message": "Hello folks 😎."}


@app.post("/upload")
# def upload(table: str, file: UploadFile = File(...)):
def upload(file: UploadFile = File(...)):

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

    if nrows < 1 or nrows > 1000:
        return JSONResponse(status_code=405,
                            content={'error': 'The number of lines should be between 1 and 1000'})

    data = df.to_dict(orient="records")

    codeline = generate_filter_codes(df, ttype)
    loc = {'df': df}
    exec(codeline, globals(), loc)

    df_ok = loc['df_ok'].to_dict(orient="records")
    df_failed = loc['df_failed'].fillna('').to_dict(orient="records")
    return JSONResponse(status_code=200, content={'file': file.filename,
                                                  'type': ttype,
                                                  'columns': tcolumns,
                                                  'rows': nrows,
                                                  'data_failed': df_failed})
