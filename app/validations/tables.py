from app.templates.data_dict import table_dict


def generate_filter_codes(df, ttype):

    codeline = ''
    filters = []
    for c in list(df.columns):

        dtype = table_dict[ttype][c]

        filter_name = f'filter_{c}'

        if dtype == 'INTEGER':
            codeline += f"\n{filter_name} = df['{c}'].str.contains('^[0-9]*$').notnull().fillna(False)"
        elif dtype == 'ISO_DATE':
            codeline += f";{filter_name} =  pd.to_datetime(df['{c}'], errors='coerce',format='%Y-%m-%dT%H:%M:%SZ').notnull().fillna(False)"
        elif dtype == 'STRING':
            codeline += f"\n{filter_name} = df['{c}'].notnull()"
        filters.append(filter_name)
    codeline += "\ndf_ok = df[("+')&('.join(filters)+")]"
    codeline += "\ndf_failed = df[(~"+')|(~'.join(filters)+")]"
    return codeline
