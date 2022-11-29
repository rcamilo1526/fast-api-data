import redshift_connector
import pandas as pd

creds = {'host': 'company-cluster.cij1xx7rk2xm.us-east-1.redshift.amazonaws.com',
         'database': 'company',
         'user': 'dataeng',
         'password': 'Strongpass1'}


def upload_df_redshift(df, ttype):
    conn = redshift_connector.connect(**creds)
    with conn.cursor() as cursor:
        cursor.write_dataframe(df, f"public.{ttype}")
        cursor.execute(f"select * from public.{ttype}; ")
        result = cursor.fetchall()
    conn.commit()
    conn.close()
    return result


def backup_table(ttype):
    conn = redshift_connector.connect(**creds)
    with conn.cursor() as cursor:
        cursor.execute(f"CALL BACKUP_{ttype}();")
    conn.commit()
    conn.close()
    return f'Backup process for {ttype} started.'


def restore_table(ttype):
    conn = redshift_connector.connect(**creds)
    with conn.cursor() as cursor:
        cursor.execute(f"CALL RESTORE_{ttype}();")
    conn.commit()
    conn.close()
    return f'Table {ttype} restored from backup.'


def get_insights(type, limit):
    conn = redshift_connector.connect(**creds)
    with conn.cursor() as cursor:
        cursor.execute(f"select * from public.{type} limit {limit};")
        result = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(result )
        df.columns = field_names
    conn.commit()
    conn.close()
    return df.to_dict(orient="records")
