import redshift_connector


def upload_df_redshift(df, ttype):
    conn = redshift_connector.connect(
        host='company-cluster.ckcxnp1vuf26.us-east-1.redshift.amazonaws.com',
        database='company',
        user='dataeng',
        password='Strongpass1'
    )
    with conn.cursor() as cursor:
        cursor.write_dataframe(df, f"public.{ttype}")
        cursor.execute(f"select * from public.{ttype}; ")
        result = cursor.fetchall()
    conn.commit()
    conn.close()
    return result
