from pathlib import Path
import os
import pandas as pd
import s3fs
import streamlit as st

GASBUDDY_BUCKET = 'gasbuddybucket'


def pull_secret(key):
    try:
        return st.secrets[key]
    except FileNotFoundError:
        return os.getenv(key, None)


def get_s3filesystem():
    s3 = s3fs.S3FileSystem(
        key=pull_secret("GASBUDDY_ACCESS_KEY"),
        secret=pull_secret("GASBUDDY_SECRET_KEY")
    )

    return s3


def upload_s3(df, filename):
    s3 = get_s3filesystem()
    pathname = f'{GASBUDDY_BUCKET}/{filename}'
    if s3.exists(pathname):
        with s3.open(pathname, 'rb') as f:
            existing_df = pd.read_parquet(f)
        df = pd.concat([existing_df,  df])

    with s3.open(pathname, 'wb') as f:
        df.drop_duplicates().to_parquet(f, index=False)

