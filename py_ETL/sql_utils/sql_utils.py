import pandas as pd
from tqdm import tqdm
import sqlalchemy
from config import conn_string_prod


def CHONK(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def insert_with_progress(df, name,
                         schema,
                         if_exists, index,
                         CONN_STRING, dtype=None):
    ENGINE = sqlalchemy.create_engine(CONN_STRING, fast_executemany=True)
    chunksize = int(len(df) / 10)  # 10%
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(CHONK(df, chunksize)):
            if not dtype:
                cdf.to_sql(name=name,
                           con=ENGINE,
                           schema=schema,
                           if_exists=if_exists,
                           index=index)
            else:
                cdf.to_sql(name=name,
                           con=ENGINE,
                           schema=schema,
                           if_exists=if_exists,
                           index=index,
                           dtype=dtype)

            pbar.update(chunksize)
    print('Appended to table {}'.format(name))
