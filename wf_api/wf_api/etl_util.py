import pandas as pd
import numpy as np
import glob
import ntpath
import pyodbc
import urllib3
from sqlalchemy import create_engine
from config import db_conn_string
import os
import logging

logger = logging.getLogger(__name__)
CONNECTION = None
engine = None
os.chdir("../input")

# get list of all data source folders that are NOT empty
data_source_list = [name for name in os.listdir(".") if os.path.isdir(name) and len(os.listdir(name)) != 0]
# used for dat files that may contain any delimiter
common_delimiters = [',', ';', '\t', ' ', '|', ':']
material_data = 'data_source_2/material_reference.csv'


def get_all_sample_data():

    df_all = pd.DataFrame()

    for path in data_source_list:
        # get filepaths for all csv files in directory
        filepath = glob.glob(path + "/*.csv")
        # extract filename
        path_head, file_name = ntpath.split(filepath[0])
        # only use data from sample_data files
        if file_name.startswith('sample_data'):
            df = pd.read_csv(filepath[0], index_col=None, header=0)
            # add file_origin column based on file source name
            df['file_origin'] = file_name
            df_all = pd.concat([df_all, df])

    for path in data_source_list:
        # get filepaths for all dat files in directory
        filepath = glob.glob(path + "/*.dat")
        # extract filename
        path_head, file_name = ntpath.split(filepath[0])
        # allow file to have any delimiter
        for d in common_delimiters:
            # only use data from sample_data files
            if file_name.startswith('sample_data'):
                df = pd.read_csv(filepath[0], index_col=None, header=0, sep=d)
                # check if df was correctly separated with a delimiter in common_delimiters
                if len(df.columns) > 1:
                    # add file_origin column based on file source name
                    df['file_origin'] = file_name
                    df_all = pd.concat([df_all, df])

    return df_all.reset_index(drop=True)


def filter_data_by_worth(df):
    # maintain column ordering consistency
    df_columns = df.columns

    # filter on sample_data_1
    sample_df_1 = df[df['file_origin'].str.contains('sample_data.1.csv')]
    sample_df_1 = sample_df_1[sample_df_1['worth'] > 1.0]
    sample_df_1 = sample_df_1[df_columns]

    # filter on sample_data_3
    sample_df_3 = df[df['file_origin'].str.contains('sample_data.3.dat')]
    sample_df_3 = sample_df_3.assign(worth=sample_df_3['worth'] * sample_df_3['material_id'])
    sample_df_3 = sample_df_3[df_columns]

    # filter on sample_data_2
    sample_df_2 = df[df['file_origin'].str.contains('sample_data.2.dat')]
    # break out worth, quality, material by filter needs
    sample_df_2_worth = sample_df_2.groupby(['product_name', 'file_origin'])['worth'].sum().reset_index()
    sample_df_2_qual = sample_df_2.groupby(['product_name'])['quality'].first().reset_index()
    sample_df_2_material = sample_df_2.groupby(['product_name'])['material_id'].max().reset_index()
    # merge back together
    sample_df_2_final = sample_df_2_worth.merge(sample_df_2_qual, on='product_name').merge(sample_df_2_material, on='product_name')
    sample_df_2_final = sample_df_2_final[df_columns]

    # concat all df
    df_all = pd.concat([sample_df_1, sample_df_3, sample_df_2_final]).reset_index(drop=True)
    # bring in materials
    materials_df = pd.read_csv(material_data, index_col=None, header=0)
    # join materials on material_id and id
    df_all = df_all.merge(materials_df, left_on='material_id', right_on='id', how='left')
    # organize columns for output
    final_cols_organized = ['product_name', 'material_name', 'quality', 'material_id', 'worth', 'file_origin']
    df_final = df_all[final_cols_organized]

    return df_final


def filter_df_by_quality(df, quality):

    quality_df = df[df['quality'].str.contains(quality)]

    return quality_df


def get_db_conn():
    global engine
    CONNECTION = pyodbc.connect(db_conn_string)
    if engine is None:
        params = urllib.parse.quote_plus(db_conn_string)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, echo=False, fast_executemany=True)
    return CONNECTION, engine


def push_df_to_db(df, table_name, method='pyodbc', fastexecute=True):
    df = df.replace("nan", np.NaN)
    df = df.replace([np.inf, -np.inf], np.Nan)
    df = df.replace({np.NaN:None})
    df = df.where(pd.notnull(df), None)
    connection, engine = get_db_conn()

    df_list = df.values.tolist()

    try:
        if method == "pyodbc":
            cursor = connection.cursor()
            cursor.fast_executemany = fastexecute
            logger.info(f"Inserting {len(df_list)} rows into {table_name}")
            columns = ','.join(["[" + str(col) + "]" for col in df.columns.values])
            sql_query = "insert into dbo.[{}] ({}) values (" + ("?, " * (len(df.columns.values) - 1)) + "?)"
            cursor.executemany(sql_query.format(table_name, columns), df_list)
            connection.commit()

        else:
            df.to_sql(table_name, con=engine, if_exists="append", index=True)
    except Exception as err:
        if fastexecute:
            try:
                push_df_to_db(df, table_name, fastexecute=False)
            except Exception as err2:
                raise UnableToUploadToDbError(err2)
        else: 
            raise UnableToUploadToDbError(err2)
    finally:
        connection.close()



