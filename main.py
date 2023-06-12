# This is a sample Python script.


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import datahenter
import pandas as pd
import os
import datetime as dt
import plotly.express as px

# Sample code for getting a dataframe with data from either smg or brady according to data in the config file
def get_df(start: dt.datetime, end: dt.datetime, config_filename: str, db='SMG') -> pd.DataFrame:

    # Creating an empty dataframe with just the headings - no data
    COLUMN_NAMES = ['DESCRIPTION', 'TIMESERIE']
    df = pd.DataFrame(columns=COLUMN_NAMES)

    current_file = os.path.abspath(os.path.dirname(__file__))
    try:
        config_filename = os.path.join(current_file, config_filename)
    except FileNotFoundError:
        print('not found config')
        return df  # just return an empty dataframe

    if db == 'SMG':
        with datahenter.datahenter('SMG_PROD') as dh_smg:
            timeseries = dh_smg.read_config(filename=config_filename)
            df = dh_smg.get_data(start, end, timeseries, resample='H')

    elif db == 'BRADY':
        with datahenter.datahenter('BRADY_PROD') as dh_brady:
            timeseries = dh_brady.read_config(filename=config_filename)
            df = dh_brady.get_data(start, end, timeseries, resample='H')
    else:
        return df  # emtpy df:

    return df  # return the dataframe...

#example code for calculating with dataframes and returning a dataframe result
def df_ume(df: pd.DataFrame) -> pd.DataFrame:

    df_result=\
           df['Bjurfors Övre plan']+\
           df['Bjurfors Nedre plan']+\
           df['Harrsele plan']

    return df_result

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
   start='2023-05-01 00:00'
   end='2023-06-07 23:00'
   df_smg=get_df(start,end,'smg_config',db='SMG')
   df_brady=get_df(start,end,'brady_config',db='BRADY')
   df_all=pd.concat([df_brady, df_smg], axis=1)
   fig=px.line(df_brady['SE1 Spotpris'])
   fig.show()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
