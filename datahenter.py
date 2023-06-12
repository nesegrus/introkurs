from statkraft.ssa.wrappers import ReadWrapper
import pandas as pd


class datahenter:

    def __enter__(self):
        return self

    def __init__(self, database):
        self.__database = database
        
    
    def __exit__(self, exception_type, exception_val, trace):
        # once the with block is over, the __exit__ method would be called
        # with that, you close the connnection
        try:
            #self.connection.close()
            print('connection to db closed')
        except AttributeError:  # isn't closable
            print('Not closable.')
            return True  # exception handled successfully
        
    @property
    def database(self):
        return self.__database
    
    @property
    def df(self):
        return self.__df
   
    
    def df_from_db(self,ts, ts_start, ts_end, timezone='Europe/Oslo', db=database,resample='H'):
        """

        :param ts: dict with ts_id's and description for each timeseries        :
        :param ts_start: a date string. ex: '2019-09-01 00:00'
        :param ts_end: ex: a date string. '2019-09-01 00:00'
        :param timezone: default 'Europe/Oslo'
        :return: A pandas dataframe
        :
        """
        
        #ts_ids=ts['TIMESERIE']
        #"ts_ids_description=ts['DESCRIPTION']
        
        #ts_hent=dict(zip(ts_ids_description, ts_ids))
        
        
        db_reader = ReadWrapper(read_from=db)
        start_time = pd.Timestamp(ts_start).tz_localize(timezone)
        end_time = pd.Timestamp(ts_end).tz_localize(timezone)



        ts = db_reader.read(ts,
                        start_time.tz_convert('utc'),
                        end_time.tz_convert('utc'),
                        max_time_series_per_query=200)  # default is 100
        if db == 'SMG_PROD':
            # Forward fill smg series - knekkpunktserier så bruker siste verdi kjent
            pframe = ts.resample(resample).mean().fillna(method='ffill')
            #Dersom siste verdi kjent ikke finnes - så legg inn 0
            pframe=pframe.fillna(0)
            #pframe=pframe.between_time(start_time.tz_convert('utc'),  end_time.tz_convert('utc'))
            pframe = pframe[(pframe.index >= start_time.tz_convert('utc')) & (pframe.index <= end_time.tz_convert('utc'))]

        if db == 'BRADY_PROD':
            pframe = ts.resample('H').mean().fillna(0)
        
        self.__df=pframe.tz_convert(timezone)
        
        return self.df

    def get_data(self, dt_from, dt_to, timeseries_dict,resample='H'):
        
        """
        :param dt_from date string. ex: '2019-09-01 00:00'
        :param dt_to date string. ex: '2019-09-01 00:00'
        :param timeseries_dict dict with description,ts
        :paramt resample str , example 'H'=hour        
        """
        
        return self.df_from_db(timeseries_dict, dt_from,dt_to, db=self.__database,resample=resample)

    def read_config(self,filename=None):
    
        """
        :param filename is the name of the file with the following struct        
        example : 
                DESCRIPTION;TIMESERIE
                Solgt SE1-FCR-N D1;305005
                ..
                ..
                
        :return a dict with timeseries and description to use as input to df_from_db function
        """
        
        df = pd.read_csv(filename, sep=";", index_col=None, header=0).squeeze(1)
        
        ts_id=df['TIMESERIE']
        ts_description=df['DESCRIPTION']
        
        df_dict=dict(zip(ts_description,ts_id))
        
        return df_dict