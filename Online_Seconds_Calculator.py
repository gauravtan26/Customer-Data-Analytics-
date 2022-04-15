import pandas as pd
from dataloading_utils import *
import constants
import math
import datetime

class Online_Seconds_Calculator:
    def __init__(self,df:pd.DataFrame):
        """
        Online Seconds Initialization

        Args:
        df(Pandas DataFrame): DataFrame object having the processed data
        
        """
        self.__df=df
        self.__status='offline'
        self.__joined_df=None
        self.__format='''%Y-%m-%d %H:%M:%S'''
        self.__online_seconds_df=None

    def __select_relevant_columns(self,columns=['provider_id','event_time','date','hour','status']):
        """
        Select the columns provided as the argument from the dataframe  

        Args:
        columns(list): List of columns to be selected
        Returns: None
        
        """

        self.__df=self.__df[columns]
    
    def __calculate_status_of_row(self,row):
        """
        Calculate the status of a row based on the status of the previous row
        The calculated status can take 1 out of the 4 values: 'online', 'offline', 'offline_online', 'online_offline'

        Args:
        row(pandas.core.series.Series): The row for which status has to be calculated

        Returns:
            calculated status: str
        """


        if int(row['Hour Start Time'])==0:
            self.__status='offline'
        if pd.isnull(row.status):
            return self.__status
        elif row.status==self.__status:            
            return self.__status              
        elif row.status=='online' and self.__status =='offline':
            self.__status='online'
            return 'offline_online'
        elif row.status=='offline' and self.__status=='online':
            self.__status='offline'
            return 'online_offline'

    def __combine_time_and_status_of_row(self,row):
        """
        Combine the event time and status of a row

        Args:
        row(pandas.core.series.Series): The row for which event time and status have to be combined

        Returns:
            combined status: str
        """


        if pd.isnull(row.event_time):
            return row['calculated status']
        else:
            return str(row.event_time)+'/'+row['calculated status']  

    def __add_all_possible_hrs_and_dates_rows_to_data(self):
        """
        Form a dataframe by joining dates, hours and unique provider_id values and merge that dataframe with the processed dataframe
    
        Returns:
            combined status: str
        """        
        

        dates= pd.DataFrame(constants.DATES,columns=['date']) 
        ids = pd.DataFrame(self.__df.provider_id.unique(),columns=['provider_id'])
        hours=pd.DataFrame(constants.HOURS,columns=['Hour Start Time'])
        ids['key'] = 1
        dates['key'] = 1
        result = pd.merge(ids, dates, on ='key').drop("key", 1)
        result['key'] = 1
        hours['key'] = 1
        result = pd.merge(result, hours, on ='key').drop("key", 1)
        self.__joined_df=result.merge(self.__df, left_on=['provider_id','date','Hour Start Time'], right_on=['provider_id','date','hour'],how='left')

    def __calculate_status(self):
        """
        Add a calculated status column to dataframe by calculating status for all rows in the dataframe

        Returns:
            None
        """

        self.__joined_df['calculated status']=self.__joined_df.apply(lambda x: self.__calculate_status_of_row(x),axis=1)

    def __combine_time_and_status(self):
        """
        Add a combined_time_and_status column to dataframe by combining event_time and status values of each row

        Returns:
            None

        """

        self.__joined_df['combined_time_and_status']=self.__joined_df.apply(lambda x: self.__combine_time_and_status_of_row(x),axis=1)

    def __select_operational_hours(self):
        """
        Select rows based on the operational hours

        Returns: None
        """

        self.__joined_df=self.__joined_df[self.__joined_df['Hour Start Time']>=constants.MORNING_OPERATIONAL_TIME]
    
    def __calculate_online_seconds(self,rows):
        """
        Calculate number of online seconds for an hour interval

        Args:
            rows(pandas.core.series.Series): rows corresponding to an hour interval

        Returns:
            seconds_online: float
        """

        time_status=list(rows)
        status=[]
        timestamp=[]
        if time_status[0][0]=='o':
            time_status[0]='2017-09-01 00:00:00'+'/'+time_status[0]
        for item in time_status:
            a=item.split('/')
            status.append(a[1])
            timestamp.append(datetime.datetime.strptime(a[0], self.__format))

        seconds_online=0
        n=len(timestamp)
        
        if status[0]=='online_offline' or status[0]=='online':
            seconds_online+=60*timestamp[0].minute+timestamp[0].second
        
        for i in range(n-1):
            if status[i]=='online' or status[i]=='offline_online':
                seconds_online+=(timestamp[i+1]-timestamp[i]).total_seconds()
        
        if status[n-1]=='online' or status[n-1]=='offline_online':
            seconds_online+=60*(60-(timestamp[n-1].minute+1))+60-timestamp[n-1].second
        return seconds_online


    def __group_rows_and_calculate_seconds(self):
        """
        Group Rows by columns ['provider_id','date','Hour Start Time'] and calculate online seconds for each group

        Returns:
            None 

        """

        self.__online_seconds_df=self.__joined_df[
            ['provider_id','date','Hour Start Time','combined_time_and_status']
            ].groupby(by=['provider_id','date','Hour Start Time']).agg(
            {'combined_time_and_status':self.__calculate_online_seconds}).reset_index()
        
    def __calculate_hour_end_time(self):
        """
        Calculate hour end time for each row
        Returns:
            None

        """


        self.__online_seconds_df['Hour End Time'] = self.__online_seconds_df['Hour Start Time'].apply(lambda x: x+1)
        self.__online_seconds_df.rename(columns={'combined_time_and_status':'Seconds Online'},inplace=True)
        self.__online_seconds_df=self.__online_seconds_df.reindex(columns=['provider_id','date','Hour Start Time','Hour End Time','Seconds Online'])

    def __print_online_seconds_stats(self):
        """
        Calculate and print online second statistics

        Returns:
            None
        
        """

        print('---- Number of Rows in final ouput --------')
        print(self.__online_seconds_df.shape[0])

        print('---------- Total Online Seconds ----------')
        print(sum(self.__online_seconds_df['Seconds Online']))
        
        print('--------- No of Rows in final output where seconds online -------')
        print(sum(self.__online_seconds_df['Seconds Online']>0))
    
    def __save_results_data(self,results_path):
        """
        Save the result DataFrame in csv format
        
        Returns:
            None
        """

        print('---------- Saving Results in a csv file -----------')
        

        self.__online_seconds_df.to_csv(results_path,index=False)

    def calculate_online_seconds_and_save_results(self,results_path):

        """
        Public Function to perform all the steps to calculate provider wise,  hour wise online seconds and save the results

        Args:
            results_path(str): path to save the results in csv format

        Returns:
            None
        
        """

        self.__select_relevant_columns()
        self.__add_all_possible_hrs_and_dates_rows_to_data()
        self.__calculate_status()
        self.__combine_time_and_status()
        self.__select_operational_hours()
        self.__group_rows_and_calculate_seconds()
        self.__calculate_hour_end_time()
        self.__print_online_seconds_stats()
        self.__save_results_data(results_path)


