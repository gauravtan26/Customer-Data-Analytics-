import pandas as pd
import datetime
from dataloading_utils import Data_Loader

class Pre_Processor:
    def __init__(self,df:pd.DataFrame):
        """
        Pre_Processor Initialization

        Args:
        df(Pandas DataFrame): DataFrame object having the unprocessed data
        
        """

        self.__df=df
        self.__initial_nproviders=None
        self.__nproviders_after_error_removal=None
        self.__nproviders_after_removing_events_on_or_after_7PM=None
        self.__status_stats=None

    def __display_first_n_rows(self,n=5):
        """
        Display first n rows of dataframe

        Args:
        n(int): number of rows to display.
        
        Returns: None
        
        """
        print("--------First {} Rows--------".format(n))
        print(self.__df.head(n))
    
    def __display_data_info(self):
        """
        Print a concise summary of a DataFrame.

        This method prints information about a DataFrame including the index dtype and columns, non-null values and memory usage.
        
        Returns: None
        
        """

        pd.options.display.max_info_rows=self.__df.shape[0]
        print(self.__df.info())

    def __event_time_to_date(self):
        """
        Extract date from event time and print first 5 rows of dataframe after adding 'date' column
        
        Returns: None
        
        """

        self.__df['date']=self.__df.event_time.apply(lambda x: datetime.datetime.strftime(x, '%d-%b'))
        print(self.__df.date.head(5))

    def __event_time_to_hour(self):
        """
        Extract hour from event time and print first 5 rows of dataframe after adding 'hour' column
        
        Returns: None
        
        """

        self.__df['hour']=self.__df.event_time.apply(lambda x: datetime.datetime.strftime(x, '%H'))
        print(self.__df.hour.head(5))

    def __calculate_initial_no_unique_providers(self):
        """
        Calculate the number of unique providers in the original data
        
        Returns: None
        
        """

        self.__initial_nproviders=self.__df.provider_id.nunique()
        print("--------------Initial Number of Providers: {} ---------------------".format(self.__initial_nproviders))

    def __check_status(self,row,detail_col_name='detail',source_col_name='source'):
        """
        Extract status of a row 

        Args:

        - row(pandas.core.series.Series): row for which the status has to be calculated

        - detail_col_name(str): name of the column having detail attribute of data. Default = 'detail'
        
        - source_col_name(str): name of the column having source attribute of data. Default = 'source'

        Returns: status: str or None
                    the status of the row given in the argurment


        """

        if (type(row[detail_col_name])==str  and row[detail_col_name].find('True')!=-1) or row[source_col_name]=='Action on Job':
            return 'online'
        elif type(row[detail_col_name])==str and row[detail_col_name].find('False')!=-1:
            return 'offline'
        else:
            return None

    def __extract_status(self):
        """
        Extract status values for all the rows in dataframe
        
        Returns: None
        
        """

        self.__df['status']=self.__df.apply(self.__check_status,axis=1)
        
    def __drop_null_status_entries(self):
        """
        Drop rows for which status column contains null value 
        
        Returns: None
        
        """

        self.__df.dropna(subset=['status'],inplace=True)

    def __calculate_error_row_statistics(self):
        """
        Calculate and print the counts of providers removed due to error removal and left after that

        Returns: None 

        """

        self.__nproviders_after_error_removal=self.__df.provider_id.nunique()
        print("------------ Number of Providers Removed Due to Removal of Error Rows: {} -------------".
        format(self.__initial_nproviders -self.__nproviders_after_error_removal))

        print("Number of Providers Left After Removing Error Rows: {}".format(self.__nproviders_after_error_removal))

    def __remove_events_on_or_after_7PM(self):
        """
        Select the events with hour values < 7 PM (19) and Print the number of providers left after this selection

        Returns: None
        
        """


        self.__df=self.__df[self.__df.hour<'19']
        self.__nproviders_after_removing_events_on_or_after_7PM=self.__df.provider_id.nunique()
        print("-------------- Number of unique providers left after removing events on or after 7PM: {} ------------------".
        format(self.__nproviders_after_removing_events_on_or_after_7PM))

    def __sort_data(self):
        """
        Sort the dataframe according to the values in the columns: ['provider_id','event_time','status']

        Returns: None

        """
        self.__df.sort_values(by=['provider_id','event_time','status'],inplace=True)

    def __remove_redundant_columns(self):
        """
        Drop the irrelevant columns from the dataframe

        Returns: None
        
        """

        self.__df.drop(['detail','source'],axis=1,inplace=True)

    def __drop_duplicates(self):
        """
        Drop duplicate rows from dataframe based on the values in the columns ['provider_id','event_time'].
        Keeping the last row of duplicates will preserve the rows with status online and will remove the rows with 
        offline. Hence, giving preference to 'online'

        Returns: None
        
        """

        self.__df.drop_duplicates(subset=['provider_id','event_time'],keep='last',ignore_index=True,inplace=True)

    def __calculate_status_statistics(self):
        
        """
        Calculate and print the number of online and offline status entries.

        Returns: None 
        """

        self.__status_stats=self.__df.status.value_counts()
        print("----------- Status Value Counts ------------")
        print(self.__status_stats)

    def __save_data(self,processed_file_path):

        """
        Save the processed data

        Args:
        - processed_file_path(str): path to store the processed file

        Returns: None 
        """
        print(" ---- Saving the Processed Data ----")
        self.__df.to_csv(processed_file_path,index=False)

    def process_and_save_data(self,processed_file_path):
        """
        Public Function to call the private functions of the Pre_Processor class
        
        Returns:
            None
        """

        self.__display_first_n_rows() 
        self.__display_data_info()
        print(' ------------Performing Processing Steps on Data-----------------')

        self.__event_time_to_date()
        self.__event_time_to_hour()    
        self.__calculate_initial_no_unique_providers()
        self.__extract_status()
        self.__drop_null_status_entries()
        self.__calculate_error_row_statistics()
        self.__remove_events_on_or_after_7PM()
        self.__sort_data()
        self.__remove_redundant_columns()
        self.__drop_duplicates()
        self.__calculate_status_statistics()
        self.__save_data(processed_file_path)






    