import pandas as pd
import logging
class Data_Loader:
    def load_csv(path):
        """Read CSV file from remote path.

        Args:
        path(str): filepath to read.
        Returns:
        DataFrame of CSV file.
        """
        df = None
        try:
            df=pd.read_csv(path,parse_dates=['event_time'])
        except IOError:
            logging.exception('')
        return df

        