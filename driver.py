from Online_Seconds_Calculator import *
from Pre_Processor import *
import sys

if __name__=='__main__':
    
    """
    Driver function to call the Pre_Processor and Online_Seconds_Calculator modules 
    which will perform processing steps on the data and generate results
    """

    data_file_path=sys.argv[1]
    processed_data_file_path=sys.argv[2]
    results_file_path=sys.argv[3]

    pre_processor=Pre_Processor(Data_Loader.load_csv(data_file_path))
    pre_processor.process_and_save_data(processed_data_file_path)

    seconds_calculator=Online_Seconds_Calculator(Data_Loader.load_csv(processed_data_file_path))
    seconds_calculator.calculate_online_seconds_and_save_results(results_file_path)

    
