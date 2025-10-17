import logging
import time
from datetime import datetime 
from src.data_processor import DataProcessor
from src.logging_setup import setup_logger
from tests.test_1_customer import Test1
from src.behavioral_incentive import Incentive



def main():
    #initialize processor

    setup_logger()
    
    logger = logging.getLogger(__name__)
    
    Incentive('c1')

    logger.info("Welcome to DC Research!")
    
    processor = DataProcessor()
    test = Test1()

    #Test 1
    test.testing()
    
    
    


    




if __name__ == "__main__":
    main()
    