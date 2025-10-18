import logging
import time
from datetime import datetime 
import os 
from src.data_layer.processor import DataProcessor
from src.logging_setup import setup_logger
from tests.test_1_customer import Test1
#from tests.test_2_50k import Test2
#from src.incentive import Incentive



def main():
    #initialize processor

    setup_logger()
    
    logger = logging.getLogger(__name__)
    
    #Incentive('c1')

    logger.info("Welcome to DC Research!")
    
    time.sleep(1)
    
    logger.info("Launching test1")
    
    processor = DataProcessor()
    test = Test1()

    #Test 1
    test.testing()
    
    #processor.clean_pc()
    #os.remove(os.path.join("data", "pc.db"))
    
    logger.info("Launching test2 in 10 seconds...")
    time.sleep(10)


    #test2 = Test2()


    


    




if __name__ == "__main__":
    main()
    