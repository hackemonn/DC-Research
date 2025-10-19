import logging
import time
from datetime import datetime 
import os 
import asyncio
from src.data_layer.processor import DataProcessor
from src.logging_setup import setup_logger
from tests.test_1_customer import Test1
from tests.test_2_50k import Test2
#from src.incentive import Incentive



async def main():
    #initialize processor

    setup_logger()
    
    logger = logging.getLogger(__name__)
    
    #Incentive('c1')

    logger.info("Welcome to DC Research!")
    
    await asyncio.sleep(1)
    
    #logger.info("Launching test1")
    
    processor = DataProcessor()
    test = Test1()

    #Test 1
    await test.testing()

    logger.info("Launching test2 in 1 second...")

    await asyncio.sleep(1)

    
    test2 = Test2(processor)

    await test2.run()
    

if __name__ == "__main__":
    asyncio.run(main())
    