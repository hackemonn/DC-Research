import asyncio
import logging
from datetime import datetime
from src.data_layer.processor import DataProcessor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Test1:
    async def testing(self):
        try:
            print("running Test1.testing()")
            dp = DataProcessor()
            await dp.init()  # initialize pool + tables

            # Save 1 customer
            await dp.save_customer({
                'id': 'c1',
                'age': 30,
                'name_full': 'Alice',
                'profession': 'Engineer',
                'salary': 50000,
                'level': 3,
                'acc_balance': 1000,
                'description': 'VIP customer',
                'industry': 'tech',
                'behavior': 'Aggressive'
            })

            # Verify if good
            async with dp.pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT customer_id, age, name_full, profession, salary, acc_balance, description '
                    'FROM customers WHERE customer_id=$1;', 'c1'
                )
            assert row is not None
            assert row['customer_id'] == 'c1'
            assert row['name_full'] == 'Alice'

            # Save 1 merchant
            await dp.save_merchant({
                'merchant_id': 'm1',
                'category': 'Food',
                'description': 'Local Cafe',
                'acc_balance': 5000,
            })

            # Verify merchant
            async with dp.pool.acquire() as conn:
                mrow = await conn.fetchrow(
                    'SELECT merchant_id, category, description, acc_balance '
                    'FROM merchants WHERE merchant_id=$1;', 'm1'
                )
            assert mrow is not None
            assert mrow['merchant_id'] == 'm1'

            # make transaction
            success = await dp.make_transaction(customer_id='c1', merchant_id='m1', amount=100)
            if success:
                logger.info("Transaction successful")
            else:
                logger.error("Transaction failed, insufficient balance")

            # Check history
            hist = await dp.get_historical_data()
            assert isinstance(hist, list)
            #assert len(hist) == 1  # exactly one transaction
            assert hist[0]['customer_id'] == 'c1'
            assert hist[0]['merchant_id'] == 'm1'
            assert float(hist[0]['amount']) == 100.0

            logger.info("Test1.testing(): all checks passed")
            
            #await dp.clear_db()
        except Exception as e:
            logger.error(f"Issue found: {e}")
            raise


if __name__ == "__main__":
    test = Test1()
    asyncio.run(test.testing())
