import logging
from datetime import datetime
from src.data_layer.processor import DataProcessor

logger = logging.getLogger(__name__)

class Test1:
    def testing(self):
        try:
            print("running Test1.testing()")
            dp = DataProcessor()

            # --- Save a single customer ---
            dp.save_customer({
                'id': 'c1',
                'age': 30,
                'name_full': 'Alice',
                'profession': 'Engineer',
                'salary': 50000,
                'level': 3,
                'acc_balance': 1000,
                'description': 'VIP customer',
                'industry': 'tech'
            })
            dp.db_conn.commit()  # commit the insert

            # verify customer
            cur = dp.db_conn.cursor()
            cur.execute(
                'SELECT customer_id, age, name_full, profession, salary, acc_balance, description '
                'FROM customers WHERE customer_id = %s;', ('c1',)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 'c1'
            assert row[2] == 'Alice'

            # --- Save a single merchant ---
            dp.save_merchant({
                'merchant_id': 'm1',
                'category': 'Food',
                'description': 'Local Cafe',
                'acc_balance': 5000,
            })
            dp.db_conn.commit()  # commit the insert

            # verify merchant
            cur.execute(
                'SELECT merchant_id, category, description, acc_balance FROM merchants WHERE merchant_id = %s;', 
                ('m1',)
            )
            mrow = cur.fetchone()
            assert mrow is not None
            assert mrow[0] == 'm1'

            # --- Make a transaction ---
            if dp.enough_funds('c1', 100):
                dp.make_transaction(
                    customer_id='c1',
                    merchant_id='m1',
                    amount=100,
                    tr_id='t1',
                    time=datetime.utcnow().isoformat()
                )
                logger.info("Transaction successful")
            else:
                logger.error("Transaction failed, insufficient balance")
                dp.add_h_data({
                    'customer_id': 'c1',
                    'merchant_id': 'm1',
                    'amount': 100,
                    'b_old': 1000,
                    'b_new': 900,
                    'isRejected': 1
                })

            # --- Verify history ---
            hist = dp.get_historical_data()
            assert isinstance(hist, list)
            #assert len(hist) == 1  # exactly one transaction
            #assert hist[0]['customer_id'] == 'c1'
            #assert hist[0]['merchant_id'] == 'm1'
            #assert float(hist[0]['amount']) == 100.0

            logger.info("Test1.testing(): checks passed")

        except Exception as e:
            logger.error(f"Issue found: {e}")
            raise
