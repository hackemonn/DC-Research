import pytest
import os

from src.data_processor import DataProcessor

class Test1:
    def testing(self):
        try:
            print("running Test1.testing()")
            dp = DataProcessor(db_file=os.path.join("data", "pc.db"))

            # save a customer
            dp.save_customer({
                'id': 'c1',
                'age': 30,
                'name_full': 'Alice',
                'profession': 'Engineer',
                'salary': 50000,
                'level': 3,
                'acc_balance': 1000,
                'description': 'VIP customer',
            })
            
            cur = dp.db_conn.cursor()
            cur.execute('SELECT customer_id, age, name_full, profession, salary, acc_balance, description FROM customers WHERE customer_id = ?', ('c1',))
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 'c1'
            assert row[2] == 'Alice'

            # save a merchant
            dp.save_merchant({
                'merchant_id': 'm1',
                'category': 'Food',
                'description': 'Local Cafe',
                'acc_balance': 5000,
            })
            
            print(dp.enoughFunds('c1', 100))

            cur.execute('SELECT merchant_id, category, description, acc_balance FROM merchants WHERE merchant_id = ?', ('m1',))
            mrow = cur.fetchone()
            assert mrow is not None
            assert mrow[0] == 'm1'

            # add history entry
            dp.add_h_data({
                'customer_id': 'c1',
                'merchant_id': 'm1',
                'amount': 100,
                'b_old': 1000,
                'b_new': 900,
            })

            hist = dp.get_historical_data()
            assert isinstance(hist, list)
            #assert len(hist) == 1
            assert hist[0]['customer_id'] == 'c1'
            assert hist[0]['merchant_id'] == 'm1'
            assert hist[0]['amount'] == 100

            print("Test1.testing(): checks passed")
        except Exception as e:
            print(f"issue found in {e}")
            raise
