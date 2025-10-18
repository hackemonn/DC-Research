import os
import logging
from datetime import datetime
import uuid
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataProcessor:

    def __init__(self):
        # Load environment variables
        load_dotenv()

        try:
            self.db_conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                cursor_factory=DictCursor
            )
            self._init_db()
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _init_db(self):
        try:
            with self.db_conn.cursor() as cursor:
                # Ensure pgcrypto extension exists for gen_random_uuid()
                cursor.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

                # Create tables
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS merchants (
                    merchant_id TEXT PRIMARY KEY,
                    category TEXT DEFAULT 'General',
                    description TEXT DEFAULT '',
                    acc_balance NUMERIC(20,4) DEFAULT 0.0000,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                ''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id TEXT PRIMARY KEY,
                    age INTEGER CHECK (age >= 0) DEFAULT 18,
                    name_full TEXT NOT NULL,
                    profession TEXT DEFAULT 'Unknown',
                    salary NUMERIC(20,4) DEFAULT 0.0000,
                    level INTEGER CHECK (level > 0) DEFAULT 1,
                    acc_balance NUMERIC(20,4) DEFAULT 0.0000,
                    description TEXT DEFAULT '',
                    industry TEXT DEFAULT 'General',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                ''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS history (
                    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
                    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
                    amount NUMERIC(20,4) NOT NULL DEFAULT 0.0000,
                    time TIMESTAMPTZ DEFAULT NOW(),
                    is_rejected BOOLEAN DEFAULT FALSE,
                    b_old NUMERIC(20,4) DEFAULT 0.0000,
                    b_new NUMERIC(20,4) DEFAULT 0.0000
                );
                ''')

            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize database tables: {e}")
            self.db_conn.rollback()
            raise

    def save_customer(self, customer: dict):
        try:
            customer_id = customer.get('id')
            if not customer_id:
                raise ValueError("Customer dict must include 'id' key")
            with self.db_conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO customers
                        (customer_id, age, name_full, profession, salary, level, acc_balance, description, industry)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        age = EXCLUDED.age,
                        name_full = EXCLUDED.name_full,
                        profession = EXCLUDED.profession,
                        salary = EXCLUDED.salary,
                        level = EXCLUDED.level,
                        acc_balance = EXCLUDED.acc_balance,
                        description = EXCLUDED.description,
                        industry = EXCLUDED.industry,
                        updated_at = NOW()
                ''', (
                    customer_id,
                    customer.get('age', 18),
                    customer.get('name_full') or customer.get('name'),
                    customer.get('profession', 'Unknown'),
                    customer.get('salary', 0.0),
                    customer.get('level', 1),
                    customer.get('acc_balance', 0.0),
                    customer.get('description', ''),
                    customer.get('industry', 'General')
                ))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error saving customer {customer.get('id')}: {e}")
            self.db_conn.rollback()

    def save_merchant(self, merchant: dict):
        try:
            merchant_id = merchant.get('merchant_id') or merchant.get('id')
            if not merchant_id:
                raise ValueError("Merchant dict must include 'merchant_id' or 'id'")
            with self.db_conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO merchants
                        (merchant_id, category, description, acc_balance)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (merchant_id) DO UPDATE SET
                        category = EXCLUDED.category,
                        description = EXCLUDED.description,
                        acc_balance = EXCLUDED.acc_balance,
                        updated_at = NOW()
                ''', (
                    merchant_id,
                    merchant.get('category', 'General'),
                    merchant.get('description', ''),
                    merchant.get('acc_balance', 0.0)
                ))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error saving merchant {merchant.get('id')}: {e}")
            self.db_conn.rollback()

    def get_historical_data(self):
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute('''
                    SELECT h.history_id, h.customer_id, c.name_full, h.merchant_id, m.category,
                           h.amount, h.time, h.is_rejected, h.b_old, h.b_new
                    FROM history h
                    LEFT JOIN customers c ON c.customer_id = h.customer_id
                    LEFT JOIN merchants m ON m.merchant_id = h.merchant_id
                    ORDER BY h.time DESC
                ''')
                rows = cursor.fetchall()
                data = []
                for row in rows:
                    data.append({
                        'history_id': row['history_id'],
                        'customer_id': row['customer_id'],
                        'customer_name': row['name_full'],
                        'merchant_id': row['merchant_id'],
                        'merchant_category': row['category'],
                        'amount': row['amount'],
                        'time': row['time'],
                        'is_rejected': row['is_rejected'],
                        'b_old': row['b_old'],
                        'b_new': row['b_new']
                    })
                return data
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return []

    def add_h_data(self, values):
        try:
            if isinstance(values, dict):
                history_id = values.get('history_id') or str(uuid.uuid4())
                customer_id = values.get('customer_id')
                merchant_id = values.get('merchant_id')
                amount = values.get('amount', 0.0)
                time = values.get('time') or datetime.utcnow()
                is_rejected = values.get('is_rejected', False)
                b_old = values.get('b_old', 0.0)
                b_new = values.get('b_new', 0.0)
            else:
                (history_id, customer_id, merchant_id, amount, time, is_rejected, b_old, b_new) = values

            with self.db_conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO history
                        (history_id, customer_id, merchant_id, amount, time, is_rejected, b_old, b_new)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (history_id) DO NOTHING
                ''', (history_id, customer_id, merchant_id, amount, time, is_rejected, b_old, b_new))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error adding history data: {e}")
            self.db_conn.rollback()

    def enough_funds(self, customer_id, amount: float = 0.0):
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute('SELECT acc_balance FROM customers WHERE customer_id = %s', (customer_id,))
                result = cursor.fetchone()
                if not result:
                    raise ValueError(f"Customer {customer_id} not found")
                return result['acc_balance'] >= amount
        except Exception as e:
            logger.error(f"Error checking funds for customer {customer_id}: {e}")
            return False

    def balance_query(self, customer_id):
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute('SELECT acc_balance FROM customers WHERE customer_id = %s', (customer_id,))
                result = cursor.fetchone()
                if not result:
                    raise ValueError(f"Customer {customer_id} not found")
                return result['acc_balance']
        except Exception as e:
            logger.error(f"Error querying balance for {customer_id}: {e}")
            return 0.0

    def make_transaction(self, customer_id, merchant_id, amount, tr_id=None, time=None):
        tr_id = tr_id or str(uuid.uuid4())
        time = time or datetime.utcnow()
        try:
            with self.db_conn:
                with self.db_conn.cursor() as cursor:
                    cursor.execute('SELECT acc_balance FROM customers WHERE customer_id = %s', (customer_id,))
                    cust_row = cursor.fetchone()
                    if not cust_row or cust_row['acc_balance'] < amount:
                        logger.warning(f"Transaction failed: insufficient funds for {customer_id}")
                        return False

                    b_old = cust_row['acc_balance']
                    b_new = b_old - amount

                    cursor.execute('UPDATE customers SET acc_balance = %s, updated_at = NOW() WHERE customer_id = %s',
                                   (b_new, customer_id))
                    cursor.execute('UPDATE merchants SET acc_balance = acc_balance + %s, updated_at = NOW() WHERE merchant_id = %s',
                                   (amount, merchant_id))
                    self.add_h_data({
                        'history_id': tr_id,
                        'customer_id': customer_id,
                        'merchant_id': merchant_id,
                        'amount': amount,
                        'time': time,
                        'is_rejected': False,
                        'b_old': b_old,
                        'b_new': b_new
                    })
            return True
        except Exception as e:
            logger.error(f"Error making transaction {tr_id}: {e}")
            self.db_conn.rollback()
            return False

    def __del__(self):
        try:
            if self.db_conn:
                self.db_conn.close()
        except Exception as e:
            logger.warning(f"Error closing DB connection: {e}")
