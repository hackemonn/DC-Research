import os
import logging
import asyncio
import uuid
from datetime import datetime
from decimal import Decimal

from dotenv import load_dotenv

import asyncpg


# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Constants
BONUS_RATE = 0.03
DECAY_RATE = 0.02
TARGET_VELOCITY = 0.5


class DataProcessor:
    def __init__(self):

        load_dotenv()

        self.db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}"
        self.pool = None

    async def init(self):
        try:
            self.pool = await asyncpg.create_pool(dsn=self.db_url, min_size=1, max_size=10)
            await self._init_db()
        except Exception as e:
            logger.error(f"Failed to initialize DB pool: {e}")
            raise

    # Initialize Database
    async def _init_db(self):
        async with self.pool.acquire() as conn:
            try:
                await conn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

                # Core tables
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id TEXT PRIMARY KEY,
                    age INT CHECK(age >= 0) DEFAULT 18,
                    name_full TEXT NOT NULL,
                    profession TEXT DEFAULT 'Unknown',
                    salary NUMERIC(20,4) DEFAULT 0.0,
                    level INT CHECK(level > 0) DEFAULT 1,
                    acc_balance NUMERIC(20,4) DEFAULT 0.0,
                    description TEXT DEFAULT '',
                    industry TEXT DEFAULT 'General',
                    behavior TEXT DEFAULT 'Conservative',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );''')

                await conn.execute('''
                CREATE TABLE IF NOT EXISTS merchants (
                    merchant_id TEXT PRIMARY KEY,
                    category TEXT DEFAULT 'General',
                    description TEXT DEFAULT '',
                    acc_balance NUMERIC(20,4) DEFAULT 0.0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );''')

                await conn.execute('''
                CREATE TABLE IF NOT EXISTS history (
                    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_id TEXT REFERENCES customers(customer_id),
                    merchant_id TEXT REFERENCES merchants(merchant_id),
                    amount NUMERIC(20,4) DEFAULT 0.0,
                    time TIMESTAMPTZ DEFAULT NOW(),
                    is_rejected BOOLEAN DEFAULT FALSE,
                    b_old NUMERIC(20,4) DEFAULT 0.0,
                    b_new NUMERIC(20,4) DEFAULT 0.0
                );''')

                # Metrics tables
                await conn.execute('''
                CREATE TABLE IF NOT EXISTS cust_core (
                    cust_id TEXT PRIMARY KEY REFERENCES customers(customer_id),
                    avg_daily_bal NUMERIC(20,4) DEFAULT 0.0,
                    max_bal NUMERIC(20,4) DEFAULT 0.0,
                    min_bal NUMERIC(20,4) DEFAULT 0.0,
                    bal_std NUMERIC(20,4) DEFAULT 0.0,
                    inactive_days INT DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );''')

                await conn.execute('''
                CREATE TABLE IF NOT EXISTS freqvol (
                    cust_id TEXT PRIMARY KEY REFERENCES customers(customer_id),
                    num_tr_day INT DEFAULT 0,
                    num_tr_week INT DEFAULT 0,
                    avg_tr_val NUMERIC(20,4) DEFAULT 0.0,
                    total_tr_val NUMERIC(20,4) DEFAULT 0.0,
                    tr_std NUMERIC(20,4) DEFAULT 0.0,
                    velocity NUMERIC(20,4) DEFAULT 0.0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );''')

                await conn.execute('''
                CREATE TABLE IF NOT EXISTS cust_incentives (
                    cust_id TEXT PRIMARY KEY REFERENCES customers(customer_id),
                    cashback_earned NUMERIC(20,4) DEFAULT 0.0,
                    decay_loss_cnt INT DEFAULT 0,
                    incentive_resp NUMERIC(5,4) DEFAULT 1.0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );''')

                logger.info("Database tables initialized successfully.")
            except Exception as e:
                logger.error(f"DB initialization error: {e}")
                raise

    # Core functions
    async def save_customer(self, customer: dict):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                INSERT INTO customers(customer_id, age, name_full, profession, salary, level, acc_balance, description, industry, behavior)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT(customer_id) DO UPDATE SET
                    age=EXCLUDED.age,
                    name_full=EXCLUDED.name_full,
                    profession=EXCLUDED.profession,
                    salary=EXCLUDED.salary,
                    level=EXCLUDED.level,
                    acc_balance=EXCLUDED.acc_balance,
                    description=EXCLUDED.description,
                    industry=EXCLUDED.industry,
                    behavior=EXCLUDED.behavior,
                    updated_at=NOW()
                ''',
                customer['id'], customer.get('age', 18), customer.get('name_full') or customer.get('name'),
                customer.get('profession', 'Unknown'), customer.get('salary', 0.0), customer.get('level', 1),
                customer.get('acc_balance', 0.0), customer.get('description',''), customer.get('industry','General'),
                customer.get('behavior', 'Conservative'))
                logger.info(f"Customer {customer['id']} saved/updated successfully.")
        except Exception as e:
            logger.error(f"Error saving customer {customer.get('id')}: {e}")

    async def save_merchant(self, merchant: dict):
        try:
            merchant_id = merchant.get('merchant_id') or merchant.get('id')
            async with self.pool.acquire() as conn:
                await conn.execute('''
                INSERT INTO merchants(merchant_id, category, description, acc_balance)
                VALUES($1,$2,$3,$4)
                ON CONFLICT(merchant_id) DO UPDATE SET
                    category=EXCLUDED.category,
                    description=EXCLUDED.description,
                    acc_balance=EXCLUDED.acc_balance,
                    updated_at=NOW()
                ''',
                merchant_id, merchant.get('category','General'), merchant.get('description',''), merchant.get('acc_balance',0.0))
                logger.info(f"Merchant {merchant_id} saved/updated successfully.")
        except Exception as e:
            logger.error(f"Error saving merchant {merchant_id}: {e}")

    # make transaction
    async def make_transaction(self, customer_id, merchant_id, amount: float):
        tr_id = str(uuid.uuid4())
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    cust = await conn.fetchrow('SELECT acc_balance FROM customers WHERE customer_id=$1', customer_id)
                    if not cust:
                        logger.warning(f"Customer {customer_id} not found")
                        return False
                    if cust['acc_balance'] < amount:
                        logger.warning(f"Transaction failed: insufficient funds for {customer_id}")
                        # record rejected transaction
                        await conn.execute('''
                        INSERT INTO history(customer_id, merchant_id, amount, is_rejected, b_old, b_new)
                        VALUES($1,$2,$3,TRUE,$4,$4)
                        ''', customer_id, merchant_id, amount, cust['acc_balance'])
                        return False

                    b_old = cust['acc_balance']
                    b_new = b_old - Decimal(amount)

                    await conn.execute('UPDATE customers SET acc_balance=$1, updated_at=NOW() WHERE customer_id=$2', b_new, customer_id)
                    await conn.execute('UPDATE merchants SET acc_balance=acc_balance+$1, updated_at=NOW() WHERE merchant_id=$2', amount, merchant_id)

                    await conn.execute('''
                    INSERT INTO history(history_id, customer_id, merchant_id, amount, b_old, b_new)
                    VALUES($1,$2,$3,$4,$5,$6)
                    ''', tr_id, customer_id, merchant_id, amount, b_old, b_new)

                    # Update metrics asynchronously
                    asyncio.create_task(self.update_metrics(customer_id, amount, b_new))

                    return True
                except Exception as e:
                    logger.error(f"Error processing transaction {tr_id}: {e}")
                    raise

    # Metrics

    #issues to solve tomorrow
    #Float and decimal being not compatible with each other
    async def update_metrics(self, cust_id, amount, b_new):
        amount = Decimal(amount)
        b_new = Decimal(b_new)
        async with self.pool.acquire() as conn:
            try:
                # --- cust_core ---
                core = await conn.fetchrow('SELECT * FROM cust_core WHERE cust_id=$1', cust_id)
                if not core:
                    await conn.execute('INSERT INTO cust_core(cust_id, avg_daily_bal, max_bal, min_bal, bal_std, inactive_days) VALUES($1,$2,$3,$4,$5,$6)', cust_id, b_new, b_new, b_new, 0.0, 0)
                else:
                    avg_daily_bal = (core['avg_daily_bal'] + b_new) / 2
                    max_bal = max(core['max_bal'], b_new)
                    min_bal = min(core['min_bal'], b_new)
                    await conn.execute('UPDATE cust_core SET avg_daily_bal=$1, max_bal=$2, min_bal=$3, updated_at=NOW() WHERE cust_id=$4',
                                       avg_daily_bal, max_bal, min_bal, cust_id)

                # --- freqvol ---
                freq = await conn.fetchrow('SELECT * FROM freqvol WHERE cust_id=$1', cust_id)
                if not freq:
                    await conn.execute('INSERT INTO freqvol(cust_id, num_tr_day, num_tr_week, avg_tr_val, total_tr_val, velocity) VALUES($1,1,1,$2,$2,$3)',
                                       cust_id, amount, amount / TARGET_VELOCITY)
                else:
                    num_tr_day = freq['num_tr_day'] + 1
                    num_tr_week = freq['num_tr_week'] + 1
                    total_tr_val = freq['total_tr_val'] + amount
                    avg_tr_val = total_tr_val / num_tr_week
                    velocity = total_tr_val / TARGET_VELOCITY
                    await conn.execute('UPDATE freqvol SET num_tr_day=$1, num_tr_week=$2, total_tr_val=$3, avg_tr_val=$4, velocity=$5, updated_at=NOW() WHERE cust_id=$6',
                                       num_tr_day, num_tr_week, total_tr_val, avg_tr_val, velocity, cust_id)

                # --- cust_incentives ---
                inc = await conn.fetchrow('SELECT * FROM cust_incentives WHERE cust_id=$1', cust_id)
                bonus = amount * Decimal(BONUS_RATE)
                if not inc:
                    await conn.execute('INSERT INTO cust_incentives(cust_id, cashback_earned, decay_loss_cnt, incentive_resp) VALUES($1,$2,0,1.0)',
                                       cust_id, bonus)
                else:
                    cashback = inc['cashback_earned'] + bonus
                    decay_loss = inc['decay_loss_cnt']
                    incentive_resp = min(1.0, inc['incentive_resp'] + 0.01)
                    await conn.execute('UPDATE cust_incentives SET cashback_earned=$1, decay_loss_cnt=$2, incentive_resp=$3, updated_at=NOW() WHERE cust_id=$4',
                                       cashback, decay_loss, incentive_resp, cust_id)

                logger.info(f"Metrics updated for customer {cust_id}")
            except Exception as e:
                logger.error(f"Error updating metrics for customer {cust_id}: {e}")

    # Fetch history 
    async def get_historical_data(self):
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch('''
                SELECT h.history_id, h.customer_id, c.name_full, h.merchant_id, m.category, h.amount, h.time, h.is_rejected, h.b_old, h.b_new
                FROM history h
                LEFT JOIN customers c ON c.customer_id=h.customer_id
                LEFT JOIN merchants m ON m.merchant_id=h.merchant_id
                ORDER BY h.time DESC
                ''')
                return [dict(r) for r in rows]
            except Exception as e:
                logger.error(f"Failed fetching historical data: {e}")
                return []

    # Cleanup
    async def close(self):
        try:
            await self.pool.close()
        except Exception as e:
            logger.warning(f"Error closing DB pool: {e}")


    # Clear database (Be careful)
    async def clear_db(self):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Delete from dependent tables first
                    await conn.execute("DELETE history, cust_core, freqvol, cust_incentives RESTART IDENTITY CASCADE;")
                    # Then core tables
                    await conn.execute("DELETE customers, merchants RESTART IDENTITY CASCADE;")
                    logger.info("Database cleared successfully for next test.")
                except Exception as e:
                    logger.error(f"Error clearing database: {e}")
    
