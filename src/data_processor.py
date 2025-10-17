import sqlite3
import os
import logging
from datetime import datetime
import uuid


logger = logging.getLogger(__name__)

class DataProcessor:  

    def __init__(self, db_file=os.path.join("data", "pc.db")): #pc = programmable currency
        # ensure the data directory exists
        db_dir = os.path.dirname(db_file)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.db_conn = sqlite3.connect(db_file, check_same_thread=False)
        self._init_db()
    
    # Set up database tables
    def _init_db(self):
        cursor = self.db_conn.cursor()
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                age INTEGER,
                name_full TEXT,
                profession TEXT,
                salary REAL,
                level INTEGER, -- level 1: base salary, level 2: higher salary, level 3: highest salary
                acc_balance REAL,
                description TEXT
            );
            CREATE TABLE IF NOT EXISTS relationship (
                customer_id TEXT,
                merchant_id TEXT,
                PRIMARY KEY (customer_id, merchant_id)
            );
            CREATE TABLE IF NOT EXISTS history (
                history_id TEXT PRIMARY KEY,
                customer_id TEXT,
                merchant_id TEXT,
                amount INTEGER,
                time TEXT,
                isRejected INTEGER, -- either 0 or 1
                b_old INTEGER, 
                b_new INTEGER
            );
            CREATE TABLE IF NOT EXISTS merchants (
                merchant_id TEXT PRIMARY KEY,
                category TEXT,
                description TEXT,
                acc_balance INTEGER
            );
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                customer_id TEXT,
                merchant_id TEXT,
                amount INTEGER,
                time TEXT,
                description TEXT
            );
        ''')
        #Transactions include all successful transactions, and history contains all data

        #For the sake of simplicity whenever someone gets a salary every t period, merchant no. 1e9+7
        #is going to pay the customer the level x salary
        self.db_conn.commit()


    def save_customer(self, customer):
        cursor = self.db_conn.cursor()
        # accept either 'customer_id' or 'id' as the identifier key
        customer_id = customer.get('id')
        if not customer_id:
            raise ValueError('customer must include customer_id or id')

        cursor.execute('''
            INSERT OR REPLACE INTO customers (customer_id, age, name_full, profession, salary, level, acc_balance, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            customer_id,
            customer.get('age'),
            customer.get('name_full') or customer.get('name'),
            customer.get('profession'),
            customer.get('salary') or 0,
            customer.get('level'),
            customer.get('acc_balance') or 0,
            customer.get('description')
        ))       
    
    def save_merchant(self, merchant):
        cursor = self.db_conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO merchants (merchant_id,category,description,acc_balance)
            VALUES (?, ?, ?, ?)
        ''', (
            merchant.get('merchant_id') or merchant.get('id'), # accept 'id' as alias
            merchant.get('category'),
            merchant.get('description'),
            merchant.get('acc_balance') or 0,
        ))
        self.db_conn.commit()
    

    
    def get_historical_data(self):
        cursor = self.db_conn.cursor()
        # Return history rows joined with customer and merchant basic info
        cursor.execute('''
                       SELECT h.history_id, h.customer_id, c.name_full, h.merchant_id, m.category, h.amount, h.time, h.isRejected, h.b_old, h.b_new
                       FROM history h
                       LEFT JOIN customers c ON c.customer_id = h.customer_id
                       LEFT JOIN merchants m ON m.merchant_id = h.merchant_id
                       ORDER BY h.time DESC
                       ''')
        rows = cursor.fetchall()
        data = []
        for (history_id, customer_id, customer_name, merchant_id, merchant_category, amount, time, isRejected, b_old, b_new) in rows:
            data.append({
                'history_id': history_id,
                'customer_id': customer_id,
                'customer_name': customer_name,
                'merchant_id': merchant_id,
                'merchant_category': merchant_category,
                'amount': amount,
                'time': time,
                'isRejected': bool(isRejected) if isRejected is not None else None,
                'b_old': b_old,
                'b_new': b_new
            })
        return data
    
    def add_h_data(self, values):
        cursor = self.db_conn.cursor()
        # values can be a dict with history fields or a tuple/list in column order
        if isinstance(values, dict):
            history_id = values.get('history_id') or str(uuid.uuid4())
            customer_id = values.get('customer_id')
            merchant_id = values.get('merchant_id')
            amount = values.get('amount') or 0
            time = values.get('time') or datetime.utcnow().isoformat()
            isRejected = 1 if values.get('isRejected') else 0
            b_old = values.get('b_old')
            b_new = values.get('b_new')
        else:
            # expect tuple: (history_id, customer_id, merchant_id, amount, time, isRejected, b_old, b_new)
            (history_id, customer_id, merchant_id, amount, time, isRejected, b_old, b_new) = values

        cursor.execute('''
            INSERT OR REPLACE INTO history (history_id, customer_id, merchant_id, amount, time, isRejected, b_old, b_new)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            history_id,
            customer_id,
            merchant_id,
            amount,
            time,
            isRejected,
            b_old,
            b_new,
        ))
        self.db_conn.commit()
    
    def enoughFunds(self, customer_id, amount : float = 0.0):
        cursor = self.db_conn.cursor()
        cursor.execute('''
        SELECT customers.acc_balance
        FROM customers
        WHERE customers.customer_id = ?''', (customer_id,)
        )
        
        result = cursor.fetchone()

        value = result[0]

        if(value >= amount):
            return True
        else:
            return False

    def balance_query(self, customer_id): 
        cursor = self.db_conn.cursor()
        cursor.execute('''
        SELECT acc_balance
        FROM customers
        WHERE customer_id = ?
        ''', (customer_id,))
        value = cursor.fetchone()
        return value[0]


    def make_transaction(self, customer_id, merchant_id, amount, tr_id, time):
        if(self.enoughFunds(customer_id, amount)):
            cursor = self.db_conn.cursor()
            cursor.execute('''
            UPDATE customers
            SET acc_balance = acc_balance - ?
            WHERE customer_id = ?;
            ''', (amount, customer_id))
            cursor.execute('''UPDATE merchants
            SET acc_balance = acc_balance + ? 
            WHERE merchant_id = ?
                           ''', (amount, merchant_id))
            b = self.balance_query(customer_id)

            self.add_h_data((tr_id, customer_id, merchant_id, amount, time, 0, b + amount, b))
            
        else:
            return False

    


        
