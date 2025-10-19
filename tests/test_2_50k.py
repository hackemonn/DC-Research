import asyncio
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
import logging
import uuid

from src.data_layer.processor import DataProcessor

logger = logging.getLogger(__name__)

NUM_CUSTOMERS = 5000
NUM_MERCHANTS = 500
NUM_TRANSACTIONS = 50000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 9, 30)

class Test2:

    def __init__(self, processor: DataProcessor):
        self.dp = processor
        

    # ------------------- Helpers -------------------
    def weighted_choice(self, choices, weights, size=1):
        return np.random.choice(choices, size=size, p=np.array(weights)/np.sum(weights))

    def random_dates(self, start, end, n):
        delta = end - start
        return [start + timedelta(seconds=np.random.randint(0, int(delta.total_seconds()))) for _ in range(n)]

    # ------------------- Generate Customers -------------------
    def generate_customers(self, n):
        ages = np.random.randint(18, 70, n)
        genders = np.random.choice(['M', 'F'], n)
        salaries = np.random.randint(20000, 200000, n)
        industries = np.random.choice(['Tech','Finance','Healthcare','Retail','Education'], n)
        behaviors = np.random.choice(['Aggressive','Moderate','Conservative'], n)

        customers = []
        for i in range(n):
            customers.append({
                'id': f'c{i+1}',
                'age': int(ages[i]),
                'name_full': f'c{i+1}',
                'profession': '',
                'salary': float(salaries[i]),
                'level': 1,
                'acc_balance': float(np.random.randint(1000, 20000)),
                'description': '',
                'industry': str(industries[i]),
                'behavior': str(behaviors[i]),
            })
        return customers
    
    # Potentially useful tables to add: behavior, gender
    # ------------------- Generate Merchants -------------------
    def generate_merchants(self, n):
        categories = ['Grocery','Tech','Entertainment','Healthcare','Retail']
        merchants = []
        for i in range(n):
            merchants.append({
                'merchant_id': f'm{i+1}',
                'category': np.random.choice(categories),
                'description': '',
                'acc_balance': float(np.random.randint(5000, 50000)),  
            })
        return merchants

    # ------------------- Generate Transactions -------------------


    def generate_transactions(self, customers, merchants, num_tx):
        transactions = []
        
        # Build merchant dictionary by category
        merchant_dict = {}
        for m in merchants:
            if m['category'] not in merchant_dict:
                merchant_dict[m['category']] = []
            merchant_dict[m['category']].append(m['merchant_id'])
        
        #t = True
        
        for _ in range(num_tx):
            # Pick a random customer from the list
            cust = random.choice(customers)  # cust is a dict
            
            # Select merchant category based on heuristics
            if cust['salary'] > 120000:
                weights = {'Tech':0.4,'Entertainment':0.3,'Grocery':0.1,'Healthcare':0.1,'Retail':0.1}
            elif cust['age'] < 30:
                weights = {'Tech':0.3,'Entertainment':0.4,'Grocery':0.1,'Healthcare':0.1,'Retail':0.1}
            else:
                weights = {'Tech':0.2,'Entertainment':0.2,'Grocery':0.3,'Healthcare':0.2,'Retail':0.1}

            category = self.weighted_choice(list(weights.keys()), list(weights.values()))[0]
            merchant_id = np.random.choice(merchant_dict[category])

            # Transaction amount
            base = next(m['acc_balance'] for m in merchants if m['merchant_id']==merchant_id)/100
            multiplier = {'Aggressive': 1.5, 'Moderate': 1.0, 'Conservative': 0.7}[cust['behavior']] #9 means behavior
            amount = float(np.round(base * multiplier * np.random.uniform(0.8,1.2),2))

            date = self.random_dates(START_DATE, END_DATE, 1)[0]

            transactions.append({
                'customer_id': cust['id'],
                'merchant_id': merchant_id,
                'amount': amount,
                'date': date
            })
        
        return transactions

    # ------------------- Run Test2 -------------------
    async def run(self):
        logger.info("Generating customers and merchants...")
        customers = self.generate_customers(NUM_CUSTOMERS)
        merchants = self.generate_merchants(NUM_MERCHANTS)

        logger.info("Generating transactions...")
        transactions = self.generate_transactions(customers, merchants, NUM_TRANSACTIONS)
        await self.dp.init()
        # Save all to PostgreSQL via DataProcessor
        logger.info("Saving customers to DB...")
        for c in customers:
            await self.dp.save_customer(c)
        logger.info("Saving merchants to DB...")
        for m in merchants:
            await self.dp.save_merchant(m)
        logger.info("Running transactions...")
        for tx in transactions:
            await self.dp.make_transaction(tx['customer_id'], tx['merchant_id'], tx['amount'])

        logger.info("Test2 completed: 50k transactions simulated!")




