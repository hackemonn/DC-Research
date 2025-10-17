import numpy as np
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

NUM_CUSTOMERS = 5000
NUM_MERCHANTS = 500
NUM_TRANSACTIONS = 50000  # total transactions to simulate
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 9, 30)

class Test2:
    def weighted_choice(self, choices, weights, size=1):
        return np.random.choice(choices, size=size, p=np.array(weights)/np.sum(weights))

    def random_dates(self, start, end, n):
        delta = end - start
        return [start + timedelta(seconds=np.random.randint(0, int(delta.total_seconds()))) for _ in range(n)]


    def generate_customers(n):
        
        #ages generated with no bias

        ages = np.random.randint(18, 70, n)
        
        #roughly 50/50
        genders = np.random.choice(['M', 'F'], n)
        
        #not represenatative, but for our purposes it is sufficient

        salary = np.random.randint(20000, 200000, n)
        
        #the industries of merchants

        industries = np.random.choice(['Tech','Finance','Healthcare','Retail','Education'], n)
        
        #spending behavior of the customer
        behavior = np.random.choice(['Aggressive','Moderate','Conservative'], n)
        
        df = pd.DataFrame({
            'customer_id': range(1, n+1),
            'age': ages,
            'gender': genders,
            'salary': salary,
            'industry': industries,
            'behavior': behavior
        })
        
        # Risk score based on income & behavior
        df['risk_score'] = df['income'] / 200000 + df['behavior'].map({'Aggressive': 0.3, 'Moderate': 0.1, 'Conservative': 0.0})
        
        # Salary segment using standard deviation
        mean_income = df['income'].mean()
        std_income = df['income'].std()
        
        def salary_segment(income):
            if income < mean_income - std_income:
                return 1  # basic salary
            elif income > mean_income + std_income:
                return 3  # highest salary
            else:
                return 2  # higher salary
        
        df['salary_segment'] = df['income'].apply(salary_segment)
        
        return df



    def generate_merchants(m):
        categories = np.random.choice(['Grocery','Tech','Entertainment','Healthcare','Retail'], m)
        avg_tx = np.random.randint(10, 500, m)
        
        df = pd.DataFrame({
            'merchant_id': range(1, m+1),
            'category': categories,
            'avg_transaction': avg_tx
        })
        
        return df


    def generate_transactions(self, customers, merchants, num_tx):
        transactions = []
        
        # Map merchants by category for correlation
        merchant_dict = merchants.groupby('category')['merchant_id'].apply(list).to_dict()
        
        for _ in range(num_tx):
            cust = customers.sample(1).iloc[0]
            
            # Choose merchant category weighted by income & behavior
            if cust['income'] > 120000:
                category_weights = {'Tech': 0.4, 'Entertainment': 0.3, 'Grocery': 0.1, 'Healthcare': 0.1, 'Retail': 0.1}
            elif cust['age'] < 30:
                category_weights = {'Tech': 0.3, 'Entertainment': 0.4, 'Grocery': 0.1, 'Healthcare': 0.1, 'Retail': 0.1}
            else:
                category_weights = {'Tech': 0.2, 'Entertainment': 0.2, 'Grocery': 0.3, 'Healthcare': 0.2, 'Retail': 0.1}
            
            category = self.weighted_choice(list(category_weights.keys()), list(category_weights.values()))[0]
            merchant_id = np.random.choice(merchant_dict[category])
            
            # Transaction amount influenced by income & behavior
            base = merchants.loc[merchants['merchant_id'] == merchant_id, 'avg_transaction'].values[0]
            multiplier = {'Aggressive': 1.5, 'Moderate': 1.0, 'Conservative': 0.7}[cust['behavior']]
            amount = np.round(base * multiplier * np.random.uniform(0.8, 1.2), 2)
            
            # Random date
            date = self.random_dates(START_DATE, END_DATE, 1)[0]
            
            transactions.append({
                'customer_id': cust['customer_id'],
                'merchant_id': merchant_id,
                'amount': amount,
                'date': date
            })
        
        return pd.DataFrame(transactions)


    customers_df = generate_customers(NUM_CUSTOMERS)
    merchants_df = generate_merchants(NUM_MERCHANTS)
    transactions_df = generate_transactions(customers_df, merchants_df, NUM_TRANSACTIONS)

    conn = sqlite3.connect('synthetic_data.db')
    customers_df.to_sql('customers', conn, if_exists='replace', index=False)
    merchants_df.to_sql('merchants', conn, if_exists='replace', index=False)
    transactions_df.to_sql('transactions', conn, if_exists='replace', index=False)
    conn.close()

    logger.info(" Synthetic dataset generated and saved to 'synthetic_data.db'")
