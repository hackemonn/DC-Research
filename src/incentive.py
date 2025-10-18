from src.data_layer.processor import DataProcessor
import logging 
import os 
import sqlite3
from datetime import datetime 

BONUS_RATE = 0.03
DECAY_RATE = 0.02
TARGET_VELOCITY = 0.5
PERIOD_DAYS = 7


logger = logging.getLogger(__name__)

#Encourage spending before monetary tools
#before monetary tools even come into play

#If you spend, your money automatically becomes inflation
#inflation-protected for this year

class Incentive:
    def __init__(self, customer_id, db_file=os.path.join("data", "pc.db")):
        # ensure the data directory exists
        try:   
            db_dir = os.path.dirname(db_file)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            self.customer_id = customer_id
            self.db_conn = sqlite3.connect(db_file, check_same_thread=False)
        
            self._init_db()
        except sqlite3.Error as e:
            logger.error(f"DB Error found: {e}")
    
    def _init_db(self):
        cursor = self.db_conn.cursor()
        '''
        Core table
        Create frequency & volume table
        spending behavior 
        saving patterns
        behavioral anomalies
        incentives

        (future) 
        network feature
        temporal patterns 
        '''
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS cust_core (
            cust_id INT PRIMARY KEY,
            avg_daily_bal DECIMAL(15,2),
            max_bal DECIMAL(15,2),
            min_bal DECIMAL(15,2),
            bal_std DECIMAL(12,2),
            inactive_days INT
            );

            CREATE TABLE IF NOT EXISTS freqvol (
            cust_id INT PRIMARY KEY,
            num_tr_day INT,
            num_tr_week INT,
            avg_tr_val DECIMAL(12,2),
            total_tr_val DECIMAL(15,2),
            tr_std DECIMAL(12,2),
            velocity DECIMAL(12,2),
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );
                             
            CREATE TABLE IF NOT EXISTS cust_spending (
            cust_id INT,
            cat_dist JSON,                 -- category distribution
            merchant_div INT,
            recurring_pay_cnt INT,
            peak_tr_hour INT,
            dow_pref INT,                  -- day-of-week preference
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );
                             
            CREATE TABLE IF NOT EXISTS cust_risk (
            cust_id INT,
            late_failed_tr INT,
            suspicious_flag BOOLEAN,
            high_risk_tr_ratio DECIMAL(5,4),
            rapid_tr_events INT,
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );

            CREATE TABLE IF NOT EXISTS cust_incentives (
            cust_id INT,
            cashback_earned DECIMAL(12,2),
            decay_loss_cnt INT,
            incentive_resp DECIMAL(5,4),
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );

            CREATE TABLE IF NOT EXISTS cust_network (
            cust_id INT,
            peer_tr_cnt INT,
            merchant_interact_cnt INT,
            rel_centrality DECIMAL(10,4),
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );

            CREATE TABLE IF NOT EXISTS cust_temporal (
            cust_id INT,
            avg_time_btwn_tr DECIMAL(10,2),
            tr_burst DECIMAL(10,2),
            season_score DECIMAL(5,4),
            FOREIGN KEY (cust_id) REFERENCES cust_core(cust_id)
            );

        ''')


        
