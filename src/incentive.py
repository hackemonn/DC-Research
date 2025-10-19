from src.data_layer.processor import DataProcessor
import logging 
import uuid
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
    def __init__(self, customer_id: str, dp: DataProcessor):
        self.customer_id = customer_id
        self.dp = dp
    
    #apply cashback if consumption happened
    async def apply_cashback(self, amount: float):
        if amount <= 0: 
            return 0

        
        await cashback = amount * BONUS_RATE

        await tr_id = str(uuid.uuid4())
        
        await now = datetime.utcnow().isoformat()
        
        await self.dp.add_h_data({
            'history_id': tr_id,
            'customer_id': self.customer_id,
            'merchant_id': None,
            'amount': cashback,
            'time': now,
            'isRejected': False,
            'b_old': self.dp.balance_query(self.customer_id),
            'b_new': self.dp.balance_query(self.customer_id) + cashback
        })
        # Update customer balance
        await self.dp.make_transaction(None, self.customer_id, cashback, tr_id, now)  # may need a "source" merchant for bookkeeping
        return cashback


    