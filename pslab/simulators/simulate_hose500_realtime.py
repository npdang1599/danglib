from redis import StrictRedis
import pandas as pd
import time
from datetime import datetime, timedelta
import random
from functools import wraps
from danglib.pslab.resources import Adapters

class TimeSimulator:
    """Singleton class để quản lý thời gian giả lập"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TimeSimulator, cls).__new__(cls)
            cls._instance.current_time = None
            cls._instance.is_simulating = False
        return cls._instance
    
    def start_simulation(self, start_time: datetime):
        self.current_time = start_time
        self.start_real_time = time.time()
        self.start_sim_time = start_time.timestamp()
        self.is_simulating = True
    
    def get_current_time(self) -> datetime:
        if not self.is_simulating:
            return datetime.now()
            
        elapsed = time.time() - self.start_real_time
        current_ts = self.start_sim_time + elapsed
        return datetime.fromtimestamp(current_ts)
    
    def get_timestamp(self) -> float:
        if not self.is_simulating:
            return time.time()
            
        elapsed = time.time() - self.start_real_time
        return self.start_sim_time + elapsed

class RedisRandomReplay:
    def __init__(
        self,
        source_redis: StrictRedis,
        target_redis: StrictRedis,
        day: str,
        data_loader,
        tick_interval: float = 0.1
    ):
        self.source_redis = source_redis
        self.target_redis = target_redis
        self.day = day
        self.data_loader = data_loader
        self.tick_interval = tick_interval
        self.target_key = f"realtime_stock_data_{day}"
        self.time_simulator = TimeSimulator()
        
        # Load source data
        self.source_data = self._load_source_data()
        print(f"Loaded {len(self.source_data)} records from source")

    def _load_source_data(self) -> list:
        df = self.data_loader(self.source_redis, self.day, 0, -1)
        if df.empty:
            return []
            
        # Sort by timestamp to ensure correct order
        df = df.sort_values('timestamp')
        # Convert to list of records
        return df.to_dict('records')

    def _clear_target_redis(self):
        self.target_redis.delete(self.target_key)

    def start_replay(self, start_time: datetime = None, clear_existing: bool = True):
        """
        Start replaying data with simulated time
        
        Args:
            start_time: Start time for simulation (default: 9:15 AM)
            clear_existing: Whether to clear existing data
        """
        if clear_existing:
            self._clear_target_redis()

        # Set up time simulation
        if start_time is None:
            start_time = datetime.now().replace(hour=9, minute=15, second=0)
        self.time_simulator.start_simulation(start_time)
        
        current_pos = 0
        try:
            while current_pos < len(self.source_data):
                current_time = self.time_simulator.get_current_time()
                
                # Random số lượng records
                batch_size = random.randint(1, 10)
                end_pos = min(current_pos + batch_size, len(self.source_data))
                
                # Lấy batch data và update timestamps
                batch_data = self.source_data[current_pos:end_pos]
                current_timestamp = int(self.time_simulator.get_timestamp() * 1000)
                
                # Update timestamps trong data
                for record in batch_data:
                    record['timestamp'] = current_timestamp
                
                # Append to target Redis
                pipe = self.target_redis.pipeline()
                for record in batch_data:
                    pipe.rpush(self.target_key, str(record))
                pipe.execute()
                
                current_pos = end_pos
                print(f"[{current_time.strftime('%H:%M:%S')}] "
                      f"Appended {len(batch_data)} records. Total: {current_pos}")
                
                time.sleep(self.tick_interval)

        except KeyboardInterrupt:
            print("\nReplay stopped by user")
        except Exception as e:
            print(f"Error during replay: {str(e)}")
            raise

def patch_time_functions():
    """Patch time.time() và datetime.now() để sử dụng simulated time"""
    time_simulator = TimeSimulator()
    
    # Patch time.time()
    original_time = time.time
    def patched_time():
        if time_simulator.is_simulating:
            return time_simulator.get_timestamp()
        return original_time()
    time.time = patched_time
    
    # Patch datetime.now()
    original_datetime_now = datetime.now
    def patched_datetime_now():
        if time_simulator.is_simulating:
            return time_simulator.get_current_time()
        return original_datetime_now()
    datetime.now = patched_datetime_now

def test_replay():
    # Patch time functions
    patch_time_functions()
    
    # Set up Redis connections
    source_redis = StrictRedis(host='localhost', port=6379, db=0)
    target_redis = StrictRedis(host='localhost', port=6379, db=1)
    
    replayer = RedisRandomReplay(
        source_redis=source_redis,
        target_redis=target_redis,
        day="2025_02_14",
        data_loader=Adapters.RedisAdapters.load_realtime_stock_data_from_redis,
        tick_interval=0.1
    )
    
    # Start replay từ 9:15 AM
    start_time = datetime.now().replace(hour=9, minute=15, second=0)
    replayer.start_replay(start_time=start_time)

from datetime import datetime, timedelta

class SimulatedTime:
    def __init__(self, initial_time: datetime = None):
        self.start_real_time = datetime.now()
        self.start_sim_time = initial_time or datetime.now().replace(hour=9, minute=15, second=0)
        
    def now(self) -> datetime:
        """Trả về thời gian giả lập hiện tại"""
        elapsed = datetime.now() - self.start_real_time
        return self.start_sim_time + elapsed
        
    def time(self) -> float:
        """Trả về timestamp giả lập hiện tại"""
        return self.now().timestamp()
    
sim_time = SimulatedTime()
pd.to_datetime(sim_time.time(), unit='s')


if __name__ == "__main__":
    test_replay()