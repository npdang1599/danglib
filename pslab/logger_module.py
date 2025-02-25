import logging
import os
from datetime import datetime, timedelta
import glob
from logging.handlers import RotatingFileHandler

class DataLogger:
    def __init__(self, log_dir="logs"):
        self.log_dir = log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.today = datetime.now().strftime('%Y_%m_%d')
        self.setup_logger()
        self.clean_old_logs()
        
    def setup_logger(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('DataAggregator')
        self.logger.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler - daily rotating
        file_handler = RotatingFileHandler(
            filename=os.path.join(self.log_dir, f'aggregator_{self.today}.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # # Console handler
        # console_handler = logging.StreamHandler()
        # console_handler.setLevel(logging.INFO)
        # console_handler.setFormatter(formatter)
        
        # Clear existing handlers
        self.logger.handlers = []
        
        # Add handlers
        self.logger.addHandler(file_handler)
        # self.logger.addHandler(console_handler)
    
    def clean_old_logs(self):
        """Delete logs older than 10 days"""
        cutoff_date = datetime.now() - timedelta(days=10)
        for log_file in glob.glob(os.path.join(self.log_dir, 'aggregator_*.log*')):
            try:
                file_date_str = log_file.split('aggregator_')[-1].split('.')[0]
                file_date = datetime.strptime(file_date_str, '%Y_%m_%d')
                if file_date < cutoff_date:
                    os.remove(log_file)
                    self.logger.info(f"Deleted old log file: {log_file}")
            except (ValueError, IndexError) as e:
                self.logger.warning(f"Could not parse date from filename: {log_file}, error: {str(e)}")
    
    def check_rotate(self):
        """Check if we need to rotate to a new day's log file"""
        current_day = datetime.now().strftime('%Y_%m_%d')
        if current_day != self.today:
            self.today = current_day
            self.setup_logger()
            self.clean_old_logs()
    
    def log(self, level, message):
        """Log a message with the specified level"""
        self.check_rotate()
        if level == 'DEBUG':
            self.logger.debug(message)
        elif level == 'INFO':
            self.logger.info(message)
        elif level == 'WARNING':
            self.logger.warning(message)
        elif level == 'ERROR':
            self.logger.error(message)
        elif level == 'CRITICAL':
            self.logger.critical(message)