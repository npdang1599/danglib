import sys
from datetime import datetime
from redis import StrictRedis

from danglib.utils import check_run_with_interactive
from danglib.pslab.logger_module import DataLogger
from danglib.pslab.process_realtime.process_stocks_data import ProcessData, ProcessStockData
from danglib.pslab.process_realtime.process_ps_data import ProcessPsData
from danglib.pslab.process_realtime.aggregator import Aggregator



# Current day for processing
current_day = datetime.now().strftime("%Y_%m_%d")

# Main execution block
if __name__ == "__main__" and not check_run_with_interactive():
    # Create argument parser
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time market data processing')
    parser.add_argument(
        '--type', 
        type=str,
        choices=['stock', 'ps'],
        required=True,
        help='Type of data to process: stock or ps (futures)'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        default='30S',
        help='Timeframe for data resampling (default: 30S)'
    )
    
    args = parser.parse_args()
    
    # Configure processor based on type argument
    if args.type == 'stock':
        processor = ProcessStockData
        output_key = 'pslab_realtime_stockdata2'
    else:  # ps (futures)
        processor = ProcessPsData
        output_key = 'pslab_realtime_psdata2'
    
    # Configure logger with processor-specific prefix
    logger = DataLogger(
        "/home/ubuntu/Dang/project_ps/logs", 
        file_prefix="processdata", 
        prefix=str.upper(args.type)
    )
    
    # Log startup information
    logger.log('INFO', f"Starting {args.type.upper()} data processor with timeframe {args.timeframe}")

    try:
        # Create and start aggregator
        aggregator = Aggregator(
            day=current_day,
            timeframe=args.timeframe,
            output_plasma_key=output_key,
            data_processor=processor,
            redis_cli=StrictRedis(decode_responses=True)
        )
        
        # Start real-time resampling process
        aggregator.start_realtime_resampling()
        
    except Exception as e:
        logger.log('CRITICAL', f"Fatal error in main process: {str(e)}", exc_info=True)
        sys.exit(1)