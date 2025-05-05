import pandas as pd
from datetime import date, datetime, timedelta


def today():
    """Returns today's date in YYYY_MM_DD format
    
    Returns:
        str: Today's date in YYYY_MM_DD format
    """
    return datetime.now().strftime('%Y_%m_%d')


def is_weekend(day):
    """Check if a day is weekend
    
    Args:
        day (str): Date in YYYY_MM_DD format
        
    Returns:
        bool: True if weekend, False otherwise
    """
    date = datetime.strptime(day, '%Y_%m_%d')
    return (date.weekday() == 5) | (date.weekday() == 6)


def day_to_timestamp(day, is_end_day=False, unit='ns'):
    """Convert day string to timestamp
    
    Args:
        day (str): Date in YYYY_MM_DD format
        is_end_day (bool): If True, returns timestamp for end of day
        unit (str): Unit for timestamp ('ns', 's', 'ms')
        
    Returns:
        int: Timestamp in specified unit
    """
    stamp = int(pd.to_datetime(day, format='%Y_%m_%d').timestamp())
    if is_end_day:
        stamp += 86400  # Add seconds in a day
        
    if unit == 'ns':
        return stamp * 1000000000
    elif unit == 'ms':
        return stamp * 1000
    return stamp


def totime(stamp, unit='s'):
    """Convert timestamp to datetime
    
    Args:
        stamp: Timestamp value
        unit (str): Unit of timestamp ('s', 'ms', 'ns')
        
    Returns:
        datetime: Converted datetime object
    """
    return pd.to_datetime(stamp, unit=unit, utc=True).tz_convert('Asia/Ho_Chi_Minh').strftime('%Y_%m_%d %H:%M:%S')


def get_maturity_dates(year):
    """Get third Thursday and Friday dates for each month in a year
    
    Args:
        year (int): Year to calculate dates for
        
    Returns:
        tuple: (dict of third Thursdays, dict of third Fridays)
    """
    dic_third_thursdays = {}
    dic_third_fridays = {}
    
    # Iterate over the months of the year
    for month in range(1, 13):
        # Calculate the first day of the month
        d = date(year, month, 1)

        # Use the `weekday()` method to determine the day of the week (0 = Monday, 6 = Sunday)
        if d.weekday() == 3:
            # If the first day of the month is a Thursday, the third Thursday is in the same week
            third_thursday = d + timedelta(days=14)
        else:
            # If the first day of the month is not a Thursday, calculate the next Thursday
            first_thursday = d + timedelta(days=(3 - d.weekday()) % 7)
            third_thursday = first_thursday + timedelta(days=14)
            
        third_friday = third_thursday + timedelta(days=1)
        third_thursday = third_thursday.strftime('%Y_%m_%d')
        third_friday = third_friday.strftime("%Y_%m_%d")
        dic_third_thursdays[month] = third_thursday
        dic_third_fridays[month] = third_friday
    
    return dic_third_thursdays, dic_third_fridays


def get_previous_maturity_date(day):
    """Get previous maturity date
    
    Args:
        day (str): Date in YYYY_MM_DD format
        
    Returns:
        str: Previous maturity date
    """
    month = int(day[5:7])
    year = int(day[0:4])
    dic_third_thursdays = get_maturity_dates(year)[0]
    last_december_maturity_date = get_maturity_dates(year-1)[0][12]
    
    if day > dic_third_thursdays[month]:
        last_maturity_date = dic_third_thursdays[month]
    elif day <= dic_third_thursdays[1]:
        last_maturity_date = last_december_maturity_date
    else:
        last_maturity_date = dic_third_thursdays[month-1]
    
    return last_maturity_date


def get_ps_ticker(day):
    """Get ticker for given day
    
    Args:
        day (str): Date in YYYY_MM_DD format
        
    Returns:
        dict: Dictionary of tickers
    """
    last_maturity_day = get_previous_maturity_date(day)
    month = int(last_maturity_day[5:7])
    year = int(last_maturity_day[0:4])
    
    if month == 12:
        F1_month = 1
        F1_year = year+1
    else:
        F1_month = month+1
        F1_year = year
        
    F2_month = F1_month+1
    
    if F2_month % 3 == 0:
        F3_month = F2_month+3
        F4_month = F2_month+6
    else:
        F3_month = F2_month//3 * 3 + 3
        F4_month = F3_month+3
        
    if F2_month > 12:
        F2_month = F2_month - 12
        F2_year = F1_year+1
    else:
        F2_year = F1_year
        
    if F3_month > 12:
        F3_month = F3_month - 12
        F3_year = F1_year+1
    else:
        F3_year = F1_year
        
    if F4_month > 12:
        F4_month = F4_month - 12
        F4_year = F1_year+1
    else:
        F4_year = F1_year

    F1_month = str(F1_month).zfill(2)
    F2_month = str(F2_month).zfill(2)
    F3_month = str(F3_month).zfill(2)
    F4_month = str(F4_month).zfill(2)

    F1_year = str(F1_year)[-2:]
    F2_year = str(F2_year)[-2:]
    F3_year = str(F3_year)[-2:]
    F4_year = str(F4_year)[-2:]

    return {
        'f1': f'VN30F{F1_year}{F1_month}',
        'f2': f'VN30F{F2_year}{F2_month}',
        'f3': f'VN30F{F3_year}{F3_month}',
        'f4': f'VN30F{F4_year}{F4_month}'
    }


def generate_candle_times(timeframe: str, day=None, start_time='09:15:00', end_time='14:45:00', to_timestamp=False, unit='ns'):
    """
    Generate time series for candles based on timeframe
    
    Args:
        timeframe (str): Timeframe ('1s', '5s', '15s', '30s', '1min', '5min', '15min', '30min', '1h', '4h', '1d')
        day (str): Day in YYYY_MM_DD format
        start_time (str): Start time (HH:MM:SS)
        end_time (str): End time (HH:MM:SS)
        to_timestamp (bool): If True, convert to timestamp
        unit (str): Unit for timestamp if to_timestamp is True ('ns', 's', 'ms')
        
    Returns:
        pd.Series: Series of candle times
    """
    timeframe = timeframe.replace('S', 's')
    start_time_full = day.replace('_', '-') + ' ' + start_time
    lunch_time = day.replace('_', '-') + ' ' + '11:30:00'
    
    afternoon_time = day.replace('_', '-') + ' ' + '13:00:00'
    atc_time = day.replace('_', '-') + ' ' + '14:30:00'
    end_time_full = day.replace('_', '-') + ' ' + end_time
    
    start_dt = pd.to_datetime(start_time_full)
    end_dt = pd.to_datetime(end_time_full)
    
    candle_times = pd.Series(pd.date_range(start=start_dt, end=end_dt, freq=timeframe))
    
    # Morning session: 9:15:00 - 11:30:00
    filter_morning = (candle_times < lunch_time)
    
    # Afternoon session: 13:00:00 - 14:30:00
    filter_afternoon = (candle_times >= afternoon_time) & (candle_times < atc_time)

    # ATC
    filter_atc = (candle_times == end_dt)

    candle_times = candle_times[filter_morning|filter_afternoon|filter_atc]
    
    if to_timestamp:
        candle_times = candle_times.astype(int)

        if unit=='s':
            candle_times = candle_times // 1e9
        elif unit=='ms':
            candle_times = candle_times // 1e6

        candle_times = candle_times.astype(int)

    return candle_times.reset_index(drop=True)


def convert_timeframe_to_rolling(timeframe):
    """
    Convert timeframe string to number of 30-second periods.
    
    Args:
        timeframe (str): Timeframe string with format like '1Min', '5S', '1H', '1D'
        
    Returns:
        int: Number of 30-second periods
    """
    number = int(''.join(filter(str.isdigit, timeframe)))
    unit = ''.join(filter(str.isalpha, timeframe))
    
    seconds = 0
    if unit == 'S':
        seconds = number
    elif unit == 'Min':
        seconds = number * 60
    elif unit == 'H':
        seconds = number * 3600
    elif unit == 'D':
        seconds = number * 86400
    
    return seconds // 30
