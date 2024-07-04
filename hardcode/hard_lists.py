from datetime import date, timedelta


def get_vn30_lst_hardcode(day):
    if day >= "2023_02_06":
        VN30_LIST = ['ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG',
                    'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'POW', 'SAB', 'SSI', 'STB', 
                    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
    elif day >= "2022_08_01":
        VN30_LIST = ['ACB', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'KDH',
                    'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'VIB', 'POW', 'SAB', 'SSI',
                    'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
    elif  '2021_08_01' <= day < "2022_08_01":
        VN30_LIST = ['ACB', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'KDH',
                    'MBB', 'MSN', 'MWG', 'NVL', 'PDR', 'PLX', 'PNJ', 'POW', 'SAB', 'SSI',
                    'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
    else: # truoc thang 8 thi dung list nay
        VN30_LIST = ['BID', 'BVH', 'CTG', 'FPT', 'GAS', 'HDB', 'HPG', 'KDH', 'MBB', 'MSN',
                    'MWG', 'NVL', 'PDR', 'PLX', 'PNJ', 'POW', 'REE', 'SBT', 'SSI', 'STB',
                    'TCB', 'TCH', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
    return VN30_LIST


holidays_2023_dic = {
    '2023_01_02': 'nghi bu tet duong',
    '2023_01_20': 'tet am lich',
    '2023_01_23': 'tet am lich',
    '2023_01_24': 'tet am lich',
    '2023_01_25': 'tet am lich',
    '2023_01_26': 'tet am lich',
    '2023_04_29': 'gio to Hung vuong',
    '2023_04_30': '30 thang 4',
    '2023_05_01': 'Quoc te lao dong',
    '2023_05_02': 'nghi bu',
    '2023_05_03': 'nghi bu',
    '2023_09_02': 'Le Quoc Khanh',
    '2023_09_04': 'nghi bu quoc khanh',
    '2024_01_01': 'tet duong lich'
}


def get_trading_days_2023():
    year = 2023
    num_weekdays = 0
    day_lst = []
    d = date(year, 1, 1)
    while d.year == year:
        
        if d.weekday() < 5:
            num_weekdays += 1
            day_str = d.strftime(format='%Y_%m_%d')
            if day_str not in holidays_2023_dic:
                day_lst.append(day_str)

        d += timedelta(days=1)

    return day_lst


def get_maturity_dates(year):
    # year = 2023
    dic_third_thursdays = {}
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

        dic_third_thursdays[month] = third_thursday.strftime(format='%Y_%m_%d')
    return dic_third_thursdays


if __name__ == "__main__":
    trading_days_2023 = get_trading_days_2023()
    dic_maturity_dates = get_maturity_dates(2023)