"""import library"""

from celery import Celery
from redis import Redis
from danglib.pylabview.funcs import Simulator, pd, Adapters, Conds
import logging

class CELERY_RESOURCES:
    HOST = 'localhost'
    CELERY_INPUT_REDIS = 8

def clean_redis():
    """Xóa tất cả dữ liệu trong Redis để chuẩn bị tài nguyên cho các tác vụ Celery mới.

    Hàm này kết nối đến Redis tại các cơ sở dữ liệu `CELERY_INPUT_REDIS` và `CELERY_INPUT_REDIS + 1`,
    sau đó xóa tất cả các khóa (keys) hiện có trong cả hai cơ sở dữ liệu để đảm bảo rằng không còn dữ liệu cũ nào còn lại.

    Returns:
        int: Số lượng khóa đã xóa từ Redis.
    """
    r_input = Redis(
        CELERY_RESOURCES.HOST,
        db=CELERY_RESOURCES.CELERY_INPUT_REDIS,
        decode_responses=True)
    r_output = Redis(
        CELERY_RESOURCES.HOST,
        db=CELERY_RESOURCES.CELERY_INPUT_REDIS + 1,
        decode_responses=True)
    count = 0
    for key in r_output.keys():
        count += r_output.delete(key)
    for key in r_input.keys():
        count += r_input.delete(key)
    return count

def app_factory(host):
    broker_url = f'redis://{host}:6379/' \
                    f'{CELERY_RESOURCES.CELERY_INPUT_REDIS}'
    backend_url = f'redis://{host}:6379/' \
                    f'{CELERY_RESOURCES.CELERY_INPUT_REDIS + 1}'
    remote_app = Celery(
        'dc_slavemaster',
        broker=broker_url, 
        backend=backend_url)
    remote_app.conf.task_serializer = 'pickle'
    remote_app.conf.result_serializer = 'pickle'
    remote_app.conf.accept_content = [
        'application/json',
        'application/x-python-serialize']
    return remote_app

app = app_factory(CELERY_RESOURCES.HOST)

class TaskName:
    SCAN_STOCK = 'scan_one_stock'
    SCAN_STOCK_V2 = 'scan_one_stock_v2'
    SCAN_STOCK_V3 = 'scan_one_stock_v3'
    SCAN_STOCK_V4 = 'scan_one_stock_v4'
    COMPUTE_ONE_STOCK_SIGNALS = 'compute_one_stock_signals'
    
@app.task(name=TaskName.SCAN_STOCK)
def scan_one_stock(
        params, 
        stock,
        name="", 
        trade_direction='Long', 
        use_shift=False,
        n_shift=15, 
        holding_periods=15
    ):
    """ Backtest 1 cp, không có điều kiện loss threshold và take-profit threshold
    - Tải dữ liệu cổ phiếu từ Plasma.
    - Khởi tạo đối tượng Simulator (bt) với hàm Conds.compute_any_conds, và hàm "run"
    """
    def test():
        params = (
            {
                'price_change':{
                    'lower_thres':5,
                    'use_flag': True
                }
            }
        )
        stock = 'HPG'
        name = stock
        trade_direction='Long' 
        use_shift=False
        n_shift=15 
        holding_periods=15
        

    df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
    df = df_stock[df_stock['stock'] == stock].reset_index(drop=True)

    bt = Simulator(
        func=Conds.compute_any_conds,
        df_ohlcv=df,
        params=params,
        name=name,
    )
    try:
        bt.run(
            trade_direction=trade_direction, 
            use_shift=use_shift,
            n_shift=n_shift,
            holding_periods=holding_periods
        )
    except Exception as e:
        print(f"scan error: {e}")

    return bt

@app.task(name=TaskName.SCAN_STOCK_V2)
def scan_one_stock_v2(
        params: dict, 
        stock,
        name="", 
        trade_direction='Long',
        use_shift=False,
        n_shift=15,
        use_holding_periods = True, 
        holding_periods=15,
        use_takeprofit_cutloss=False,
        profit_thres=5,
        loss_thres=5
    ):
    """
    Backtest 1 cp, xét thêm loss threshold và take-profit threshold

    Tải dữ liệu cổ phiếu từ Plasma, tạo "Simulator" (bt) với hàm compute_any_conds  và sử dụng hàm `bt.run2()` để thực hiện backtest. 
    """
    exit_params = params.pop('exit_cond') if 'exit_cond' in params else {}
    
    df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
    df = df_stock[df_stock['stock'] == stock].reset_index(drop=True)

    bt = Simulator(
        func=Conds.compute_any_conds,
        df_ohlcv=df,
        params=params,
        exit_params=exit_params,
        name=name,
    )

    try:
        bt.run2(
            trade_direction, 
            use_shift=use_shift,
            n_shift=n_shift,
            use_holding_periods=use_holding_periods,
            holding_periods=holding_periods,
            use_takeprofit_cutloss=use_takeprofit_cutloss,
            profit_thres=profit_thres,
            loss_thres=loss_thres 
            )
    except Exception as e:
        print(f"scan error: {e}")

    return bt
    
@app.task(name=TaskName.SCAN_STOCK_V3)
def scan_one_stock_v3(
    params, 
    stock,
    name="", 
    trade_direction='Long', 
    use_shift=False,
    n_shift=15, 
    holding_periods=15
    ):
    """ Backtest 1 cp không có điều kiện loss threshold và take-profit threshold
    - Tải dữ liệu cổ phiếu từ Plasma.
    - Tạo đối tượng Simulator với hàm compute_any_conds và gọi bt.run3() 
    """
    df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
    df = df_stock[df_stock['stock'] == stock].reset_index(drop=True)
    
    bt = Simulator(
        func=Conds.compute_any_conds,
        df_ohlcv=df,
        params=params,
        name=name,
    )
    try:
        bt.run3(
            trade_direction=trade_direction, 
            use_shift=use_shift,
            n_shift=n_shift,
            holding_periods=holding_periods
        )
    except Exception as e:
        print(f"scan error: {e}")

    return bt
    
@app.task(name=TaskName.SCAN_STOCK_V4)
def scan_one_stock_v4(
    params, 
    stock,
    name="", 
    trade_direction='Long', 
    use_shift=False,
    n_shift=15, 
    holding_periods=15
    ):
    """ 
    Backtest tương tự scan_one_stock_v3 nhưng với định dạng dữ liệu đầu vào khác,
    dữ liệu được pivot lại trước khi đưa vào mô hình.

    """
    df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
    df = df_stock.pivot(index = 'day', columns = 'stock')\
        .xs(stock, axis=1, level='stock')\
        .reset_index(drop=False)
    df['stock'] = stock

    
    bt = Simulator(
        func=Conds.compute_any_conds,
        df_ohlcv=df,
        params=params,
        name=name,
    )
    try:
        bt.run3(
            trade_direction=trade_direction, 
            use_shift=use_shift,
            n_shift=n_shift,
            holding_periods=holding_periods
        )
    except Exception as e:
        print(f"scan error: {e}")

    return bt

@app.task(name=TaskName.COMPUTE_ONE_STOCK_SIGNALS)
def compute_one_stock_signals(params, stock):
    def test():
        params = {
            'price_change': {'use_flag': True}
        }
        stock = 'KDC'

    df_stock: pd.DataFrame = Adapters.load_stocks_data_from_plasma()
    df = df_stock.pivot(index = 'day', columns = 'stock')\
        .xs(stock, axis=1, level='stock')\
        .reset_index(drop=False)
    df['stock'] = stock

    return Conds.compute_any_conds(df, params)



    
    


# celery -A celery_worker worker --concurrency=30 --loglevel=INFO -n celery_worker@pylabview