import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dc_server.adapters.pickle_adapter_ssi_ws import PICKLED_WS_ADAPTERS
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import redis, pickle
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

redis_client = redis.Redis(host='localhost', port=6379, db=0)

chooseStocks = ['SHS','VGS','MBS','VIX','CTS','ORS','CEO','FTS','DTD','AGR','GEX','BSI','HUT','VCI','DIG','VND','VDS','L14','DXG','DGW','PDR','HCM','CII','HTN','GVR','NKG','BVS','HSG','TCI','NVL','SSI','GIL','PXL','KSB','PLC','NLG','KBC','DDV','FCN','LCG','DPG','DBC','TCH','VOS','VPG','HDC','IDJ','ANV','VCG','PET','VGC','PC1','HAH','ASM','IJC','C4G','BCG','HHV','DXS','CSV','IDI','TNG','SZC','HHS','CTD','KHG','ADS','PVC','TLH','DCM','DGC','SCR','S99','TIG','MWG','LAS','PVD','AAA','PVT','POM','MSN','HBC','PVS','CMX','VTP','PVB','LSS','IDC','TIP','DPM','HDG','VSC','HQC','HPG','SMC','EVF','NTL','VGI','DRI','PAN','VHC','CSC','CNG','VRE','EIB','STB','DSC','TCB','KDH','MSB','VHM','TV2','CTR','LDG','VGT','CTI','BSR','SHB','MSH','CTG','ELC','PHR','VIB','VIC','PSH','TTF','BFC','TPB','MSR','LHG','VCS','SIP','OCB','SKG','DPR','GMD','VPB','MBB','BID','APH','HAG','PLX','AGG','HAX','SBT','LPB','BMP','BCM','GEG','FPT','POW','HVN','PVP','TNH','DHC','NT2','OIL','DRC','BVH','REE','FRT','ABB','ACB','HNG','CMG','GAS','GSP','VIP','HDB','DHA','SAB','VTO','PNJ','BAF','QNS','NAF','YEG','VJC','VNM','NAB','PTB','VCB','ITD','TCM','VPI','VEA','SJS','MCH','SSB','FOX','ACV','SCS','BWE','NCT','KDC']

def get_data_days():
    db = PICKLED_WS_ADAPTERS.get_target_pickled_db()
    return sorted(db.list_collection_names())

def get_max_datatime():
    max_col = max(get_data_days())
    return max_col

def load_data_for_date(day=None):
    if day is None:
        day = get_max_datatime()

    db = PICKLED_WS_ADAPTERS.load_hose500_from_db(day)
    df = pd.DataFrame.from_dict(db.get("data"))


    stocks = [s for s in chooseStocks if s in df['stock'].unique()]
    df['datetime'] = day + ' ' + df['time'].astype(str).str[2:]

    return df[df['stock'].isin(stocks) & (df['last'] != 0)].sort_values('time').copy()

def resample(df: pd.DataFrame, timeframe):
    def test():
        day = get_max_datatime()
        df = load_data_for_date(day)
        seconds = 30

    df['candleTime'] = pd.to_datetime(df['datetime'], format='%Y_%m_%d %H%M%S')
    df['candleTime'] = df['candleTime'].dt.floor(timeframe)

    df['firstTime'] = df['lastTime'] = df['candleTime'].copy()

    # Calculate candle price
    df['open'] = df['high'] = df['low'] = df['close'] = df['last']
    df['return'] = df['close'] - df['refPrice']

    # Calculate cumulative value
    df['cumVal'] = df.groupby(['stock', 'matchedBy'])['matchingValue'].cumsum() / 1e9
    df['bu'] = np.where(df['matchedBy'] >= 0, df['cumVal'],np.nan)
    df['bu'] = df['bu'].ffill()
    df['sd'] = np.where(df['matchedBy'] == -1, df['cumVal'], np.nan)
    df['sd'] = df['sd'].ffill()

    # Calculate Bid/Ask
    df['bid'] = df['bestBid1'] * df['bestBid1Volume'] + df['bestBid2'] * df['bestBid2Volume'] + df['bestBid2'] * df['bestBid2Volume'] + df['bestBid3'] * df['bestBid3Volume']
    df['ask'] = df['bestOffer1'] * df['bestOffer1Volume'] + df['bestOffer2'] * df['bestOffer2Volume'] + df['bestOffer2'] * df['bestOffer2Volume'] + df['bestOffer3'] * df['bestOffer3Volume']

    return df.groupby(['stock', 'candleTime']).agg({
        'firstTime': 'first', 
        'lastTime': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'return': 'last',
        'matchingVolume': 'sum',
        'bu': 'last',
        'sd': 'last',
        'bid': 'max', 
        'ask': 'max'
    })




def plot(df: pd.DataFrame, symbol, timeframe):
    def test():
        df = load_data_for_date()
        symbol = 'HPG'
        timeframe = '30s'
    df_plot = resample(df, timeframe)
    df_plot = df_plot.loc[symbol].reset_index()
    df_plot['candleTime'] = df_plot['candleTime'].astype(str).str[-8:]
    fig = make_subplots(rows=5, cols=1, 
                        shared_xaxes=True, 
                        vertical_spacing=0.02,
                        row_heights=[0.4, 0.15, 0.15, 0.15, 0.15])

    # Candlestick chart
    fig.add_trace(go.Candlestick(x=df_plot['candleTime'],
                                open=df_plot['open'],
                                high=df_plot['high'],
                                low=df_plot['low'],
                                close=df_plot['close'],
                                name='Price'),
                            row=1, col=1)

    # Volume chart
    fig.add_trace(go.Bar(x=df_plot['candleTime'],
                        y=df_plot['matchingVolume'],
                        name='Volume'),
                row=2, col=1)

    # Return chart
    fig.add_trace(go.Scatter(x=df_plot['candleTime'],
                            y=df_plot['return'],
                            mode='lines',
                            name='Return'),
                row=3, col=1)

    # BUSD chart
    fig.add_trace(go.Scatter(x=df_plot['candleTime'],
                            y=df_plot['bu'],
                            mode='lines',
                            name='BU'),
                row=4, col=1)
    fig.add_trace(go.Scatter(x=df_plot['candleTime'],
                        y=df_plot['sd'],
                        mode='lines',
                        name='SD'),
            row=4, col=1)

    # Bid-Ask Spread chart
    bid_ask_spread = df_plot['ask'] - df_plot['bid']
    fig.add_trace(go.Scatter(x=df_plot['candleTime'],
                            y=bid_ask_spread,
                            mode='lines',
                            name='Bid-Ask Spread'),
                row=5, col=1)

    # Update layout
    fig.update_layout(title='Stock Price, Volume, Return, BUSD, and Bid-Ask Spread',
                    xaxis_rangeslider_visible=False,
                    height=1500)

    # Update y-axis labels
    fig.update_yaxes(title_text='Price', row=1, col=1)
    fig.update_yaxes(title_text='Volume', row=2, col=1)
    fig.update_yaxes(title_text='Return', row=3, col=1)
    fig.update_yaxes(title_text='BUSD', row=4, col=1)
    fig.update_yaxes(title_text='Bid-Ask', row=5, col=1)

    # Show the plot
    fig.show()
    return fig


# Khởi tạo ứng dụng Dash
app = dash.Dash(__name__, prevent_initial_callbacks=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout của ứng dụng
app.layout = html.Div([
    html.H1("Phân tích cổ phiếu"),
    html.Div([
        dcc.Dropdown(
            id='date-picker',
            options=get_data_days(),
            value=get_max_datatime()
        ),
        dbc.Button("Load Data",id='cfm_load_data'),
        dcc.Dropdown(
            id='timeframe-dropdown',
            options=[
                {'label': '30 giây', 'value': '30S'},
                {'label': '1 phút', 'value': '1T'},
                {'label': '5 phút', 'value': '5T'}
            ],
            value='30S'
        ),
        dcc.Dropdown(
            id='stock-dropdown',
            options=[{'label': stock, 'value': stock} for stock in chooseStocks],  # Thêm các mã cổ phiếu khác vào đây
            value='HPG'
        )
    ]),
    dcc.Loading(dcc.Graph(id='stock-graph'))
])

@app.callback(
    Output('stock-graph', 'figure', allow_duplicate=True),
    [
        State('timeframe-dropdown', 'value'),
        State('stock-dropdown', 'value'),
        State('date-picker', 'value')
    ],
    [
        Input('cfm_load_data', 'n_clicks')
    ]
)
def update_graph(timeframe, symbol, day, n_clicks):
    stock_data = load_data_for_date(day)
    if stock_data is not None:
        redis_client.set("dang_demo_cached", pickle.dumps(stock_data))
    fig = plot(stock_data, symbol, timeframe)
    return fig

@app.callback(
    Output('stock-graph', 'figure', allow_duplicate=True),
    [
        Input('timeframe-dropdown', 'value'),
        Input('stock-dropdown', 'value')
    ]
)
def update_graph2(timeframe, symbol):
    stock_data = pickle.loads(redis_client.get("dang_demo_cached"))
    if stock_data is not None:
        fig = plot(stock_data, symbol, timeframe)
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, host='localhost', port = 1599)