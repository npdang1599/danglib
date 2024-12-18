import pandas as pd
from danglib.pylabview.funcs import Ta, glob_obj

def plot_cross_signals(
    df: pd.DataFrame,
    src1_name: str = "close",
    src2_name: str = "ma20",
    direction: str = "crossover",
    bars_no_reverse: int = 5,
    start_index: int = None,
    end_index: int = None
):
    """Plot interactive chart showing cross signals and valid entry points
    
    Args:
        df (pd.DataFrame): Input dataframe
        src1_name (str): Name of first line
        src2_name (str): Name of second line
        direction (str): "crossover" or "crossunder"
        bars_no_reverse (int): Bars to check for no reverse signal
        start_index (int): Start index for plotting
        end_index (int): End index for plotting
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Get data slice
    if start_index is None:
        start_index = 0
    if end_index is None:
        end_index = len(df)
    
    plot_df = df.iloc[start_index:end_index].copy()
    
    # Calculate signals
    line1 = plot_df[src1_name]
    line2 = plot_df[src2_name]

    # Get initial cross signals
    if direction == "crossover":
        cross_signals = Ta.crossover(line1, line2)
        reverse_signals = Ta.crossunder(line1, line2)
    else:  # crossunder
        cross_signals = Ta.crossunder(line1, line2)
        reverse_signals = Ta.crossover(line1, line2)

    # Calculate valid signals
    reverse_windows = reverse_signals.rolling(
        window=bars_no_reverse,
        min_periods=1
    ).sum()
    
    valid_signals = cross_signals.shift(bars_no_reverse) & (reverse_windows == 0)

    # Create figure
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                       vertical_spacing=0.05,
                       row_heights=[0.7, 0.3])

    # Plot lines
    fig.add_trace(
        go.Scatter(
            x=plot_df.index,
            y=line1,
            name=src1_name,
            line=dict(color='blue')
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=plot_df.index,
            y=line2,
            name=src2_name,
            line=dict(color='orange')
        ),
        row=1, col=1
    )

    # Plot cross signals
    cross_points = plot_df[cross_signals].index
    if direction == "crossover":
        marker_symbol = "triangle-up"
        marker_color = "green"
    else:
        marker_symbol = "triangle-down"
        marker_color = "red"
        
    fig.add_trace(
        go.Scatter(
            x=cross_points,
            y=plot_df.loc[cross_points, src1_name],
            mode='markers',
            name='Cross Signal',
            marker=dict(
                symbol=marker_symbol,
                size=10,
                color=marker_color
            )
        ),
        row=1, col=1
    )

    # Plot valid entry points
    valid_points = plot_df[valid_signals].index
    fig.add_trace(
        go.Scatter(
            x=valid_points,
            y=plot_df.loc[valid_points, src1_name],
            mode='markers',
            name='Valid Entry',
            marker=dict(
                symbol='circle',
                size=12,
                color='blue'
            )
        ),
        row=1, col=1
    )

    # Plot signals in lower panel
    fig.add_trace(
        go.Scatter(
            x=plot_df.index,
            y=cross_signals.astype(int),
            name='Cross',
            line=dict(color='green')
        ),
        row=2, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=plot_df.index,
            y=valid_signals.astype(int),
            name='Valid Entry',
            line=dict(color='blue')
        ),
        row=2, col=1
    )

    # Update layout
    fig.update_layout(
        title=f"{direction.capitalize()} signals with {bars_no_reverse} bars confirmation",
        xaxis_title="Date",
        height=800,
        showlegend=True
    )

    return fig


df = glob_obj.get_one_stock_data("HPG")
# Tạo một MA20 để test
df['ma20'] = Ta.sma(df['close'], 20)



# Vẽ biểu đồ cho 200 bars gần nhất
fig = plot_cross_signals(
    df,
    src1_name="close",
    src2_name="ma20",
    direction="crossover",
    bars_no_reverse=5,
    start_index=-200
)
fig.show()