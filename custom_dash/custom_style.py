"""import libraries"""

import pandas as pd
from dash import dcc, dash_table, html
import dash_bootstrap_components as dbc

class Colors:
    veri_peri = "#6667AB"


class Components:
    @staticmethod
    def rangeInput(
        id_lower, id_upper, lower_title, upper_title, lower_value=-999, upper_value=999
    ):
        return html.Div(
            [
                html.Div(
                    html.P(lower_title),
                    style={"display": "inline-block", "margin-right": "5px"},
                ),
                html.Div(
                    dbc.Input(
                        type="number", id=id_lower, value=lower_value, debounce=True
                    ),  
                    style={
                        "display": "inline-block",
                        "margin-right": "20px",
                        "max-width": "100px",
                    },
                ),
                html.Div(
                    html.P("Upper"),
                    style={"display": "inline-block", "margin-right": "5px"},
                ),
                html.Div(
                    dbc.Input(
                        type=upper_title, id=id_upper, value=upper_value, debounce=True
                    ),
                    style={"display": "inline-block", "max-width": "100px"},
                ),
            ],
        )
    
    @staticmethod
    def accordion(content: list):
        accordion = html.Div(
            dbc.Accordion(
                content
            )
        )
        return accordion
    
    @staticmethod
    def accordion_item(title, content):
        return dbc.AccordionItem(
            content,
            title=title,
        )
    
    @staticmethod
    def content_area(content):
        return html.Div(
                children=content,
                style={
                    'backgroundColor': '#D9DAF0',  # Màu Very Peri nhạt
                    'borderRadius': '8px',  # Bo góc
                    'padding': '10px',  # Khoảng cách bên trong
                    # 'color': 'white',  # Màu chữ
                }
            )

    @staticmethod
    def enter_space():
        return html.Br()

    @staticmethod
    def title(txt: str):
        return html.H5(txt)

    @staticmethod
    def sub_title(txt: str):
        return html.H6(txt)

    @staticmethod
    def sep_line():
        return html.Hr()
    
    @staticmethod
    def sub_sep_line():
        return dbc.Row(
            [
                dbc.Col(html.Div(),width=4),
                dbc.Col(html.Hr(), width=4),
                dbc.Col(html.Div(),width=4),
            ]
        )
    
    @staticmethod
    def sub_group_title(title):
        return html.P(title, style={'textAlign': 'center',  'color': Colors.veri_peri})

    @staticmethod
    def group_to_one_div(cpns: list):
        return html.Div(cpns)

    @staticmethod
    def textInput(id, placeholder, value):
        return html.Div(
            [
                html.Div(
                    html.P(placeholder),
                    style={"display": "inline-block", "margin-right": "5px"},
                ),
                dcc.Input(
                    id=id,
                    type="text",
                    value=value,
                    placeholder=placeholder,
                    debounce=True,
                ),
            ],
            style={"display": "inline"},
        )

    @staticmethod
    def numberInput(id, value, title, min=None, max=None, style=None):
        title = html.Div(
            html.P(title),
            style={"display": "inline-block", "margin-right": "5px"},
        )
        input_box = html.Div(
            dbc.Input(
                type="number",
                min=min,
                max=max,
                id=id,
                value=value,
                debounce=True,
            ),
            style={"display": "inline-block"},
        )
        content = [title, input_box]
        if style is not None:
            content.append(style)

        return html.Div(content)

    @staticmethod
    def dropDown(id, defval, values: list, title, multi=False):
        
        content = []
        if len(title) > 0:
            content.append(html.P(title, style={"margin-right":"5px"}))
        
        content.append(                
            dcc.Dropdown(
                    values,
                    value=defval,
                    id=id,
                    multi=multi,
                    style={ "min-width": "200px", "margin":0, "padding":0}
                )
        )
        
        drop_down = html.Div(
            content, 
            style={
                'display': 'flex',
                'alignItems': 'baseline'
            }
        )
        return drop_down
    
    @staticmethod
    def checkBox(id, value, title):
        use_flag = dbc.Checkbox(
            id=id,
            label=title,
            value=value,
        )
        return use_flag

    @staticmethod
    def dataTable(df: pd.DataFrame, id, style_data_conditional=None):
        params = {
            "data": df.to_dict("records"),
            "columns": [{"name": i, "id": i} for i in df.columns],
            "style_table": {"overflowY": "auto", "overflowX": "auto"},
            "fixed_rows": {"headers": True},  # Cố định các hàng tiêu đề
            "id": id,
            "style_header":{
                'backgroundColor': Colors.veri_peri,  # Đổi màu nền của header sang Veri Peri
                'color': 'white',  # Đổi màu chữ của header
                # 'fontWeight': 'bold',
                'border': '1px solid black'  # Viền header
            },
            "style_cell": {
                'textAlign': 'left',
                'whiteSpace': 'normal',
                'height': 'auto',
                'minWidth': '100px', 'width': '100px', 'maxWidth': '100px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis'
            },
            "filter_action": "native",
            "sort_action": "native",
            "sort_mode": "multi",
        }
        if style_data_conditional is not None:
            params['style_data_conditional'] = style_data_conditional
        return dash_table.DataTable(**params)
    
    @staticmethod
    def rowSelectDataTable(df: pd.DataFrame, id):
        return dash_table.DataTable(
            id=id,
            columns=[
                {"name": i, "id": i} for i in df.columns
            ],
            data=df.to_dict('records'),

            style_table = {"overflowY": "auto", "overflowX": "auto"},
            fixed_rows={"headers": True},
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            row_selectable="multi",
            selected_rows=[],
        )

    @staticmethod
    def loading():
        return html.Div(
            [
                dcc.Loading(
                    id="loading-1",
                    type="circle",
                    children=[html.Div(id="loading-output")],
                )
            ]
        )

    @staticmethod
    def graph(id, fig):
        return dcc.Graph(id=id, figure=fig)

    @staticmethod
    def radio_items(id, defval, options, inline):
        return dbc.RadioItems(
            options=options,
            value=defval,
            id=id,
            inline=inline,
        )
        # return dcc.RadioItems(options, defval, inline=inline, id=id)

    @staticmethod
    def store_data(id):
        return dcc.Store(id)
    
    @staticmethod
    def inline(components, gap=2):
        return dbc.Stack(
            components,
            direction="horizontal",
            gap=gap,
        )
        
    @staticmethod
    def button(id, text, class_name="me-2", color = "light", n_clicks=0):
        return dbc.Button(children=text, id=id, n_clicks=n_clicks, className=class_name, color=color, style={'color': Colors.veri_peri})

def create_tabs(
    tab1_name, tab1_content, tab2_name, tab2_content, tab3_name, tab3_content
):
    tab1 = dbc.Card(
        dbc.CardBody([tab1_content]),
        className="mt-3",
    )

    tab2 = dbc.Card(
        dbc.CardBody([tab2_content]),
        className="mt-3",
    )

    tab3 = dbc.Card(
        dbc.CardBody([tab3_content]),
        className="mt-3",
    )

    tabs = dbc.Tabs(
        [
            dbc.Tab(tab1, label=tab1_name),
            dbc.Tab(tab2, label=tab2_name),
            dbc.Tab(tab3, label=tab3_name),
        ]
    )
    return tabs

def create_modal(modal_id, modal_button_name, modal_title, contents: list, footer_content: list = None):
    
    footers = [Components.button(text="Close", id=f"{modal_id}_close", n_clicks=0)]
    if footer_content is not None:
        footers = footer_content + footers
    
    modal = html.Div(
        [
            Components.button(text=modal_button_name, id=f"{modal_id}_open", n_clicks=0),
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle(modal_title)),
                    dbc.ModalBody(
                        html.Div(
                            contents
                        )
                    ),
                    dbc.ModalFooter(
                        Components.inline(footers)
                    ),
                ],
                id=modal_id,
                is_open=False,
            ),
        ]
    )

    return modal

def create_space(contents: list):
    return html.Div(
        children=contents,
        style={
            'padding': '10px 10px 10px 10px',  # padding-top, padding-right, padding-bottom, padding-left
        }
    )
    
