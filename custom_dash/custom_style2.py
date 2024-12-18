"""Custom style components for Dash applications"""

import pandas as pd
from dash import dcc, dash_table, html
import dash_bootstrap_components as dbc

class Colors:
    # Main color palette
    primary = "#6667AB"  # Veri Peri
    secondary = "#D9DAF0"  # Light Veri Peri
    success = "#2A9D8F"  # Teal
    warning = "#F4A261"  # Sandy Brown
    danger = "#E76F51"  # Burnt Sienna
    info = "#457B9D"  # Celadon Blue
    light = "#F8F9FA"
    dark = "#1D3557"
    white = "#FFFFFF"
    
    # Background colors
    bg_light = "#F8F9FA"
    bg_lighter = "#FFFFFF"
    
    # Text colors
    text_primary = "#2C3E50"
    text_secondary = "#6C757D"
    text_light = "#ADB5BD"

class Styles:
    # Common styles
    CARD = {
        'backgroundColor': Colors.bg_lighter,
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
        'padding': '20px',
        'marginBottom': '20px'
    }
    
    BUTTON = {
        'backgroundColor': Colors.primary,
        'color': Colors.white,
        'border': 'none',
        'borderRadius': '6px',
        'padding': '10px 20px',
        'cursor': 'pointer',
        'transition': 'all 0.2s ease-in-out',
        ':hover': {
            'backgroundColor': Colors.secondary,
            'transform': 'translateY(-1px)'
        }
    }
    
    INPUT = {
        'border': f'1px solid {Colors.secondary}',
        'borderRadius': '6px',
        'padding': '8px 12px',
        'width': '100%',
        'transition': 'border-color 0.2s ease-in-out',
        ':focus': {
            'borderColor': Colors.primary,
            'outline': 'none'
        }
    }

class Components:
    @staticmethod
    def card(title, content, subtitle=None):
        """Create a styled card component"""
        header = [html.H4(title, className="card-title")] 
        if subtitle:
            header.append(html.P(subtitle, className="card-subtitle text-muted"))
            
        return dbc.Card([
            dbc.CardHeader(header),
            dbc.CardBody(content)
        ], className="shadow-sm mb-4")

    @staticmethod
    def section(title, content):
        """Create a section with title and content"""
        return html.Div([
            html.H5(title, className="mb-3", style={'color': Colors.primary}),
            html.Div(content, style={'padding': '10px'})
        ], className="mb-4")

    @staticmethod
    def accordion(content: list):
        """Create a styled accordion"""
        return html.Div([
            dbc.Accordion(
                content,
                style={
                    'borderRadius': '8px',
                    'overflow': 'hidden'
                }
            )
        ])
    
    @staticmethod
    def accordion_item(title, content):
        """Create a styled accordion item"""
        return dbc.AccordionItem(
            content,
            title=title,
            style={
                'backgroundColor': Colors.bg_light,
                'border': f'1px solid {Colors.secondary}'
            }
        )
    
    @staticmethod
    def content_area(content):
        """Create a styled content area"""
        return html.Div(
            children=content,
            style={
                'backgroundColor': Colors.secondary,
                'borderRadius': '8px',
                'padding': '15px',
                'marginBottom': '15px'
            }
        )

    @staticmethod 
    def dropDown(id, defval, values: list, title, multi=False):
        """Create a styled dropdown"""
        content = []
        if title:
            content.append(
                html.Label(
                    title,
                    style={
                        'marginBottom': '5px',
                        'color': Colors.text_primary,
                        'fontWeight': '500'
                    }
                )
            )
        
        content.append(
            dcc.Dropdown(
                values,
                value=defval,
                id=id,
                multi=multi,
                style={
                    'borderRadius': '6px',
                    'border': f'1px solid {Colors.secondary}',
                    'minWidth': '200px'
                }
            )
        )
        
        return html.Div(
            content,
            style={
                'marginBottom': '15px'
            }
        )

    @staticmethod
    def checkBox(id, value, title):
        """Create a styled checkbox"""
        return dbc.Checkbox(
            id=id,
            label=title,
            value=value,
            style={
                'marginBottom': '10px'
            }
        )

    @staticmethod
    def numberInput(id, value, title, min=None, max=None, style=None):
        """Create a styled number input"""
        input_style = {
            'width': '150px',
            'borderRadius': '6px',
            'border': f'1px solid {Colors.secondary}'
        }
        if style:
            input_style.update(style)
            
        return html.Div([
            html.Label(
                title,
                style={
                    'marginBottom': '5px',
                    'color': Colors.text_primary,
                    'fontWeight': '500',
                    'display': 'block'
                }
            ),
            dbc.Input(
                type="number",
                min=min,
                max=max,
                id=id,
                value=value,
                style=input_style,
                debounce=True
            )
        ], style={'marginBottom': '15px'})

    @staticmethod
    def button(id, text, class_name="me-2", color="primary", n_clicks=0):
        """Create a styled button"""
        return dbc.Button(
            children=text,
            id=id,
            n_clicks=n_clicks,
            className=f"{class_name} shadow-sm",
            color=color,
            style={
                'borderRadius': '6px',
                'padding': '8px 16px'
            }
        )
    
    @staticmethod
    def inline(components, gap=2):
        """Create horizontal stack of components"""
        return dbc.Stack(
            components,
            direction="horizontal", 
            gap=gap
        )

    @staticmethod
    def graph(id, fig):
        """Create a styled graph"""
        return html.Div([
            dcc.Graph(
                id=id,
                figure=fig,
                style={
                    'backgroundColor': Colors.bg_lighter,
                    'borderRadius': '8px',
                    'padding': '10px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
                }
            )
        ])

    @staticmethod
    def dataTable(df: pd.DataFrame, id, style_data_conditional=None):
        """Create a styled data table"""
        params = {
            "data": df.to_dict("records"),
            "columns": [{"name": i, "id": i} for i in df.columns],
            "style_table": {
                "overflowY": "auto",
                "overflowX": "auto",
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
            },
            "style_header": {
                'backgroundColor': Colors.primary,
                'color': Colors.white,
                'fontWeight': '600',
                'border': '1px solid rgba(255,255,255,0.2)'
            },
            "style_cell": {
                'textAlign': 'left',
                'padding': '12px 15px',
                'backgroundColor': Colors.bg_lighter,
                'color': Colors.text_primary,
                'border': f'1px solid {Colors.secondary}'
            },
            "style_data": {
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            "id": id,
            "filter_action": "native",
            "sort_action": "native",
            "sort_mode": "multi",
        }
        
        if style_data_conditional:
            params['style_data_conditional'] = style_data_conditional
            
        return dash_table.DataTable(**params)

# Helper functions
def create_modal(modal_id, modal_button_name, modal_title, contents: list, footer_content: list = None):
    """Create a styled modal"""
    footers = [Components.button(text="Close", id=f"{modal_id}_close", n_clicks=0)]
    if footer_content:
        footers = footer_content + footers
        
    return html.Div([
        Components.button(
            text=modal_button_name,
            id=f"{modal_id}_open",
            n_clicks=0
        ),
        dbc.Modal([
            dbc.ModalHeader(
                dbc.ModalTitle(modal_title),
                style={'backgroundColor': Colors.primary, 'color': Colors.white}
            ),
            dbc.ModalBody(
                html.Div(contents),
                style={'padding': '20px'}
            ),
            dbc.ModalFooter(
                html.Div(footers, className="d-flex gap-2"),
                style={'borderTop': f'1px solid {Colors.secondary}'}
            )
        ],
        id=modal_id,
        is_open=False,
        size="xl",
        style={'borderRadius': '12px'})
    ])

def create_space(contents: list):
    """Create a styled container with space"""
    return html.Div(
        children=contents,
        style={
            'padding': '24px',
            'backgroundColor': Colors.bg_light,
            'minHeight': '100vh'
        }
    )