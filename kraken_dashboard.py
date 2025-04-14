# 启动数据消费线程
def start_data_threads():
    # 深度数据线程
    depth_thread = threading.Thread(target=consume_depth_data)
    depth_thread.daemon = True
    depth_thread.start()

    # 成交数据线程
    trade_thread = threading.Thread(target=consume_trade_data)
    trade_thread.daemon = True
    trade_thread.start()

    # Ticker数据线程
    ticker_thread = threading.Thread(target=consume_ticker_data)
    ticker_thread.daemon = True
    ticker_thread.start()

    print("所有数据消费线程已启动")


if __name__ == '__main__':
    # 启动数据线程
    start_data_threads()

    # 启动Dash应用
    print("启动订单流分析仪表盘服务器...")
    app.run_server(debug=False, host='0.0.0.0', port=8050)
    print("仪表盘服务器已关闭")  # 如果启用了高级分析，添加高级分析回调
if advanced_analysis_enabled:
    @app.callback(
        [Output('iceberg-detection', 'children'),
         Output('darkpool-activity', 'children'),
         Output('hft-activity', 'children'),
         Output('whale-tracking', 'children')],
        [Input('interval-component', 'n_intervals')]
    )
    def update_advanced_analysis(n):
        # 检查是否有足够的数据
        if len(depth_data) < 10 or len(trade_data) < 10:
            return [html.P("等待更多数据...") for _ in range(4)]

        # 准备数据
        recent_depth = list(depth_data)[-100:]
        recent_trades = list(trade_data)[-1000:]
        trades_df = pd.DataFrame(recent_trades)

        # 构建价格历史
        price_history = []
        if 'p' in trades_df.columns and 'T' in trades_df.columns:
            trades_df['price'] = trades_df['p'].astype(float)
            trades_df['timestamp'] = trades_df['T'].astype(float)
            trades_df = trades_df.sort_values('timestamp')
            price_history = [{"timestamp": ts, "price": p} for ts, p in zip(trades_df['timestamp'], trades_df['price'])]

        # 1. 冰山订单检测
        iceberg_orders = detect_iceberg_orders(recent_depth, recent_trades)
        iceberg_content = html.Div([
            html.P(f"检测到 {len(iceberg_orders)} 个可能的冰山订单"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("价格"), html.Th("方向"), html.Th("平均交易量"),
                        html.Th("重复次数"), html.Th("置信度")
                    ])),
                    html.Tbody([
                        html.Tr([
                            html.Td(f"{order['price']}"),
                            html.Td(order['side']),
                            html.Td(f"{order['avg_volume']:.4f}"),
                            html.Td(f"{order['count']}"),
                            html.Td(f"{order['confidence']:.2f}")
                        ]) for order in iceberg_orders[:5]  # 显示前5个
                    ])
                ]) if iceberg_orders else html.P("未检测到冰山订单")
            ])
        ])

        # 2. 暗池活动检测
        darkpool_signals = detect_dark_pool_activity(trades_df, recent_depth, price_history)
        darkpool_content = html.Div([
            html.P(f"检测到 {len(darkpool_signals)} 个暗池活动信号"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("类型"), html.Th("时间"), html.Th("幅度"), html.Th("置信度")
                    ])),
                    html.Tbody([
                        html.Tr([
                            html.Td(signal['type']),
                            html.Td(datetime.datetime.fromtimestamp(signal['time'] / 1000).strftime('%H:%M:%S')),
                            html.Td(f"{signal['magnitude']:.4f}"),
                            html.Td(f"{signal['confidence']:.2f}")
                        ]) for signal in darkpool_signals[:5]  # 显示前5个
                    ])
                ]) if darkpool_signals else html.P("未检测到暗池活动")
            ])
        ])

        # 3. 高频交易模式
        if format_adapter_enabled:
            # 使用修复后的函数
            hft_patterns = fix_detect_hft_patterns(list(depth_changes), trades_df)
        else:
            # 如果没有适配器，使用原始函数但带异常处理
            try:
                hft_patterns = detect_hft_patterns(list(depth_changes), trades_df)
            except TypeError as e:
                print(f"检测高频交易模式时出错: {e}")
                hft_patterns = []

        hft_content = html.Div([
            html.P(f"检测到 {len(hft_patterns)} 种HFT模式"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("模式类型"), html.Th("详情"), html.Th("置信度")
                    ])),
                    html.Tbody([
                        html.Tr([
                            html.Td(pattern['type']),
                            html.Td(get_hft_pattern_details(pattern)),
                            html.Td(f"{pattern.get('confidence', 'N/A')}")
                        ]) for pattern in hft_patterns
                    ])
                ]) if hft_patterns else html.P("未检测到高频交易模式")
            ])
        ])

        # 4. 大玩家跟踪
        whale_update = whale_tracker.update(recent_trades, recent_depth)
        whale_content = html.Div([
            html.P(f"活跃大玩家: {whale_update['active_whales']}"),
            html.P(f"积累模式: {whale_update['accumulation_patterns']}"),
            html.P(f"大额交易: {len(whale_update['large_trades'])}"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([html.Th("信号类型"), html.Th("方向"), html.Th("强度")])),
                    html.Tbody([
                        html.Tr([
                            html.Td(signal['type']),
                            html.Td(signal['direction']),
                            html.Td(f"{signal['strength']:.2f}")
                        ]) for signal in whale_update['signals']
                    ])
                ]) if whale_update['signals'] else html.P("无大玩家交易信号")
            ])
        ])

        return iceberg_content, darkpool_content, hft_content, whale_content


def get_hft_pattern_details(pattern):
    """格式化HFT模式详情"""
    if pattern['type'] == 'flash_orders':
        return f"闪单数: {pattern['count']}, 平均持续时间: {pattern['avg_duration_ms']:.2f}ms"
    elif pattern['type'] == 'sweeping':
        return f"扫单实例: {pattern['instances']}, 最大扫单量: {pattern['largest_sweep_volume']:.4f}"
    elif pattern['type'] == 'layered_probing':
        return "多层价格试探"
    return "未知模式" @ app.callback(
        [Output('main-chart', 'figure'),
         Output('orderbook-chart', 'figure'),
         Output('ofi-chart', 'figure'),
         Output('equity-chart', 'figure'),
         Output('signals-table', 'children'),
         Output('trade-decisions', 'children'),
         Output('trade-history-table', 'children')],
        [Input('interval-component', 'n_intervals')]
    )


def update_charts(n):
    # 检查是否有足够的数据
    if len(calculated_metrics) < 2:
        # 返回空图表
        empty_fig = go.Figure()
        empty_fig.update_layout(title="等待数据...")
        return empty_fig, empty_fig, empty_fig, empty_fig, html.P("等待数据..."), html.P("等待数据..."), html.P(
            "等待数据...")

    # 转换为DataFrame
    df = pd.DataFrame(list(calculated_metrics))
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

    # 创建主图表 - 价格和Delta，新增入场出场点位
    fig1 = make_subplots(rows=2, cols=1,
                         shared_xaxes=True,
                         vertical_spacing=0.1,
                         subplot_titles=('BTC/USD价格 (带入场出场信号)', 'Delta(买卖差值)'),
                         row_heights=[0.7, 0.3])

    # 添加价格线
    fig1.add_trace(
        go.Scatter(x=df['datetime'], y=df['latest_price'], name='价格',
                   line=dict(color='blue', width=1.5)),
        row=1, col=1
    )

    # 添加VWAP线
    fig1.add_trace(
        go.Scatter(x=df['datetime'], y=df['vwap'], name='VWAP',
                   line=dict(color='purple', width=1, dash='dot')),
        row=1, col=1
    )

    # 添加入场出场点位
    if entry_exit_points:
        # 将入场出场点转为DataFrame
        entry_exit_df = pd.DataFrame(list(entry_exit_points))
        entry_exit_df['datetime'] = pd.to_datetime(entry_exit_df['timestamp'], unit='ms')

        # 添加多头入场点
        long_entries = entry_exit_df[entry_exit_df['type'] == 'ENTRY_LONG']
        if not long_entries.empty:
            fig1.add_trace(
                go.Scatter(
                    x=long_entries['datetime'],
                    y=long_entries['price'],
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=12, color='green'),
                    name='多头入场',
                    hovertemplate='多头入场<br>价格: %{y}<br>时间: %{x}<br>原因: %{text}',
                    text=long_entries['reason']
                ),
                row=1, col=1
            )

        # 添加空头入场点
        short_entries = entry_exit_df[entry_exit_df['type'] == 'ENTRY_SHORT']
        if not short_entries.empty:
            fig1.add_trace(
                go.Scatter(
                    x=short_entries['datetime'],
                    y=short_entries['price'],
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=12, color='red'),
                    name='空头入场',
                    hovertemplate='空头入场<br>价格: %{y}<br>时间: %{x}<br>原因: %{text}',
                    text=short_entries['reason']
                ),
                row=1, col=1
            )

        # 添加多头出场点
        long_exits = entry_exit_df[entry_exit_df['type'] == 'EXIT_LONG']
        if not long_exits.empty:
            fig1.add_trace(
                go.Scatter(
                    x=long_exits['datetime'],
                    y=long_exits['price'],
                    mode='markers',
                    marker=dict(symbol='circle', size=10, color='green', line=dict(width=2, color='white')),
                    name='多头出场',
                    hovertemplate='多头出场<br>价格: %{y}<br>时间: %{x}<br>原因: %{text}',
                    text=long_exits['reason']
                ),
                row=1, col=1
            )

        # 添加空头出场点
        short_exits = entry_exit_df[entry_exit_df['type'] == 'EXIT_SHORT']
        if not short_exits.empty:
            fig1.add_trace(
                go.Scatter(
                    x=short_exits['datetime'],
                    y=short_exits['price'],
                    mode='markers',
                    marker=dict(symbol='circle', size=10, color='red', line=dict(width=2, color='white')),
                    name='空头出场',
                    hovertemplate='空头出场<br>价格: %{y}<br>时间: %{x}<br>原因: %{text}',
                    text=short_exits['reason']
                ),
                row=1, col=1
            )

    # 添加Delta柱状图
    fig1.add_trace(
        go.Bar(x=df['datetime'], y=df['delta'], name='Delta',
               marker=dict(color=np.where(df['delta'] >= 0, 'green', 'red'))),
        row=2, col=1
    )

    # 更新图表布局
    fig1.update_layout(
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=40, b=40),
        hovermode="closest"
    )

    # 创建订单簿压力图表
    fig2 = make_subplots(rows=2, cols=1,
                         shared_xaxes=True,
                         vertical_spacing=0.1,
                         subplot_titles=('买卖压力比', '前3档压力比'),
                         row_heights=[0.5, 0.5])

    # 添加买卖压力比线
    fig2.add_trace(
        go.Scatter(x=df['datetime'], y=df['bid_ask_ratio'], name='买卖压力比',
                   line=dict(color='blue')),
        row=1, col=1
    )

    # 添加平衡线
    fig2.add_trace(
        go.Scatter(x=df['datetime'], y=[1] * len(df), name='平衡线',
                   line=dict(color='black', width=1, dash='dash')),
        row=1, col=1
    )

    # 添加前3档压力比
    fig2.add_trace(
        go.Scatter(x=df['datetime'], y=df['top_pressure_ratio'], name='前3档压力比',
                   line=dict(color='orange')),
        row=2, col=1
    )

    # 添加平衡线
    fig2.add_trace(
        go.Scatter(x=df['datetime'], y=[1] * len(df), name='平衡线',
                   line=dict(color='black', width=1, dash='dash')),
        row=2, col=1
    )

    # 更新图表布局
    fig2.update_layout(
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=40, b=40),
    )

    # 创建OFI图表
    fig3 = go.Figure()

    if 'order_flow_imbalance' in df.columns:
        # 添加OFI线
        fig3.add_trace(
            go.Scatter(
                x=df['datetime'],
                y=df['order_flow_imbalance'],
                name='订单流失衡',
                line=dict(color='blue', width=1.5)
            )
        )

        # 添加零线
        fig3.add_trace(
            go.Scatter(
                x=df['datetime'],
                y=[0] * len(df),
                name='零线',
                line=dict(color='black', width=1, dash='dash')
            )
        )

        # 添加背景色区分正负区域
        fig3.add_trace(
            go.Scatter(
                x=df['datetime'],
                y=[5] * len(df),
                mode='lines',
                line=dict(width=0),
                fillcolor='rgba(0, 255, 0, 0.1)',
                fill='tonexty',
                name='买方主导区'
            )
        )

        fig3.add_trace(
            go.Scatter(
                x=df['datetime'],
                y=[-5] * len(df),
                mode='lines',
                line=dict(width=0),
                fillcolor='rgba(255, 0, 0, 0.1)',
                fill='tonexty',
                name='卖方主导区'
            )
        )

    # 更新OFI图表布局
    fig3.update_layout(
        title='订单流失衡指标 (OFI)',
        height=300,
        yaxis_title='失衡度',
        xaxis_title='时间',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=60, b=40),
    )

    # 创建资金曲线图
    fig4 = go.Figure()

    if pnl_history:
        # 转换为DataFrame
        pnl_df = pd.DataFrame(list(pnl_history))
        pnl_df['datetime'] = pd.to_datetime(pnl_df['timestamp'], unit='ms')

        # 添加权益曲线
        fig4.add_trace(
            go.Scatter(
                x=pnl_df['datetime'],
                y=pnl_df['total_equity'],
                name='总权益',
                line=dict(color='green', width=2)
            )
        )

        # 添加账户余额曲线
        fig4.add_trace(
            go.Scatter(
                x=pnl_df['datetime'],
                y=pnl_df['balance'],
                name='账户余额',
                line=dict(color='blue', width=2, dash='dash')
            )
        )

        # 添加初始资金参考线
        fig4.add_trace(
            go.Scatter(
                x=pnl_df['datetime'],
                y=[INITIAL_BALANCE] * len(pnl_df),
                name='初始资金',
                line=dict(color='gray', width=1, dash='dot')
            )
        )

        # 标记盈亏区域
        fig4.add_trace(
            go.Scatter(
                x=pnl_df['datetime'],
                y=[INITIAL_BALANCE] * len(pnl_df),
                mode='lines',
                line=dict(width=0),
                showlegend=False,
                hoverinfo='none'
            )
        )

        fig4.add_trace(
            go.Scatter(
                x=pnl_df['datetime'],
                y=pnl_df['total_equity'],
                mode='lines',
                line=dict(width=0),
                fillcolor='rgba(0, 255, 0, 0.1)',
                fill='tonexty',
                showlegend=False,
                hoverinfo='none'
            )
        )

    # 更新图表布局
    fig4.update_layout(
        title='模拟交易资金曲线',
        height=350,
        yaxis_title='USDT',
        xaxis_title='时间',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified"
    )

    # 生成信号表格
    if not signals:
        signals_table = html.P("暂无交易信号")
    else:
        # 创建信号卡片
        signals_list = []
        for signal in list(signals)[-5:]:  # 只显示最近5个信号
            signal_time = datetime.datetime.fromtimestamp(signal['timestamp'] / 1000).strftime('%H:%M:%S')
            direction_class = 'signal-buy' if signal['direction'] == '做多' else 'signal-sell'
            signals_list.append(
                html.Div([
                    html.Div([
                        html.Span(f"{signal_time}", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                        html.Span(signal['type'], style={'fontWeight': 'bold'})
                    ]),
                    html.Div([
                        html.Span("方向: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                        html.Span(signal['direction'],
                                  style={'color': 'green' if signal['direction'] == '做多' else 'red'})
                    ]),
                    html.Div([
                        html.Span("强度: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                        html.Span(f"{signal['strength']:.2f}")
                    ]),
                    html.Div(signal['message'])
                ], className=f'signal-card {direction_class}')
            )

        signals_table = html.Div(signals_list, style={'maxHeight': '300px', 'overflow': 'auto'})

    # 生成交易决策显示
    if not trade_decisions:
        trade_decisions_display = html.P("暂无交易决策")
    else:
        # 创建决策卡片
        decisions_list = []
        for decision in list(trade_decisions)[-5:]:  # 只显示最近5个决策
            decision_time = datetime.datetime.fromtimestamp(decision['timestamp'] / 1000).strftime('%H:%M:%S')

            if decision['decision'] == 'BUY':
                decision_class = 'decision-buy'
                decision_text = '买入'
            elif decision['decision'] == 'SELL':
                decision_class = 'decision-sell'
                decision_text = '卖出'
            else:
                decision_class = 'decision-hold'
                decision_text = '观望'

            content = [
                html.Div([
                    html.Span(f"{decision_time}", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                    html.Span(decision_text, style={'fontWeight': 'bold', 'fontSize': '16px'})
                ]),
                html.Div([
                    html.Span("价格: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{decision['price']:.2f}")
                ])
            ]

            # 添加置信度(如果存在)
            if 'confidence' in decision and decision['decision'] != 'HOLD':
                content.append(html.Div([
                    html.Span("置信度: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{decision['confidence']:.2f}")
                ]))

            # 添加建议仓位大小(如果存在)
            if 'suggested_size' in decision and decision['decision'] != 'HOLD':
                content.append(html.Div([
                    html.Span("建议仓位: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{decision['suggested_size']:.2f}x")
                ]))

            decisions_list.append(html.Div(content, className=f'decision-card {decision_class}'))

        trade_decisions_display = html.Div(decisions_list, style={'maxHeight': '300px', 'overflow': 'auto'})

    # 生成交易历史表格
    if not trade_history:
        trade_history_table = html.P("暂无交易记录")
    else:
        # 创建交易历史表格
        trades_list = []
        for trade in list(trade_history)[-10:]:  # 只显示最近10笔交易
            trade_time = datetime.datetime.fromtimestamp(trade['timestamp'] / 1000).strftime('%H:%M:%S')

            # 设置样式
            if trade['type'] in ['OPEN_LONG', 'CLOSE_LONG']:
                trade_class = 'signal-buy'
            else:
                trade_class = 'signal-sell'

            # 准备内容
            content = [
                html.Div([
                    html.Span(f"{trade_time}", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                    html.Span(trade['type'].replace('_', ' '), style={'fontWeight': 'bold'})
                ])
            ]

            # 添加价格信息
            if 'price' in trade:
                content.append(html.Div([
                    html.Span("价格: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{trade['price']:.2f}")
                ]))

            # 添加数量信息
            if 'quantity' in trade:
                content.append(html.Div([
                    html.Span("数量: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{trade['quantity']:.4f}")
                ]))

            # 添加利润信息(如果有)
            if 'profit' in trade:
                profit_color = 'green' if trade['profit'] >= 0 else 'red'
                content.append(html.Div([
                    html.Span("利润: ", style={'fontWeight': 'bold', 'marginRight': '5px'}),
                    html.Span(f"{trade['profit']:.2f} USDT", style={'color': profit_color})
                ]))

            trades_list.append(html.Div(content, className=f'signal-card {trade_class}'))

        trade_history_table = html.Div(trades_list, style={'maxHeight': '300px', 'overflow': 'auto'})

    return fig1, fig2, fig3, fig4, signals_table, trade_decisions_display, trade_history_table  # orderflow_dashboard_enhanced.py - Kraken兼容版本


import json
import time
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import threading
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import datetime
import collections

# 导入高级订单流分析模块
try:
    from advanced_orderflow import (
        detect_iceberg_orders,
        detect_dark_pool_activity,
        detect_hft_patterns,
        WhaleTracker,
        generate_trade_decision,  # 新增：交易决策生成函数
        calculate_order_flow_imbalance,  # 新增：订单流失衡指标
        calculate_book_pressure,  # 新增：订单簿压力率
        calculate_adl,  # 新增：积累分布线
        analyze_trade_rhythm  # 新增：交易节奏分析
    )
    from depth_format_adapter import (
        fix_detect_hft_patterns,
        adapt_depth_changes_for_hft
    )

    format_adapter_enabled = True
    advanced_analysis_enabled = True
except ImportError:
    advanced_analysis_enabled = False
    print("高级订单流分析模块未找到，将禁用高级分析功能")
    format_adapter_enabled = False
    print("深度数据格式适配器未找到，高级分析可能失败")

# 配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DEPTH_TOPIC = 'depth'
TRADES_TOPIC = 'trades'
TICKER_TOPIC = 'ticker'  # 新增Ticker主题

# 数据存储
depth_data = collections.deque(maxlen=1000)  # 保存1000条深度数据
trade_data = collections.deque(maxlen=10000)  # 保存10000条成交数据
ticker_data = collections.deque(maxlen=1000)  # 新增：保存1000条行情数据
calculated_metrics = collections.deque(maxlen=500)  # 计算得到的指标

# 信号记录
signals = collections.deque(maxlen=100)  # 最近的100个信号
trade_decisions = collections.deque(maxlen=100)  # 新增：交易决策记录
entry_exit_points = collections.deque(maxlen=50)  # 新增：入场出场点位记录

# 模拟交易数据
INITIAL_BALANCE = 10000  # 初始资金10000 USDT
current_balance = INITIAL_BALANCE  # 当前资金
current_position = 0  # 当前仓位，正数为多头，负数为空头
current_position_price = 0  # 当前持仓价格
trade_history = collections.deque(maxlen=100)  # 交易历史
pnl_history = collections.deque(maxlen=500)  # 收益历史

# 初始化大玩家跟踪器
if advanced_analysis_enabled:
    whale_tracker = WhaleTracker(volume_threshold=5.0)  # 5 BTC作为门槛

# 保存订单簿变化历史
depth_changes = collections.deque(maxlen=1000)  # 订单簿变化历史

# 记录最后一次收到的价格，用于填充数据缺失
last_price = None


# Kafka消费者线程 - 深度数据
def consume_depth_data():
    consumer = KafkaConsumer(
        DEPTH_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    prev_depth = None
    for message in consumer:
        data = message.value

        # 添加本地接收时间（如果没有）
        if 'local_timestamp' not in data:
            data['local_timestamp'] = int(time.time() * 1000)

        # 标准化Kraken的深度数据格式
        data = normalize_depth_format(data)

        depth_data.append(data)

        # 计算订单簿变化
        if prev_depth:
            change = calculate_depth_change(prev_depth, data)
            change['timestamp'] = data['local_timestamp']
            depth_changes.append(change)

        prev_depth = data
        calculate_metrics()


# 标准化Kraken的深度数据格式，使其与Binance格式兼容
def normalize_depth_format(data):
    """将Kraken的深度数据格式转换为与Binance兼容的格式"""
    # 如果已经是标准格式，直接返回
    if 'bids' in data and 'asks' in data and isinstance(data['bids'], list) and isinstance(data['asks'], list):
        # 确保每个订单格式为[price, quantity]
        if data['bids'] and not isinstance(data['bids'][0], list):
            data['bids'] = [[str(item['price']), str(item['qty'])] for item in data['bids']]
        if data['asks'] and not isinstance(data['asks'][0], list):
            data['asks'] = [[str(item['price']), str(item['qty'])] for item in data['asks']]
        return data

    # 创建标准格式
    normalized = {
        'lastUpdateId': data.get('lastUpdateId', int(time.time() * 1000)),
        'bids': [],
        'asks': [],
        'local_timestamp': data.get('local_timestamp', int(time.time() * 1000)),
        'pair': data.get('pair', data.get('symbol', 'BTC/USD'))
    }

    # 处理Kraken V2 API的深度数据格式
    if 'bids' in data and isinstance(data['bids'], list):
        for bid in data['bids']:
            if isinstance(bid, dict) and 'price' in bid and 'qty' in bid:
                normalized['bids'].append([str(bid['price']), str(bid['qty'])])
            elif isinstance(bid, list) and len(bid) >= 2:
                normalized['bids'].append([str(bid[0]), str(bid[1])])

    if 'asks' in data and isinstance(data['asks'], list):
        for ask in data['asks']:
            if isinstance(ask, dict) and 'price' in ask and 'qty' in ask:
                normalized['asks'].append([str(ask['price']), str(ask['qty'])])
            elif isinstance(ask, list) and len(ask) >= 2:
                normalized['asks'].append([str(ask[0]), str(ask[1])])

    return normalized


# Kafka消费者线程 - 成交数据
def consume_trade_data():
    consumer = KafkaConsumer(
        TRADES_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value

        # 添加本地接收时间（如果没有）
        if 'local_timestamp' not in data:
            data['local_timestamp'] = int(time.time() * 1000)

        # 标准化Kraken的成交数据格式
        data = normalize_trade_format(data)

        # 更新最新价格
        global last_price
        if 'p' in data:
            last_price = float(data['p'])

        trade_data.append(data)


# 标准化Kraken的成交数据格式，使其与Binance格式兼容
def normalize_trade_format(data):
    """将Kraken的成交数据格式转换为与Binance兼容的格式"""
    # 如果已经是标准格式，直接返回
    if all(k in data for k in ['e', 'p', 'q', 'm']):
        return data

    # 创建标准格式
    normalized = {
        'e': data.get('e', 'trade'),  # 事件类型
        'E': data.get('E', int(time.time() * 1000)),  # 事件时间
        's': data.get('s', data.get('symbol', 'BTC/USD')),  # 交易对
        't': data.get('t', int(time.time() * 1000000)),  # 交易ID
        'p': data.get('p', str(data.get('price', 0))),  # 价格
        'q': data.get('q', str(data.get('qty', 0))),  # 数量
        'b': data.get('b', 0),  # 买方订单ID
        'a': data.get('a', 0),  # 卖方订单ID
        'T': data.get('T', int(time.time() * 1000)),  # 交易时间
        'm': data.get('m', False),  # 是否是卖方发起的交易
        'M': data.get('M', False),  # 是否是最佳价格匹配
        'local_timestamp': data.get('local_timestamp', int(time.time() * 1000))
    }

    return normalized


# 新增：Kafka消费者线程 - Ticker数据
def consume_ticker_data():
    consumer = KafkaConsumer(
        TICKER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value

        # 添加本地接收时间（如果没有）
        if 'timestamp' not in data:
            data['timestamp'] = int(time.time() * 1000)

        # 更新最新价格
        global last_price
        if 'last_price' in data:
            last_price = float(data['last_price'])

        ticker_data.append(data)


# 计算深度数据变化
def calculate_depth_change(prev, curr):
    """计算两个深度快照之间的变化"""
    # 提取价格作为索引
    prev_bids = {float(p[0]): float(p[1]) for p in prev['bids']}
    prev_asks = {float(p[0]): float(p[1]) for p in prev['asks']}
    curr_bids = {float(p[0]): float(p[1]) for p in curr['bids']}
    curr_asks = {float(p[0]): float(p[1]) for p in curr['asks']}

    # 计算添加和移除的量
    bid_added = sum(
        max(0, curr_bids.get(price, 0) - prev_bids.get(price, 0)) for price in set(curr_bids) | set(prev_bids))
    bid_removed = sum(
        max(0, prev_bids.get(price, 0) - curr_bids.get(price, 0)) for price in set(curr_bids) | set(prev_bids))
    ask_added = sum(
        max(0, curr_asks.get(price, 0) - prev_asks.get(price, 0)) for price in set(curr_asks) | set(prev_asks))
    ask_removed = sum(
        max(0, prev_asks.get(price, 0) - curr_asks.get(price, 0)) for price in set(curr_asks) | set(prev_asks))

    return {
        'bids': curr_bids,
        'asks': curr_asks,
        'bid_added': bid_added,
        'bid_removed': bid_removed,
        'ask_added': ask_added,
        'ask_removed': ask_removed
    }


# 计算各种指标
def calculate_metrics():
    if not depth_data:
        return

    # 获取最新的深度数据
    latest_depth = depth_data[-1]

    # 计算买卖压力比率
    total_bid_volume = sum(float(level[1]) for level in latest_depth['bids'])
    total_ask_volume = sum(float(level[1]) for level in latest_depth['asks'])

    # 防止除零
    if total_ask_volume == 0 or total_bid_volume == 0:
        bid_ask_ratio = 1.0
    else:
        bid_ask_ratio = total_bid_volume / total_ask_volume

    # 计算前3档的买卖压力
    top_bid_volume = sum(float(level[1]) for level in latest_depth['bids'][:3])
    top_ask_volume = sum(float(level[1]) for level in latest_depth['asks'][:3])

    # 防止除零
    if top_ask_volume == 0 or top_bid_volume == 0:
        top_pressure_ratio = 1.0
    else:
        top_pressure_ratio = top_bid_volume / top_ask_volume

    # 获取最近10秒的成交数据 - 创建一个副本再遍历
    recent_time = int(time.time() * 1000) - 10000
    trade_data_copy = list(trade_data)  # 创建副本
    recent_trades = [t for t in trade_data_copy if t.get('local_timestamp', 0) > recent_time]

    # 计算Delta (买卖成交量差)
    buy_volume = sum(float(t['q']) for t in recent_trades if not t.get('m', True))
    sell_volume = sum(float(t['q']) for t in recent_trades if t.get('m', False))
    delta = buy_volume - sell_volume

    # 计算成交量加权平均价格
    if recent_trades:
        vwap_num = sum(float(t['p']) * float(t['q']) for t in recent_trades)
        vwap_den = sum(float(t['q']) for t in recent_trades)
        vwap = vwap_num / vwap_den if vwap_den > 0 else 0
    else:
        vwap = 0

    # 获取当前最新价格
    global last_price
    latest_price = 0

    # 优先使用最近的成交价格
    if recent_trades:
        latest_price = float(recent_trades[-1]['p'])
    # 如果没有最近成交，使用ticker数据
    elif ticker_data:
        latest_price = float(ticker_data[-1].get('last_price', 0))
    # 如果都没有，使用上次记录的价格
    elif last_price is not None:
        latest_price = last_price

    # 计算订单流失衡指标
    order_flow_imbalance = 0
    if advanced_analysis_enabled and len(depth_changes) > 0:
        order_flow_imbalance = calculate_order_flow_imbalance(list(depth_changes))

    # 计算订单簿压力率
    book_pressure = 1.0
    if advanced_analysis_enabled:
        book_pressure = calculate_book_pressure(latest_depth)

    # 创建指标对象
    metrics = {
        'timestamp': int(time.time() * 1000),
        'latest_price': latest_price,
        'bid_ask_ratio': bid_ask_ratio,
        'top_pressure_ratio': top_pressure_ratio,
        'delta': delta,
        'vwap': vwap,
        'buy_volume': buy_volume,
        'sell_volume': sell_volume,
        'order_flow_imbalance': order_flow_imbalance,  # 新增
        'book_pressure': book_pressure  # 新增
    }

    # 添加到指标列表
    calculated_metrics.append(metrics)

    # 生成信号
    generate_signals()

    # 生成交易决策
    if advanced_analysis_enabled and len(calculated_metrics) > 10:
        decision = generate_trade_decision(metrics, list(signals))

        # 记录决策
        decision['timestamp'] = metrics['timestamp']
        decision['price'] = latest_price
        trade_decisions.append(decision)

        # 生成入场出场信号
        process_entry_exit_signals(decision, metrics)


# 处理入场出场信号，并执行模拟交易
def process_entry_exit_signals(decision, metrics):
    """根据交易决策生成入场出场信号，并执行模拟交易"""
    global current_balance, current_position, current_position_price

    # 调试输出
    print(f"处理交易决策: {decision['decision']}, 置信度: {decision.get('confidence', 0)}")

    current_time = metrics['timestamp']
    current_price = metrics['latest_price']

    # 初始化交易执行标志
    trade_executed = False
    trade_info = {}

    # 放宽交易阈值 - 降低置信度要求
    if decision['decision'] in ['BUY', 'SELL'] and decision.get('confidence', 0) > 0.3:  # 从0.6降至0.3
        print(f"交易条件满足! 决策: {decision['decision']}")

        # 简化交易逻辑 - 直接执行交易而不检查上一笔交易
        if decision['decision'] == 'BUY':
            # 如果持有空头仓位，先平仓
            if current_position < 0:
                trade_profit = (current_position_price - current_price) * abs(current_position)
                current_balance += trade_profit
                trade_history.append({
                    'type': 'CLOSE_SHORT',
                    'entry_price': current_position_price,
                    'exit_price': current_price,
                    'quantity': abs(current_position),
                    'profit': trade_profit,
                    'timestamp': current_time
                })
                print(f"平空仓: 利润 {trade_profit:.2f}")
                current_position = 0

            # 开多仓
            position_size = decision.get('suggested_size', 0.5)  # 默认仓位50%
            trade_amount = current_balance * position_size / current_price
            current_position = trade_amount
            current_position_price = current_price

            trade_history.append({
                'type': 'OPEN_LONG',
                'price': current_price,
                'quantity': trade_amount,
                'balance': current_balance,
                'timestamp': current_time
            })
            print(f"开多仓: 价格 {current_price:.2f}, 数量 {trade_amount:.4f}")

        else:  # SELL
            # 如果持有多头仓位，先平仓
            if current_position > 0:
                trade_profit = (current_price - current_position_price) * current_position
                current_balance += trade_profit
                trade_history.append({
                    'type': 'CLOSE_LONG',
                    'entry_price': current_position_price,
                    'exit_price': current_price,
                    'quantity': current_position,
                    'profit': trade_profit,
                    'timestamp': current_time
                })
                print(f"平多仓: 利润 {trade_profit:.2f}")
                current_position = 0

            # 开空仓
            position_size = decision.get('suggested_size', 0.5)  # 默认仓位50%
            trade_amount = current_balance * position_size / current_price
            current_position = -trade_amount
            current_position_price = current_price

            trade_history.append({
                'type': 'OPEN_SHORT',
                'price': current_price,
                'quantity': trade_amount,
                'balance': current_balance,
                'timestamp': current_time
            })
            print(f"开空仓: 价格 {current_price:.2f}, 数量 {trade_amount:.4f}")

    # 更新资金曲线
    unrealized_pnl = 0
    if current_position > 0:  # 多头仓位
        unrealized_pnl = (current_price - current_position_price) * current_position
    elif current_position < 0:  # 空头仓位
        unrealized_pnl = (current_position_price - current_price) * abs(current_position)

    total_equity = current_balance + unrealized_pnl

    print(f"账户更新: 余额={current_balance:.2f}, 持仓={current_position:.4f}, 总权益={total_equity:.2f}")

    pnl_history.append({
        'timestamp': current_time,
        'balance': current_balance,
        'unrealized_pnl': unrealized_pnl,
        'total_equity': total_equity,
        'price': current_price,
        'position': current_position
    })


# 生成交易信号
def generate_signals():
    if len(calculated_metrics) < 10:
        return

    # 转换为DataFrame便于分析
    df = pd.DataFrame(list(calculated_metrics))

    # 信号1: Delta分歧 - 价格走势与Delta趋势不一致
    if len(df) >= 10:
        # 计算最近的价格趋势和Delta趋势
        price_change = df['latest_price'].diff(5).iloc[-1]
        delta_sum = df['delta'].rolling(5).sum().iloc[-1]

        # 价格上涨但Delta为负
        if price_change > 0 and delta_sum < -0.1:
            signals.append({
                'timestamp': int(time.time() * 1000),
                'type': 'Delta背离',
                'direction': '做空',
                'strength': abs(delta_sum) / df['buy_volume'].rolling(5).mean().iloc[-1] if
                df['buy_volume'].rolling(5).mean().iloc[-1] > 0 else 0,
                'message': 'Delta背离: 价格上涨但买方力量减弱'
            })

        # 价格下跌但Delta为正
        elif price_change < 0 and delta_sum > 0.1:
            signals.append({
                'timestamp': int(time.time() * 1000),
                'type': 'Delta背离',
                'direction': '做多',
                'strength': abs(delta_sum) / df['sell_volume'].rolling(5).mean().iloc[-1] if
                df['sell_volume'].rolling(5).mean().iloc[-1] > 0 else 0,
                'message': 'Delta背离: 价格下跌但买方力量增强'
            })

    # 信号2: 订单簿压力变化
    if len(df) >= 5:
        pressure_change = df['top_pressure_ratio'].diff(3).iloc[-1]
        if abs(pressure_change) > 0.2:  # 阈值可调整
            signals.append({
                'timestamp': int(time.time() * 1000),
                'type': '订单压力突变',
                'direction': '做多' if pressure_change > 0 else '做空',
                'strength': abs(pressure_change) * 5,
                'message': f'订单压力{"增加" if pressure_change > 0 else "减少"}: {abs(pressure_change):.2f}'
            })

    # 信号3: 订单流失衡
    if 'order_flow_imbalance' in df.columns and len(df) >= 5:
        ofi_mean = df['order_flow_imbalance'].rolling(5).mean().iloc[-1]
        if abs(ofi_mean) > 5:  # 阈值可调整
            signals.append({
                'timestamp': int(time.time() * 1000),
                'type': '订单流失衡',
                'direction': '做多' if ofi_mean > 0 else '做空',
                'strength': min(1.0, abs(ofi_mean) / 20),
                'message': f'订单流失衡: {"买方" if ofi_mean > 0 else "卖方"}主导, 幅度: {abs(ofi_mean):.2f}'
            })


# 创建Dash应用
app = dash.Dash(__name__)

# 自定义CSS样式
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>比特币订单流分析仪表盘 (Kraken)</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f5f5f5;
            }
            .dashboard-container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 15px;
            }
            .dashboard-title {
                color: #2c3e50;
                text-align: center;
                margin-bottom: 20px;
            }
            .card {
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                padding: 15px;
                margin-bottom: 20px;
            }
            .signal-card {
                border-left: 4px solid;
                margin: 8px 0;
                padding: 8px 12px;
                background-color: #f9f9f9;
                border-radius: 4px;
            }
            .signal-buy {
                border-left-color: #27ae60;
            }
            .signal-sell {
                border-left-color: #e74c3c;
            }
            .signal-neutral {
                border-left-color: #3498db;
            }
            .decision-card {
                border-left: 4px solid;
                margin: 8px 0;
                padding: 8px 12px;
                border-radius: 4px;
                color: white;
            }
            .decision-buy {
                background-color: rgba(39, 174, 96, 0.8);
                border-left-color: #27ae60;
            }
            .decision-sell {
                background-color: rgba(231, 76, 60, 0.8);
                border-left-color: #e74c3c;
            }
            .decision-hold {
                background-color: rgba(52, 152, 219, 0.8);
                border-left-color: #3498db;
            }
            .analysis-panel {
                margin-bottom: 25px;
                background-color: white;
                border-radius: 8px;
                padding: 15px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# 构建应用布局
tabs_children = [
    # 基本分析选项卡
    dcc.Tab(label='基本分析', children=[
        html.Div([
            # 主图表 - 价格和Delta指标
            html.Div([
                dcc.Graph(id='main-chart', style={'height': '500px'})
            ], className='card'),

            # 信号和交易决策展示
            html.Div([
                html.Div([
                    html.H3("交易信号与决策", style={'textAlign': 'center', 'margin-bottom': '15px'}),
                    html.Div([
                        html.Div([
                            html.H4("最新信号", style={'marginTop': '0'}),
                            html.Div(id='signals-table')
                        ], style={'flex': '1', 'marginRight': '15px'}),
                        html.Div([
                            html.H4("交易决策", style={'marginTop': '0'}),
                            html.Div(id='trade-decisions')
                        ], style={'flex': '1'})
                    ], style={'display': 'flex'})
                ], className='card')
            ]),

            # 订单簿压力图表
            html.Div([
                dcc.Graph(id='orderbook-chart', style={'height': '400px'})
            ], className='card'),

            # OFI(Order Flow Imbalance)图表
            html.Div([
                dcc.Graph(id='ofi-chart', style={'height': '300px'})
            ], className='card'),

            # 模拟交易资金曲线图
            html.Div([
                dcc.Graph(id='equity-chart', style={'height': '350px'})
            ], className='card'),

            # 交易历史记录
            html.Div([
                html.H3("交易历史记录", style={'textAlign': 'center', 'margin-bottom': '15px'}),
                html.Div(id='trade-history-table')
            ], className='card')
        ], className='dashboard-container')
    ])
]

# 如果启用了高级分析，添加高级分析选项卡
if advanced_analysis_enabled:
    advanced_tab = dcc.Tab(label='高级分析', children=[
        html.Div([
            # 冰山订单检测面板
            html.Div([
                html.H3('冰山订单检测', style={'textAlign': 'center'}),
                html.Div(id='iceberg-detection')
            ], className='analysis-panel'),

            # 暗池活动面板
            html.Div([
                html.H3('暗池活动指标', style={'textAlign': 'center'}),
                html.Div(id='darkpool-activity')
            ], className='analysis-panel'),

            # 高频交易模式面板
            html.Div([
                html.H3('高频交易活动', style={'textAlign': 'center'}),
                html.Div(id='hft-activity')
            ], className='analysis-panel'),

            # 大玩家跟踪面板
            html.Div([
                html.H3('大玩家跟踪', style={'textAlign': 'center'}),
                html.Div(id='whale-tracking')
            ], className='analysis-panel'),
        ], className='dashboard-container')
    ])
    tabs_children.append(advanced_tab)

app.layout = html.Div([
    html.H1("比特币订单流分析仪表盘 (Kraken)", className='dashboard-title'),

    # 刷新间隔
    dcc.Interval(
        id='interval-component',
        interval=1000,  # 每秒更新一次
        n_intervals=0
    ),

    # 创建选项卡布局
    dcc.Tabs(tabs_children)
])