# advanced_orderflow.py
import numpy as np
import pandas as pd
from collections import Counter




# 冰山订单检测
def detect_iceberg_orders(depth_data, trades_data, time_window=60):
    """
    识别可能的冰山订单
    
    参数:
    - depth_data: 订单簿快照历史
    - trades_data: 逐笔成交历史
    - time_window: 观察窗口（秒）
    
    返回:
    - 冰山订单候选列表
    """
    iceberg_candidates = []
    
    # 转换为DataFrame以便分析
    trades_df = pd.DataFrame(trades_data)
    
    # 按价格级别分组成交
    price_levels = trades_df.groupby('p')
    
    for price, group in price_levels:
        # 特征1: 在同一价格点有反复成交
        if len(group) < 3:
            continue
            
        # 特征2: 成交量大致相等 (冰山订单通常被切分为等大小块)
        volumes = group['q'].astype(float)
        vol_std = volumes.std()
        vol_mean = volumes.mean()
        if vol_std / vol_mean > 0.3:  # 允许30%的波动
            continue
            
        # 特征3: 在相同价格的挂单量几乎不变或快速恢复
        # 找到这个价格对应的订单簿数据
        level_depths = []
        for depth in depth_data:
            # 在买单中查找
            for bid in depth['bids']:
                if float(bid[0]) == float(price):
                    level_depths.append(float(bid[1]))
                    break
            # 在卖单中查找
            for ask in depth['asks']:
                if float(ask[0]) == float(price):
                    level_depths.append(float(ask[1]))
                    break
        
        if level_depths:
            depth_stability = np.std(level_depths) / np.mean(level_depths) if np.mean(level_depths) > 0 else float('inf')
            # 稳定或有规律地恢复
            if depth_stability < 0.2 or has_pattern(level_depths):
                iceberg_candidates.append({
                    'price': price,
                    'avg_volume': vol_mean,
                    'count': len(group),
                    'side': 'buy' if group['m'].mode()[0] else 'sell',
                    'confidence': calculate_iceberg_confidence(vol_std, depth_stability)
                })
    
    return iceberg_candidates


def calculate_order_flow_imbalance(depth_changes, window=10):
    """计算订单流失衡指标"""
    if len(depth_changes) < window:
        return 0

    changes = depth_changes[-window:]
    bid_delta = sum([c['bid_added'] - c['bid_removed'] for c in changes])
    ask_delta = sum([c['ask_added'] - c['ask_removed'] for c in changes])

    imbalance = bid_delta - ask_delta
    return imbalance


def calculate_book_pressure(depth_data, levels=5):
    """计算订单簿压力率，通过加权不同层级的订单量"""
    if not depth_data:
        return 1.0

    # 加权系数，越靠近中间权重越高
    weights = [1.0 / (i + 1) for i in range(levels)]

    # 加权计算买卖压力
    bid_pressure = sum(float(depth_data['bids'][i][1]) * weights[i]
                       for i in range(min(levels, len(depth_data['bids']))))
    ask_pressure = sum(float(depth_data['asks'][i][1]) * weights[i]
                       for i in range(min(levels, len(depth_data['asks']))))

    if ask_pressure == 0:
        return float('inf')
    return bid_pressure / ask_pressure


def calculate_adl(trades_df, window=100):
    """计算积累分布线，评估买卖压力"""
    if trades_df.empty or len(trades_df) < 2:
        return []

    trades_df = trades_df.sort_values('T')
    trades_df['price'] = pd.to_numeric(trades_df['p'])
    trades_df['volume'] = pd.to_numeric(trades_df['q'])

    # 计算多空资金流量系数
    high = trades_df['price'].rolling(window).max()
    low = trades_df['price'].rolling(window).min()
    close = trades_df['price']

    mfm = ((close - low) - (high - close)) / (high - low)
    mfm = mfm.fillna(0)  # 处理可能的除零情况

    # 计算资金流量
    mfv = mfm * trades_df['volume']

    # 计算ADL
    adl = mfv.cumsum()

    return adl.tolist()


def analyze_trade_rhythm(trades_df, window=100):
    """分析交易节奏变化"""
    if trades_df.empty or len(trades_df) < window:
        return {"acceleration": 0, "pattern_score": 0}

    trades_df = trades_df.sort_values('T')
    trades_df['timestamp'] = pd.to_numeric(trades_df['T'])

    # 计算交易间隔
    intervals = trades_df['timestamp'].diff().dropna()

    # 计算加速度 (交易频率变化)
    recent = intervals[-window // 2:].mean()
    previous = intervals[-window:-window // 2].mean()

    if previous == 0:
        acceleration = 0
    else:
        acceleration = (previous - recent) / previous  # 正值表示加速

    # 寻找规律性模式 (使用自相关)
    if len(intervals) >= window:
        pattern_score = intervals.autocorr(lag=1)  # 一阶自相关
    else:
        pattern_score = 0

    return {
        "acceleration": acceleration,
        "pattern_score": pattern_score,
        "avg_interval": intervals[-window:].mean()
    }


def detect_trade_clusters(trades_df, price_threshold=0.01, time_threshold=5000):
    """检测价格和时间上聚集的交易，这可能表示有意识的交易行为"""
    if trades_df.empty:
        return []

    trades_df = trades_df.sort_values('T')
    trades_df['price'] = pd.to_numeric(trades_df['p'])
    trades_df['timestamp'] = pd.to_numeric(trades_df['T'])

    clusters = []
    current_cluster = [0]  # 从第一个交易开始

    # 检测价格和时间都接近的交易聚集
    for i in range(1, len(trades_df)):
        price_diff = abs(trades_df['price'].iloc[i] - trades_df['price'].iloc[current_cluster[0]]) / \
                     trades_df['price'].iloc[current_cluster[0]]
        time_diff = trades_df['timestamp'].iloc[i] - trades_df['timestamp'].iloc[current_cluster[0]]

        if price_diff < price_threshold and time_diff < time_threshold:
            current_cluster.append(i)
        else:
            if len(current_cluster) >= 3:  # 至少3个交易形成群集
                clusters.append({
                    'start_idx': current_cluster[0],
                    'end_idx': current_cluster[-1],
                    'count': len(current_cluster),
                    'avg_price': trades_df['price'].iloc[current_cluster].mean(),
                    'total_volume': trades_df['q'].iloc[current_cluster].astype(float).sum()
                })
            current_cluster = [i]

    # 处理最后一个可能的群集
    if len(current_cluster) >= 3:
        clusters.append({
            'start_idx': current_cluster[0],
            'end_idx': current_cluster[-1],
            'count': len(current_cluster),
            'avg_price': trades_df['price'].iloc[current_cluster].mean(),
            'total_volume': trades_df['q'].iloc[current_cluster].astype(float).sum()
        })

    return clusters



def has_pattern(depths):
    """检测深度数据中的周期性模式"""
    # 简单实现：检查是否有重复的"跌落-恢复"模式
    if len(depths) < 3:
        return False
        
    diffs = np.diff(depths)
    neg_followed_by_pos = 0
    for i in range(len(diffs)-1):
        if diffs[i] < 0 and diffs[i+1] > 0:
            neg_followed_by_pos += 1
    
    return neg_followed_by_pos >= 2  # 至少有两次"跌落-恢复"

def calculate_iceberg_confidence(vol_std_ratio, depth_stability):
    """计算冰山订单的置信度"""
    # 简单加权平均
    stability_score = max(0, 1 - depth_stability)
    volume_consistency = max(0, 1 - vol_std_ratio)
    return 0.7 * stability_score + 0.3 * volume_consistency

# 暗池活动检测
def detect_dark_pool_activity(trades_df, depth_history, price_history, window=100):
    """
    检测可能的暗池交易活动
    
    参数:
    - trades_df: 逐笔成交DataFrame
    - depth_history: 订单簿历史
    - price_history: 价格历史
    - window: 观察窗口
    
    返回:
    - 检测到的可能暗池活动列表
    """
    dark_pool_signals = []
    
    # 1. 价格突然跳跃但公开市场成交量小
    price_jumps = detect_price_jumps(price_history)
    for jump in price_jumps:
        # 获取跳跃期间的公开市场成交
        time_range = (jump['start_time'], jump['end_time'])
        public_volume = get_volume_in_range(trades_df, time_range)
        
        # 如果公开成交量不足以解释价格跳跃
        if public_volume * jump['avg_price'] < jump['price_change'] * 0.5:
            dark_pool_signals.append({
                'type': 'price_jump',
                'time': jump['end_time'],
                'magnitude': jump['price_change'],
                'expected_volume': jump['price_change'] / jump['avg_price'],
                'actual_volume': public_volume,
                'confidence': calculate_confidence(jump, public_volume)
            })
    
    # 2. 流动性突然变化
    liquidity_shifts = detect_liquidity_shifts(depth_history)
    for shift in liquidity_shifts:
        # 检查是否有对应的公开大额交易
        if not has_matching_large_trades(trades_df, shift['time'], shift['magnitude']):
            dark_pool_signals.append({
                'type': 'liquidity_shift',
                'time': shift['time'],
                'side': shift['side'],
                'magnitude': shift['magnitude'],
                'confidence': shift['confidence']
            })
    
    return dark_pool_signals

def detect_price_jumps(price_history, threshold=0.01):
    jumps = []
    for i in range(1, len(price_history)):
        t1, p1 = price_history[i-1]['timestamp'], price_history[i-1]['price']
        t2, p2 = price_history[i]['timestamp'], price_history[i]['price']
        change_ratio = abs(p2 - p1) / p1
        if change_ratio > threshold:
            jumps.append({
                'start_time': t1,
                'end_time': t2,
                'price_change': p2 - p1,
                'avg_price': (p1 + p2) / 2
            })
    return jumps

def get_volume_in_range(trades_df, time_range):
    """获取时间范围内的成交量"""
    start_time, end_time = time_range
    in_range = trades_df[(trades_df['T'] >= start_time) & (trades_df['T'] <= end_time)]
    return in_range['q'].astype(float).sum()


def detect_liquidity_shifts(depth_history, threshold=0.5):
    """检测订单簿突然的挂单量变化"""
    shifts = []
    for i in range(1, len(depth_history)):
        prev = depth_history[i - 1]
        curr = depth_history[i]
        time = curr['local_timestamp']

        prev_bid_vol = sum(float(b[1]) for b in prev['bids'][:5])
        curr_bid_vol = sum(float(b[1]) for b in curr['bids'][:5])
        prev_ask_vol = sum(float(a[1]) for a in prev['asks'][:5])
        curr_ask_vol = sum(float(a[1]) for a in curr['asks'][:5])

        bid_drop = (prev_bid_vol - curr_bid_vol) / max(prev_bid_vol, 1)
        ask_drop = (prev_ask_vol - curr_ask_vol) / max(prev_ask_vol, 1)

        if bid_drop > threshold:
            shifts.append({
                'time': time,
                'side': 'buy',
                'magnitude': bid_drop,
                'confidence': round(min(1.0, bid_drop), 2)
            })
        elif ask_drop > threshold:
            shifts.append({
                'time': time,
                'side': 'sell',
                'magnitude': ask_drop,
                'confidence': round(min(1.0, ask_drop), 2)
            })
    return shifts


def has_matching_large_trades(trades_df, time, magnitude, window=2000):
    """判断某时点附近是否有对应的大成交解释挂单流动性变化"""
    t_start = time - window
    t_end = time + window
    df_window = trades_df[(trades_df['T'] >= t_start) & (trades_df['T'] <= t_end)]
    return df_window['q'].astype(float).sum() > magnitude * 10  # 如果成交量足够大，说明是正常市场行为


def calculate_confidence(jump, volume):
    """计算置信度"""
    # 简化版实现
    return 0.8

# 高频交易模式识别
def detect_hft_patterns(depth_changes, trades_df, time_window=5):
    """
    识别高频交易模式
    
    参数:
    - depth_changes: 订单簿变动序列
    - trades_df: 成交数据
    - time_window: 分析窗口(秒)
    
    返回:
    - 检测到的HFT模式
    """
    hft_patterns = []
    
    # 1. 闪电挂撤单模式 (挂单后极短时间内撤销)
    flash_orders = []
    for i in range(1, len(depth_changes)):
        prev, curr = depth_changes[i-1], depth_changes[i]
        time_diff = (curr['timestamp'] - prev['timestamp']) / 1000  # ms -> s
        
        # 检测闪单: 新增订单快速消失
        if time_diff < 0.5 and has_order_disappeared(prev, curr):
            flash_orders.append({
                'time': curr['timestamp'],
                'duration_ms': time_diff * 1000,
                'price': find_disappeared_price(prev, curr),
                'side': find_disappeared_side(prev, curr)
            })
    
    # 检查在时间窗口内的闪单数量
    flash_order_counts = 0
    # [简化实现]
    
    if flash_order_counts > 10:  # 如果超过10个闪单
        hft_patterns.append({
            'type': 'flash_orders',
            'count': len(flash_orders),
            'avg_duration_ms': np.mean([o['duration_ms'] for o in flash_orders]) if flash_orders else 0,
            'dominant_side': most_common([o['side'] for o in flash_orders]) if flash_orders else 'unknown'
        })
    
    # 2. 扫荡模式 (连续吃掉同一方向的多个小订单)
    sweeps = detect_order_sweeps(trades_df)
    if sweeps:
        hft_patterns.append({
            'type': 'sweeping',
            'instances': len(sweeps),
            'largest_sweep_volume': max([s['total_volume'] for s in sweeps]) if sweeps else 0,
            'dominant_side': most_common([s['side'] for s in sweeps]) if sweeps else 'unknown'
        })
    
    # 3. 层级测试模式 (在不同价格层次小量试探)
    probing_confidence = calculate_probing_confidence(depth_changes, trades_df)
    if probing_confidence > 0.6:  # 如果置信度超过60%
        hft_patterns.append({
            'type': 'layered_probing',
            'confidence': probing_confidence
        })
    
    return hft_patterns

def has_order_disappeared(prev_depth, curr_depth, threshold=0.1):
    """检测价格是否快速消失（挂单撤单）"""
    prev_prices = set(p[0] for p in prev_depth['bids'] + prev_depth['asks'])
    curr_prices = set(p[0] for p in curr_depth['bids'] + curr_depth['asks'])
    disappeared = prev_prices - curr_prices
    return len(disappeared) / max(len(prev_prices), 1) > threshold


def find_disappeared_price(prev_depth, curr_depth):
    prev_prices = set(p[0] for p in prev_depth['bids'] + prev_depth['asks'])
    curr_prices = set(p[0] for p in curr_depth['bids'] + curr_depth['asks'])
    disappeared = list(prev_prices - curr_prices)
    if not disappeared:
        return 0.0  # 安全返回
    return float(disappeared[0])



def find_disappeared_side(prev_depth, curr_depth):
    price = find_disappeared_price(prev_depth, curr_depth)
    for p in prev_depth['bids']:
        if float(p[0]) == price:
            return 'buy'
    for p in prev_depth['asks']:
        if float(p[0]) == price:
            return 'sell'
    return 'unknown'


def detect_order_sweeps(trades_df, volume_threshold=1.0):
    """检测短时间内单边大量吃单行为"""
    if trades_df.empty:
        return []

    trades_df['side'] = trades_df['m'].apply(lambda x: 'sell' if x else 'buy')
    trades_df['T'] = pd.to_numeric(trades_df['T'])
    trades_df = trades_df.sort_values('T')

    sweeps = []
    current_sweep = []
    last_side = None
    last_time = 0

    for _, row in trades_df.iterrows():
        side = row['side']
        t = row['T']
        if last_side == side and (t - last_time) < 1000:
            current_sweep.append(row)
        else:
            if len(current_sweep) >= 3:
                total_vol = sum(float(t['q']) for t in current_sweep)
                sweeps.append({
                    'side': last_side,
                    'total_volume': total_vol
                })
            current_sweep = [row]
        last_side = side
        last_time = t

    return sweeps


def calculate_probing_confidence(depth_changes, trades_df):
    """在多个价格层反复小量试探"""
    price_counts = Counter()
    for d in depth_changes:
        for bid in d.get('bids', [])[:5]:
            price_counts[float(bid[0])] += 1
        for ask in d.get('asks', [])[:5]:
            price_counts[float(ask[0])] += 1
    unique_levels = len([k for k, v in price_counts.items() if v > 2])
    confidence = min(1.0, unique_levels / 10.0)
    return confidence


def most_common(items):
    """找出最常见的项目"""
    if not items:
        return None
    return Counter(items).most_common(1)[0][0]

def calculate_position_size(confidence, signal_strength, max_position=1.0, risk_factor=0.5):
    """根据信号强度和置信度计算建议仓位大小"""
    # 基于置信度和信号强度，返回0-1之间的值表示最大仓位的百分比
    position_pct = confidence * signal_strength * risk_factor
    return min(max_position, max(0.0, position_pct))


def generate_trade_decision(metrics, signals):
    """
    基于多种信号生成交易决策
    """
    # 权重设置
    weights = {
        'delta_divergence': 0.3,
        'order_pressure': 0.25,
        'iceberg_detection': 0.2,
        'whale_activity': 0.15,
        'hft_patterns': 0.1
    }

    # 收集各类信号
    bullish_signals = 0
    bearish_signals = 0
    signal_strength = 0

    # Delta背离信号
    delta_signals = [s for s in signals if s['type'] == 'Delta背离']
    if delta_signals:
        latest = max(delta_signals, key=lambda x: x['timestamp'])
        if latest['direction'] == '做多':
            bullish_signals += weights['delta_divergence']
        else:
            bearish_signals += weights['delta_divergence']
        signal_strength += latest['strength'] * weights['delta_divergence']

    # 订单压力信号
    pressure_ratio = metrics['top_pressure_ratio']
    if pressure_ratio > 1.2:  # 买方压力明显大于卖方
        bullish_signals += weights['order_pressure'] * (pressure_ratio - 1)
    elif pressure_ratio < 0.8:  # 卖方压力明显大于买方
        bearish_signals += weights['order_pressure'] * (1 - pressure_ratio)

    # 综合判断
    if bullish_signals - bearish_signals > 0.3:  # 明显看多
        return {
            'decision': 'BUY',
            'confidence': bullish_signals,
            'strength': signal_strength,
            'suggested_size': calculate_position_size(bullish_signals, signal_strength)
        }
    elif bearish_signals - bullish_signals > 0.3:  # 明显看空
        return {
            'decision': 'SELL',
            'confidence': bearish_signals,
            'strength': signal_strength,
            'suggested_size': calculate_position_size(bearish_signals, signal_strength)
        }
    else:
        return {'decision': 'HOLD'}
    
    

# 大玩家跟踪系统
class WhaleTracker:
    def __init__(self, volume_threshold=10.0):
        """初始化大玩家跟踪器"""
        self.volume_threshold = volume_threshold  # BTC数量阈值
        self.whales = {}  # 已识别的大玩家
        self.active_accumulations = {}  # 正在进行的积累
    
    def update(self, new_trades, new_depth):
        """处理新数据"""
        # 识别大单交易
        large_trades = [t for t in new_trades if float(t['q']) > self.volume_threshold]
        
        # 更新现有大玩家的活动
        for whale_id, whale in self.whales.items():
            self.update_whale_activity(whale_id, new_trades, new_depth)
        
        # 检测新的积累模式
        self.detect_new_accumulation(new_trades, new_depth)
        
        # 检测分布模式
        self.detect_distribution(new_trades, new_depth)
        
        # 生成信号
        signals = self.generate_signals()
        
        return {
            'large_trades': large_trades,
            'active_whales': len(self.whales),
            'accumulation_patterns': len(self.active_accumulations),
            'signals': signals
        }

    def update_whale_activity(self, whale_id, new_trades, new_depth):
        whale = self.whales[whale_id]
        recent_trades = [t for t in new_trades if float(t['q']) > self.volume_threshold]
        whale['trades'].extend(recent_trades)

    def detect_new_accumulation(self, trades, depth):
        buy_trades = [t for t in trades if float(t['q']) > self.volume_threshold and not t['m']]
        if len(buy_trades) >= 3:
            avg_price = np.mean([float(t['p']) for t in buy_trades])
            total_volume = np.sum([float(t['q']) for t in buy_trades])
            whale_id = f'accumulator_{len(self.whales) + 1}'
            self.whales[whale_id] = {
                'type': 'accumulation',
                'avg_price': avg_price,
                'total_volume': total_volume,
                'trades': buy_trades
            }
            self.active_accumulations[whale_id] = self.whales[whale_id]

    def detect_distribution(self, trades, depth):
        sell_trades = [t for t in trades if float(t['q']) > self.volume_threshold and t['m']]
        if len(sell_trades) >= 3:
            avg_price = np.mean([float(t['p']) for t in sell_trades])
            total_volume = np.sum([float(t['q']) for t in sell_trades])
            self.whales[f'distributor_{len(self.whales) + 1}'] = {
                'type': 'distribution',
                'avg_price': avg_price,
                'total_volume': total_volume,
                'trades': sell_trades
            }

    def generate_signals(self):
        signals = []
        for whale_id, whale in self.whales.items():
            signal_type = '大玩家买入' if whale['type'] == 'accumulation' else '大玩家卖出'
            direction = '做多' if whale['type'] == 'accumulation' else '做空'
            signals.append({
                'type': signal_type,
                'direction': direction,
                'strength': round(whale['total_volume'] / self.volume_threshold, 2)
            })
        return signals
