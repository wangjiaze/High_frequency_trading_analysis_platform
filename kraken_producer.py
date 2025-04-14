# data_collector.py
import json
import time
import websocket
from kafka import KafkaProducer
import logging
import threading

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_collector')

# Kafka配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_DEPTH = 'depth'
KAFKA_TOPIC_TRADES = 'trades'
KAFKA_TOPIC_TICKER = 'ticker'  # 新增ticker主题

# Kraken WebSocket V2 API URL (公共数据不需要认证)
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

# 初始化Kafka生产者
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    logger.info("Kafka生产者初始化成功")
except Exception as e:
    logger.error(f"Kafka生产者初始化失败: {e}")
    producer = None


# Kraken数据处理函数
def on_message(ws, message):
    if producer is None:
        return

    try:
        # 记录原始消息用于调试
        logger.debug(f"收到消息: {message[:200]}...")

        data = json.loads(message)

        # V2 API 使用新的消息格式
        if "channel" in data:
            channel = data.get("channel")
            msg_type = data.get("type")

            # 处理状态更新
            if channel == "status":
                if msg_type == "update":
                    status_data = data.get("data", [{}])[0]
                    logger.info(f"Kraken状态: {status_data.get('system', 'unknown')}, " +
                                f"版本: {status_data.get('version', 'unknown')}, " +
                                f"API版本: {status_data.get('api_version', 'unknown')}")
                return

            # 处理订单簿数据
            if channel == "book":
                handle_book_v2(data)
                return

            # 处理成交数据
            if channel == "trade":
                handle_trade_v2(data)
                return

            # 处理行情数据
            if channel == "ticker":
                handle_ticker_v2(data)
                return

            # 其他未知频道
            logger.info(f"收到未处理的频道数据: {channel} - {msg_type}")

        # 处理错误和其他消息
        elif "error" in data:
            error_msg = data.get("error", "未知错误")
            method = data.get("method", "未知方法")
            success = data.get("success", False)

            if not success:
                logger.error(f"API错误: {error_msg}, 方法: {method}")

                # 如果是方法不存在错误，可能需要尝试不同的API版本
                if "Method(s) not found" in error_msg:
                    logger.warning("API方法不存在，可能需要更新API格式")
            return

        # 其他未识别的消息格式
        else:
            logger.warning(f"收到未识别的消息格式: {message[:100]}...")

    except json.JSONDecodeError:
        logger.error(f"JSON解析错误: {message[:100]}...")
    except Exception as e:
        logger.error(f"处理数据错误: {e}", exc_info=True)


# 处理V2 API的订单簿数据
def handle_book_v2(data):
    try:
        msg_type = data.get("type")
        book_data = data.get("data", [{}])[0]
        symbol = book_data.get("symbol", "unknown")

        # 转换为类似于Binance格式的数据结构
        depth_data = {
            'lastUpdateId': int(time.time() * 1000),  # 使用当前时间作为更新ID
            'bids': [],
            'asks': [],
            'local_timestamp': int(time.time() * 1000),
            'pair': symbol
        }

        # 处理快照数据
        if msg_type == "snapshot":
            # 处理买单
            if "bids" in book_data:
                for bid in book_data["bids"]:
                    if "price" in bid and "qty" in bid:
                        depth_data['bids'].append([str(bid["price"]), str(bid["qty"])])

            # 处理卖单
            if "asks" in book_data:
                for ask in book_data["asks"]:
                    if "price" in ask and "qty" in ask:
                        depth_data['asks'].append([str(ask["price"]), str(ask["qty"])])

            logger.info(f"深度数据(完整): {len(depth_data['bids'])}档买单, {len(depth_data['asks'])}档卖单")

        # 处理更新数据
        elif msg_type == "update":
            # 处理买单更新
            if "bids" in book_data:
                for bid in book_data["bids"]:
                    if "price" in bid and "qty" in bid:
                        # qty为0表示删除此价格
                        if bid["qty"] > 0:
                            depth_data['bids'].append([str(bid["price"]), str(bid["qty"])])

            # 处理卖单更新
            if "asks" in book_data:
                for ask in book_data["asks"]:
                    if "price" in ask and "qty" in ask:
                        # qty为0表示删除此价格
                        if ask["qty"] > 0:
                            depth_data['asks'].append([str(ask["price"]), str(ask["qty"])])

            # 记录日志(限制频率)
            if depth_data['local_timestamp'] % 1000 < 100:
                ask_count = len(depth_data['asks'])
                bid_count = len(depth_data['bids'])
                logger.info(f"深度数据(更新): {bid_count}档买单, {ask_count}档卖单")

        # 确保数据不为空才发送
        if depth_data['bids'] or depth_data['asks']:
            # 发送到Kafka
            producer.send(KAFKA_TOPIC_DEPTH, depth_data)

    except Exception as e:
        logger.error(f"处理订单簿数据错误: {e}", exc_info=True)


# 处理V2 API的成交数据
def handle_trade_v2(data):
    try:
        msg_type = data.get("type")

        # 只处理更新数据
        if msg_type != "update":
            return

        trade_data_list = data.get("data", [])

        for trade_item in trade_data_list:
            symbol = trade_item.get("symbol", "unknown")
            side = trade_item.get("side", "")  # "buy" 或 "sell"
            price = trade_item.get("price", 0)
            qty = trade_item.get("qty", 0)
            time_str = trade_item.get("time", "")

            # 确保价格和数量有效
            if not price or not qty:
                continue

            # 转换为时间戳
            try:
                # 将RFC3339格式转换为毫秒级时间戳
                trade_time = time.mktime(time.strptime(time_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")) * 1000
            except:
                trade_time = int(time.time() * 1000)  # 使用当前时间作为备选

            # 转换为与Binance类似的格式
            trade_data = {
                'e': 'trade',  # 事件类型
                'E': int(time.time() * 1000),  # 事件时间
                's': symbol,  # 交易对
                't': int(trade_time * 1000),  # 交易ID（使用时间戳*1000作为唯一ID）
                'p': str(price),  # 价格
                'q': str(qty),  # 数量
                'b': 0,  # 买方订单ID (Kraken不提供)
                'a': 0,  # 卖方订单ID (Kraken不提供)
                'T': int(trade_time),  # 交易时间
                'm': side == "sell",  # 是否是卖方发起的交易
                'M': False,  # 是否是最佳价格匹配
                'local_timestamp': int(time.time() * 1000)
            }

            # 发送到Kafka
            producer.send(KAFKA_TOPIC_TRADES, trade_data)

            # 限制日志频率
            if trade_data['local_timestamp'] % 1000 < 100:
                logger.info(f"成交数据: 价格={trade_data['p']}, 数量={trade_data['q']}, " +
                            f"方向={'卖出' if trade_data['m'] else '买入'}")

    except Exception as e:
        logger.error(f"处理成交数据错误: {e}", exc_info=True)


# 处理V2 API的行情数据
def handle_ticker_v2(data):
    try:
        msg_type = data.get("type")

        # 只处理快照和更新数据
        if msg_type not in ["snapshot", "update"]:
            return

        ticker_data_list = data.get("data", [])

        for ticker_item in ticker_data_list:
            symbol = ticker_item.get("symbol", "unknown")
            last_price = ticker_item.get("last", 0)
            best_bid = ticker_item.get("bid", 0)
            best_ask = ticker_item.get("ask", 0)

            # 确保价格有效
            if not last_price:
                continue

            # 记录价格信息
            logger.info(f"Ticker价格信息 ({symbol}): 最新成交价={last_price}, 买价={best_bid}, 卖价={best_ask}")

            # 发送到Kafka
            ticker_info = {
                'timestamp': int(time.time() * 1000),
                'pair': symbol,
                'last_price': str(last_price),
                'best_bid': str(best_bid),
                'best_ask': str(best_ask),
                'source': 'kraken'
            }
            producer.send(KAFKA_TOPIC_TICKER, ticker_info)

    except Exception as e:
        logger.error(f"处理Ticker数据错误: {e}", exc_info=True)


# WebSocket错误处理
def on_error(ws, error):
    logger.error(f"WebSocket错误: {error}")


# WebSocket关闭处理
def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket连接关闭: {close_msg} (代码: {close_status_code})")


# WebSocket连接建立
def on_open(ws):
    logger.info("WebSocket连接已建立")

    # V2 API 使用新的订阅格式
    # 订阅市场数据（2级深度）
    book_sub = {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": ["BTC/USD"],
            "depth": 20
        },
        "req_id": 1
    }
    ws.send(json.dumps(book_sub))
    logger.info(f"发送订单簿订阅: {json.dumps(book_sub)}")

    # 订阅成交数据
    trade_sub = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": ["BTC/USD"]
        },
        "req_id": 2
    }
    ws.send(json.dumps(trade_sub))
    logger.info(f"发送成交数据订阅: {json.dumps(trade_sub)}")

    # 订阅Ticker数据
    ticker_sub = {
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": ["BTC/USD"]
        },
        "req_id": 3
    }
    ws.send(json.dumps(ticker_sub))
    logger.info(f"发送Ticker订阅: {json.dumps(ticker_sub)}")


# 启动Kraken WebSocket连接
def start_kraken_ws():
    ws = websocket.WebSocketApp(KRAKEN_WS_URL,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws


# 主函数
def main():
    # 启用更详细的websocket日志（可选，仅用于调试）
    # websocket.enableTrace(True)

    # 创建并启动Kraken WebSocket线程
    kraken_thread = threading.Thread(target=lambda: start_kraken_ws().run_forever(
        ping_interval=30,  # 30秒ping一次保持连接
        ping_timeout=10  # 10秒ping超时
    ))
    kraken_thread.daemon = True
    kraken_thread.start()

    logger.info("Kraken V2 数据收集器已启动")

    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")


if __name__ == "__main__":
    main()