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

# Binance WebSocket URLs
DEPTH_WS_URL = "wss://stream.binance.com/ws/btcusdt@depth20@100ms"
TRADES_WS_URL = "wss://stream.binance.com/ws/btcusdt@trade"

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


# 处理深度数据
def on_depth_message(ws, message):
    if producer is None:
        return

    try:
        data = json.loads(message)
        # 添加时间戳
        data['local_timestamp'] = int(time.time() * 1000)
        # 发送到Kafka
        producer.send(KAFKA_TOPIC_DEPTH, data)

        # 日志显示（限制频率，避免刷屏）
        if data['local_timestamp'] % 5000 < 100:  # 大约每5秒记录一次
            logger.info(f"深度数据: {len(data['bids'])}档买单, {len(data['asks'])}档卖单")
    except Exception as e:
        logger.error(f"处理深度数据错误: {e}")


# 处理成交数据
def on_trade_message(ws, message):
    if producer is None:
        return

    try:
        data = json.loads(message)
        # 添加时间戳
        data['local_timestamp'] = int(time.time() * 1000)
        # 发送到Kafka
        producer.send(KAFKA_TOPIC_TRADES, data)

        # 限制日志频率
        if data['local_timestamp'] % 2000 < 100:  # 大约每0.5秒记录一次
            logger.info(f"成交数据: 价格={data['p']}, 数量={data['q']}, 方向={'买入' if data['m'] else '卖出'}")
    except Exception as e:
        logger.error(f"处理成交数据错误: {e}")


# WebSocket错误处理
def on_error(ws, error):
    logger.error(f"WebSocket错误: {error}")


# WebSocket关闭处理
def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket连接关闭: {close_msg} (代码: {close_status_code})")


# WebSocket连接建立
def on_open(ws):
    logger.info("WebSocket连接已建立")


# 启动深度数据WebSocket
def start_depth_ws():
    ws = websocket.WebSocketApp(DEPTH_WS_URL,
                                on_message=on_depth_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws


# 启动成交数据WebSocket
def start_trade_ws():
    ws = websocket.WebSocketApp(TRADES_WS_URL,
                                on_message=on_trade_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws


# 主函数
def main():
    import threading

    # 创建并启动深度数据线程
    depth_thread = threading.Thread(target=lambda: start_depth_ws().run_forever())
    depth_thread.daemon = True
    depth_thread.start()

    # 创建并启动成交数据线程
    trade_thread = threading.Thread(target=lambda: start_trade_ws().run_forever())
    trade_thread.daemon = True
    trade_thread.start()

    logger.info("数据收集器已启动")

    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")


if __name__ == "__main__":
    main()