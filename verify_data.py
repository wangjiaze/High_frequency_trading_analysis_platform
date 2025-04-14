# verify_data.py - 改进版
from kafka import KafkaConsumer
import json
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_verifier')

# Kafka配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DEPTH_TOPIC = 'depth'
TRADES_TOPIC = 'trades'


def pretty_print(obj, indent=0):
    """更好地打印嵌套对象"""
    prefix = ' ' * indent
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                logger.info(f"{prefix}{key}:")
                pretty_print(value, indent + 2)
            else:
                logger.info(f"{prefix}{key}: {value}")
    elif isinstance(obj, list):
        if len(obj) > 0:
            if isinstance(obj[0], (dict, list)):
                for i, item in enumerate(obj[:5]):  # 只打印前5个
                    logger.info(f"{prefix}[{i}]:")
                    pretty_print(item, indent + 2)
                if len(obj) > 5:
                    logger.info(f"{prefix}... (共 {len(obj)} 项)")
            else:
                # 简单列表，直接打印前几项
                sample = obj[:5]
                logger.info(f"{prefix}{sample} ... (共 {len(obj)} 项)")
    else:
        logger.info(f"{prefix}{obj}")


def verify_depth_data():
    logger.info("开始验证深度数据...")
    consumer = KafkaConsumer(
        DEPTH_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # 10秒超时
    )

    count = 0
    start_time = time.time()

    try:
        for message in consumer:
            data = message.value
            count += 1

            if count == 1:
                # 详细打印第一条消息的完整结构
                logger.info("深度数据样例 (完整结构):")
                pretty_print(data)

                # 特别关注买卖盘数据
                logger.info("\n买盘示例 (前5档):")
                if 'bids' in data and data['bids']:
                    for i, bid in enumerate(data['bids'][:5]):
                        logger.info(f"  档位 {i + 1}: 价格 {bid[0]}, 数量 {bid[1]}")

                logger.info("\n卖盘示例 (前5档):")
                if 'asks' in data and data['asks']:
                    for i, ask in enumerate(data['asks'][:5]):
                        logger.info(f"  档位 {i + 1}: 价格 {ask[0]}, 数量 {ask[1]}")

            if count % 10 == 0:
                logger.info(f"已接收 {count} 条深度数据")

            if count >= 10:  # 只接收10条消息
                break

    except Exception as e:
        logger.error(f"验证深度数据时出错: {e}")
    finally:
        consumer.close()

    duration = time.time() - start_time
    logger.info(f"深度数据验证完成。共接收 {count} 条消息，用时 {duration:.2f} 秒。")
    if duration > 0:
        logger.info(f"每秒约 {count / duration:.2f} 条消息。")


def verify_trade_data():
    logger.info("开始验证成交数据...")
    consumer = KafkaConsumer(
        TRADES_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # 10秒超时
    )

    count = 0
    start_time = time.time()

    try:
        for message in consumer:
            data = message.value
            count += 1

            if count == 1:
                # 详细打印第一条消息的结构
                logger.info("成交数据样例:")
                logger.info(f"交易ID: {data.get('t')}")
                logger.info(f"时间戳: {data.get('T')}")
                logger.info(f"价格: {data.get('p')}")
                logger.info(f"数量: {data.get('q')}")
                logger.info(f"买方是否是挂单方: {data.get('m')}")
                logger.info(f"是否是最优价格匹配: {data.get('M')}")

            if count % 10 == 0:
                logger.info(f"已接收 {count} 条成交数据")

            if count >= 30:  # 收到30条消息后停止
                break

    except Exception as e:
        logger.error(f"验证成交数据时出错: {e}")
    finally:
        consumer.close()

    duration = time.time() - start_time
    logger.info(f"成交数据验证完成。共接收 {count} 条消息，用时 {duration:.2f} 秒。")
    if duration > 0:
        logger.info(f"每秒约 {count / duration:.2f} 条消息。")


if __name__ == "__main__":
    try:
        #verify_depth_data()
        # time.sleep(1)  # 稍微暂停一下
        verify_trade_data()  # 先注释掉，专注于深度数据
    except KeyboardInterrupt:
        logger.info("验证过程被用户中断")