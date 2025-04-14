from kafka.admin import KafkaAdminClient, NewTopic
import socket
import time

# 检查端口是否可以连接
def check_port(host, port, timeout=5):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"检查端口时出错: {e}")
        return False

# 检查Kafka是否可以连接
print("正在检查Kafka连接...")
if not check_port("localhost", 9092):
    print("警告: 无法连接到localhost:9092，Kafka可能未运行")
    print("等待5秒后尝试继续...")
    time.sleep(5)

# 创建管理客户端
try:
    print("正在尝试创建Kafka管理客户端...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='topic-creator'
    )

    # 定义要创建的主题
    topic_list = [
        NewTopic(name="depth", num_partitions=1, replication_factor=1),
        NewTopic(name="trades", num_partitions=1, replication_factor=1),
        NewTopic(name="metrics", num_partitions=1, replication_factor=1)
    ]

    # 创建主题
    print("正在尝试创建主题...")
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("主题创建成功")
    admin_client.close()
except Exception as e:
    print(f"发生错误: {e}")
    print("建议检查Kafka服务器是否正在运行，并确认配置正确")