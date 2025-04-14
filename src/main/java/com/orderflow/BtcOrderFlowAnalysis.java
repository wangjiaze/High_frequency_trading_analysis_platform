// src/main/java/com/orderflow/BtcOrderFlowAnalysis.java
package com.orderflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.function.DepthDataProcessFunction;
import com.orderflow.function.SignalGenerationFunction;
import com.orderflow.function.TradeDataProcessFunction;
import com.orderflow.model.DepthData;
import com.orderflow.model.OrderFlowMetrics;
import com.orderflow.model.TradeData;
import com.orderflow.model.TradingSignal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BtcOrderFlowAnalysis {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEPTH_TOPIC = "depth";
    private static final String TRADES_TOPIC = "trades";
    private static final String METRICS_TOPIC = "metrics";
    private static final String SIGNALS_TOPIC = "signals";

    public static void main(String[] args) throws Exception {
        // 设置Flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置Kafka源 - 深度数据
        KafkaSource<String> depthSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(DEPTH_TOPIC)
                .setGroupId("flink-orderflow-analysis")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 设置Kafka源 - 交易数据
        KafkaSource<String> tradesSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TRADES_TOPIC)
                .setGroupId("flink-orderflow-analysis")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 创建深度数据流
        DataStream<String> depthStringStream = env.fromSource(
                depthSource,
                WatermarkStrategy.noWatermarks(),
                "Depth Source"
        );

        // 创建交易数据流
        DataStream<String> tradesStringStream = env.fromSource(
                tradesSource,
                WatermarkStrategy.noWatermarks(),
                "Trades Source"
        );

        // ObjectMapper用于JSON解析
        final ObjectMapper objectMapper = new ObjectMapper();

        // 解析深度数据
        DataStream<DepthData> depthStream = depthStringStream.map(new MapFunction<String, DepthData>() {
            @Override
            public DepthData map(String value) throws Exception {
                return objectMapper.readValue(value, DepthData.class);
            }
        });

        // 解析交易数据
        DataStream<TradeData> tradesStream = tradesStringStream.map(new MapFunction<String, TradeData>() {
            @Override
            public TradeData map(String value) throws Exception {
                return objectMapper.readValue(value, TradeData.class);
            }
        });

        // 处理深度数据
        DataStream<OrderFlowMetrics> depthMetrics = depthStream
                .keyBy(data -> "BTC-USDT")  // 单一键，因为我们只处理一个交易对
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new DepthDataProcessFunction());

        // 处理交易数据
        DataStream<OrderFlowMetrics> tradeMetrics = tradesStream
                .keyBy(data -> "BTC-USDT")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new TradeDataProcessFunction());

        // 合并指标
        DataStream<OrderFlowMetrics> combinedMetrics = depthMetrics
                .union(tradeMetrics)
                .keyBy(metrics -> "BTC-USDT-" + (metrics.getTimestamp() / 1000))
                .reduce((m1, m2) -> {
                    // 合并两种指标
                    OrderFlowMetrics combined = new OrderFlowMetrics();
                    combined.setTimestamp(Math.max(m1.getTimestamp(), m2.getTimestamp()));
                    
                    // 如果m1包含价格信息
                    if (m1.getLatestPrice() > 0) {
                        combined.setLatestPrice(m1.getLatestPrice());
                        combined.setDelta(m1.getDelta());
                        combined.setVwap(m1.getVwap());
                        combined.setBuyVolume(m1.getBuyVolume());
                        combined.setSellVolume(m1.getSellVolume());
                    } else {
                        combined.setLatestPrice(m2.getLatestPrice());
                        combined.setDelta(m2.getDelta());
                        combined.setVwap(m2.getVwap());
                        combined.setBuyVolume(m2.getBuyVolume());
                        combined.setSellVolume(m2.getSellVolume());
                    }
                    
                    // 如果m1包含深度信息
                    if (m1.getBidAskRatio() > 0) {
                        combined.setBidAskRatio(m1.getBidAskRatio());
                        combined.setTopPressureRatio(m1.getTopPressureRatio());
                    } else {
                        combined.setBidAskRatio(m2.getBidAskRatio());
                        combined.setTopPressureRatio(m2.getTopPressureRatio());
                    }
                    
                    return combined;
                });

        // 生成交易信号
        DataStream<TradingSignal> signals = combinedMetrics
                .keyBy(metrics -> "BTC-USDT")
                .process(new SignalGenerationFunction());

        // 将指标输出到Kafka
        KafkaSink<String> metricsSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(METRICS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        combinedMetrics.map(new MapFunction<OrderFlowMetrics, String>() {
            @Override
            public String map(OrderFlowMetrics metrics) throws Exception {
                return objectMapper.writeValueAsString(metrics);
            }
        }).sinkTo(metricsSink);

        // 将信号输出到Kafka
        KafkaSink<String> signalsSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(SIGNALS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        signals.map(new MapFunction<TradingSignal, String>() {
            @Override
            public String map(TradingSignal signal) throws Exception {
                return objectMapper.writeValueAsString(signal);
            }
        }).sinkTo(signalsSink);

        // 执行作业
        env.execute("Bitcoin OrderFlow Analysis");
    }
}