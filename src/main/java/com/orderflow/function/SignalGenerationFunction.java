// src/main/java/com/orderflow/function/SignalGenerationFunction.java
package com.orderflow.function;

import com.orderflow.model.OrderFlowMetrics;
import com.orderflow.model.TradingSignal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SignalGenerationFunction extends KeyedProcessFunction<String, OrderFlowMetrics, TradingSignal> {

    private transient ListState<OrderFlowMetrics> metricsHistory;
    private static final int HISTORY_SIZE = 10;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<OrderFlowMetrics> descriptor = new ListStateDescriptor<>(
                "metrics-history",
                TypeInformation.of(new TypeHint<OrderFlowMetrics>() {})
        );
        metricsHistory = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(OrderFlowMetrics metrics, Context context, Collector<TradingSignal> out) throws Exception {
        // 更新历史数据
        List<OrderFlowMetrics> history = new ArrayList<>();
        for (OrderFlowMetrics m : metricsHistory.get()) {
            history.add(m);
        }
        
        // 添加新数据并保持历史大小限制
        history.add(metrics);
        if (history.size() > HISTORY_SIZE) {
            history.remove(0);
        }
        
        // 清除并更新状态
        metricsHistory.clear();
        for (OrderFlowMetrics m : history) {
            metricsHistory.add(m);
        }
        
        // 检查是否有足够的历史数据生成信号
        if (history.size() >= 5) {
            // 信号1: Delta背离
            OrderFlowMetrics current = history.get(history.size() - 1);
            OrderFlowMetrics previous = history.get(history.size() - 5);
            
            double priceChange = current.getLatestPrice() - previous.getLatestPrice();
            double deltaSum = 0;
            for (int i = history.size() - 5; i < history.size(); i++) {
                deltaSum += history.get(i).getDelta();
            }
            
            // 价格上涨但Delta为负
            if (priceChange > 0 && deltaSum < -0.1) {
                TradingSignal signal = new TradingSignal();
                signal.setTimestamp(current.getTimestamp());
                signal.setType("Delta背离");
                signal.setDirection("做空");
                signal.setStrength(Math.abs(deltaSum) / current.getBuyVolume());
                signal.setMessage("Delta背离: 价格上涨但买方力量减弱");
                
                out.collect(signal);
            }
            
            // 价格下跌但Delta为正
            else if (priceChange < 0 && deltaSum > 0.1) {
                TradingSignal signal = new TradingSignal();
                signal.setTimestamp(current.getTimestamp());
                signal.setType("Delta背离");
                signal.setDirection("做多");
                signal.setStrength(Math.abs(deltaSum) / current.getSellVolume());
                signal.setMessage("Delta背离: 价格下跌但买方力量增强");
                
                out.collect(signal);
            }
            
            // 信号2: 订单簿压力突变
            if (history.size() >= 3) {
                OrderFlowMetrics threeBack = history.get(history.size() - 3);
                double pressureChange = current.getTopPressureRatio() - threeBack.getTopPressureRatio();
                
                if (Math.abs(pressureChange) > 0.2) {
                    TradingSignal signal = new TradingSignal();
                    signal.setTimestamp(current.getTimestamp());
                    signal.setType("订单压力突变");
                    signal.setDirection(pressureChange > 0 ? "做多" : "做空");
                    signal.setStrength(Math.abs(pressureChange) * 5);
                    signal.setMessage("订单压力" + (pressureChange > 0 ? "增加" : "减少") + 
                                     ": " + String.format("%.2f", Math.abs(pressureChange)));
                    
                    out.collect(signal);
                }
            }
        }
    }
}