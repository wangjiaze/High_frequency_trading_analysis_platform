// src/main/java/com/orderflow/function/TradeDataProcessFunction.java
package com.orderflow.function;

import com.orderflow.model.OrderFlowMetrics;
import com.orderflow.model.TradeData;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TradeDataProcessFunction extends ProcessWindowFunction<TradeData, OrderFlowMetrics, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<TradeData> elements, Collector<OrderFlowMetrics> out) {
        double buyVolume = 0.0;
        double sellVolume = 0.0;
        double vwapNum = 0.0;
        double vwapDen = 0.0;
        double latestPrice = 0.0;

        // 处理窗口内的所有交易
        for (TradeData trade : elements) {
            double price = Double.parseDouble(trade.getPrice());
            double quantity = Double.parseDouble(trade.getQuantity());
            
            // 更新最新价格
            latestPrice = price;
            
            // 累加成交量
            vwapNum += price * quantity;
            vwapDen += quantity;
            
            // 区分买卖方向
            if (trade.isMarketMaker()) {
                // 买方是挂单方，说明卖方是主动成交方
                sellVolume += quantity;
            } else {
                // 卖方是挂单方，说明买方是主动成交方
                buyVolume += quantity;
            }
        }

        // 计算指标
        double delta = buyVolume - sellVolume;
        double vwap = vwapDen > 0 ? vwapNum / vwapDen : 0;

        // 创建指标对象
        OrderFlowMetrics metrics = new OrderFlowMetrics();
        metrics.setTimestamp(context.window().getEnd());
        metrics.setLatestPrice(latestPrice);
        metrics.setDelta(delta);
        metrics.setVwap(vwap);
        metrics.setBuyVolume(buyVolume);
        metrics.setSellVolume(sellVolume);
        
        // 输出结果
        out.collect(metrics);
    }
}