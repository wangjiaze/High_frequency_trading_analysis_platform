package com.orderflow.function;

import com.orderflow.model.DepthData;
import com.orderflow.model.OrderFlowMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DepthDataProcessFunction extends ProcessWindowFunction<DepthData, OrderFlowMetrics, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<DepthData> elements, Collector<OrderFlowMetrics> out) {
        // 获取窗口中最新的深度数据
        DepthData latestDepth = null;
        for (DepthData depth : elements) {
            if (latestDepth == null || depth.getLastUpdateId() > latestDepth.getLastUpdateId()) {
                latestDepth = depth;
            }
        }

        if (latestDepth == null || latestDepth.getBids() == null || latestDepth.getAsks() == null) {
            return;
        }

        // 计算买卖压力比率
        double totalBidVolume = 0.0;
        double totalAskVolume = 0.0;
        double top3BidVolume = 0.0;
        double top3AskVolume = 0.0;

        // 处理买单
        List<List<String>> bids = latestDepth.getBids();
        for (int i = 0; i < bids.size(); i++) {
            List<String> level = bids.get(i);
            if (level.size() >= 2) {
                double volume = Double.parseDouble(level.get(1));
                totalBidVolume += volume;
                if (i < 3) {
                    top3BidVolume += volume;
                }
            }
        }

        // 处理卖单
        List<List<String>> asks = latestDepth.getAsks();
        for (int i = 0; i < asks.size(); i++) {
            List<String> level = asks.get(i);
            if (level.size() >= 2) {
                double volume = Double.parseDouble(level.get(1));
                totalAskVolume += volume;
                if (i < 3) {
                    top3AskVolume += volume;
                }
            }
        }

        // 计算买卖比率和前3档压力比
        double bidAskRatio = totalAskVolume > 0 ? totalBidVolume / totalAskVolume : 1.0;
        double topPressureRatio = top3AskVolume > 0 ? top3BidVolume / top3AskVolume : 1.0;

        // 创建指标对象
        OrderFlowMetrics metrics = new OrderFlowMetrics();
        metrics.setTimestamp(context.window().getEnd());
        metrics.setBidAskRatio(bidAskRatio);
        metrics.setTopPressureRatio(topPressureRatio);
        
        // 输出结果
        out.collect(metrics);
    }
}