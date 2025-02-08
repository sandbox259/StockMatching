package org.bse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderRouter {
    private final List<StockPartition> partitions;
    // Map stock symbol -> StockPartition
    private final Map<String, StockPartition> stockToPartitionMap;

    public OrderRouter(List<StockPartition> partitions) {
        this.partitions = partitions;
        this.stockToPartitionMap = new HashMap<>();
        // Build the mapping based on each partition's stockSymbols set.
        for (StockPartition partition : partitions) {
            for (String stock : partition.getStockSymbols()) {
                stockToPartitionMap.put(stock, partition);
            }
        }
    }

    /**
     * Routes the given order to the appropriate partition using the pre-built mapping.
     */
    public void routeOrder(Order order) {
        StockPartition partition = stockToPartitionMap.get(order.getStockSymbol());
        if (partition != null) {
            partition.submitOrder(order);
        } else {
            System.err.println("No partition found for stock: " + order.getStockSymbol());
        }
    }
}
