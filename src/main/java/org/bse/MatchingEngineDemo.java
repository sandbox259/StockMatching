package org.bse;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MatchingEngineDemo {
    private static final AtomicLong ordersProduced = new AtomicLong(0);

    public static void main(String[] args) {
        // Define stock symbols and their respective price ranges.
        Map<String, double[]> stockPriceRanges = new HashMap<>();
        for (int i = 1; i <= 25; i++) {
            String stockName = "Stock" + i;
            double minPrice = 10.0 * i;
            double maxPrice = 15.0 * i;
            stockPriceRanges.put(stockName, new double[]{minPrice, maxPrice});
        }

        // Create partitions.
        List<StockPartition> partitions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Set<String> partitionStocks = new HashSet<>();
            for (int j = 1; j <= 5; j++) {
                partitionStocks.add("Stock" + (i * 5 + j));
            }
            StockPartition partition = new StockPartition("Partition-" + (i + 1), partitionStocks);
            partitions.add(partition);
        }

        // Create the order router.
        OrderRouter router = new OrderRouter(partitions);

        // Start time for performance monitoring.
        long startTime = System.currentTimeMillis();

        // Use a fixed thread pool for producers.
        ExecutorService producerPool = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r);
            t.setName("Producer");
            return t;
        });

        // Producer that generates orders at a throttled rate of 1 order per millisecond.
        Runnable orderProducer = () -> {
            Random random = new Random();
            List<String> stocks = new ArrayList<>(stockPriceRanges.keySet());
            while (!Thread.currentThread().isInterrupted()) {
                String stock = stocks.get(random.nextInt(stocks.size()));
                double[] range = stockPriceRanges.get(stock);
                double minPrice = range[0];
                double maxPrice = range[1];
                int steps = (int) ((maxPrice - minPrice) / 0.05);
                int step = random.nextInt(steps + 1);
                double price = minPrice + step * 0.05;
                Order.OrderType type = random.nextBoolean() ? Order.OrderType.BUY : Order.OrderType.SELL;
                int quantity = 1 + random.nextInt(100);
                Order order = new Order(stock, type, price, quantity);
                router.routeOrder(order);
                ordersProduced.incrementAndGet();

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };


        /*// Launch 3 producer tasks.
        for (int i = 0; i < 3; i++) {
            producerPool.submit(orderProducer);
        }
        */

        // Performance monitor: print stats every second.
        ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor();
        monitorExecutor.scheduleAtFixedRate(() -> {
            long elapsedMillis = System.currentTimeMillis() - startTime;
            double seconds = elapsedMillis / 1000.0;
            long totalProduced = ordersProduced.get();
            double producedPerSec = totalProduced / seconds;

            long totalTradesAllPartitions = 0;
            for (StockPartition partition : partitions) {
                totalTradesAllPartitions += partition.getTotalTrades();
            }
            double tradesPerSecAll = totalTradesAllPartitions / seconds;

            System.out.println("=== Performance Metrics ===");
            System.out.println("Total Orders Produced: " + totalProduced);
            System.out.printf("Orders Produced per Second: %.2f%n", producedPerSec);

            for (StockPartition partition : partitions) {
                long ordersRead = partition.getOrdersRead();
                double ordersReadPerSec = ordersRead / seconds;
                long totalTrades = partition.getTotalTrades();
                double tradesPerSec = totalTrades / seconds;
                System.out.println("[" + partition + "]");
                System.out.println("  Orders Read: " + ordersRead + " (" + String.format("%.2f", ordersReadPerSec) + "/sec)");
                System.out.println("  Total Trades (Matches): " + totalTrades + " (" + String.format("%.2f", tradesPerSec) + "/sec)");
            }
            System.out.println("Aggregated Matches per Second: " + String.format("%.2f", tradesPerSecAll));
            System.out.println("============================");
        }, 1, 1, TimeUnit.SECONDS);

        // Let the system run for a fixed time (e.g., 20 seconds) for demonstration.
        try {
            Thread.sleep(200000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shutdown producers.
        producerPool.shutdownNow();

        // Shutdown the performance monitor.
        monitorExecutor.shutdownNow();

        // Shutdown partitions.
        for (StockPartition partition : partitions) {
            partition.shutdown();
        }

        // Final statistics.
        System.out.println("\n=== Final Statistics ===");
        System.out.println("Total Orders Produced: " + ordersProduced.get());
        for (StockPartition partition : partitions) {
            partition.displayStats();
        }
    }
}
