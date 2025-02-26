package org.bse.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.bse.Order;
import org.bse.OrderRouter;
import org.bse.StockPartition;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * LoadTest isolates order creation and sends orders at a dynamically adjustable rate.
 * It reuses your existing Order, OrderRouter, and StockPartition classes.
 */
public class LoadTest {
    private static final Random random = new Random();
    private static List<StockInfo> stockList;
    private static List<String> dummyUsers;
    private static OrderRouter orderRouter;
    private static ScheduledExecutorService scheduler;

    public static void main(String[] args) {
        // Step 1: Load stocks and dummy users from JSON configuration files.
        try {
            loadStocks("src/main/resources/stocks.json");
            loadDummyUsers("src/main/resources/users.json");
        } catch (IOException e) {
            System.err.println("Error loading JSON files: " + e.getMessage());
            return;
        }

        // Step 2: Create partitions that mimic your production conditions.
        List<StockPartition> partitions = createPartitions();
        orderRouter = new OrderRouter(partitions);

        // Step 3: Set desired orders per second. This can be dynamically adjusted.
        int ordersPerSecond = 400;  // Change this value to 400, 1000, etc.

        // Step 4: Start the load test.
        startLoadTest(ordersPerSecond);

        // Let the load test run for a predetermined period (e.g., 60 seconds).
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 5: Stop load generation and shutdown all components.
        stopLoadTest();
        shutdownPartitions(partitions);
        System.out.println("Load test completed.");
    }

    /**
     * Starts sending orders at the specified orders per second using a scheduled executor.
     *
     * @param ordersPerSecond The rate at which orders should be generated.
     */
    public static void startLoadTest(int ordersPerSecond) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        // Calculate period between orders in nanoseconds.
        long periodNanos = (long) (1_000_000_000.0 / ordersPerSecond);
        scheduler.scheduleAtFixedRate(() -> {
            Order order = generateRandomOrder();
            orderRouter.routeOrder(order);
        }, 0, periodNanos, TimeUnit.NANOSECONDS);
        System.out.println("Load test started at " + ordersPerSecond + " orders per second.");
    }

    /**
     * Stops the load test by shutting down the scheduler.
     */
    public static void stopLoadTest() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Generates a random order by selecting a random stock, computing a random price,
     * picking a random order type, and assigning a random quantity.
     *
     * @return A new Order object.
     */
    private static Order generateRandomOrder() {
        // Choose a random stock from the list.
        StockInfo stock = stockList.get(random.nextInt(stockList.size()));
        double minPrice = stock.getMinPrice();
        double maxPrice = stock.getMaxPrice();
        int steps = (int) ((maxPrice - minPrice) / 0.05);
        int step = random.nextInt(steps + 1);
        double price = minPrice + step * 0.05;

        // Randomly decide if this is a BUY or SELL order.
        Order.OrderType type = random.nextBoolean() ? Order.OrderType.BUY : Order.OrderType.SELL;
        int quantity = 1 + random.nextInt(100);

        // Optionally, pick a dummy user (if Order class can be extended to include user data).
        String user = dummyUsers.get(random.nextInt(dummyUsers.size()));

        // Create and return the order. If needed, update the Order class to store user info.
        Order order = new Order(stock.getName(), type, price, quantity);
        // For example: order.setUser(user);
        return order;
    }

    /**
     * Loads stock information from a JSON file.
     * The JSON should have a structure with a "stocks" array.
     *
     * @param filePath Path to the stocks JSON file.
     * @throws IOException if there is an error reading the file.
     */
    private static void loadStocks(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<StockInfo>> map = mapper.readValue(new File(filePath),
                new TypeReference<Map<String, List<StockInfo>>>() {});
        stockList = map.get("stocks");
        System.out.println("Loaded " + stockList.size() + " stocks.");
    }

    /**
     * Loads dummy user identifiers from a JSON file.
     * The JSON should have a structure with a "users" array.
     *
     * @param filePath Path to the users JSON file.
     * @throws IOException if there is an error reading the file.
     */
    private static void loadDummyUsers(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<String>> map = mapper.readValue(new File(filePath),
                new TypeReference<Map<String, List<String>>>() {});
        dummyUsers = map.get("users");
        System.out.println("Loaded " + dummyUsers.size() + " dummy users.");
    }

    /**
     * Creates partitions for the matching engine based on the stock list.
     * Assumes 5 partitions with equal number of stocks.
     *
     * @return List of StockPartition objects.
     */
    private static List<StockPartition> createPartitions() {
        List<StockPartition> partitions = new ArrayList<>();
        // Sort stocks to ensure they are grouped correctly.
        stockList.sort(Comparator.comparing(StockInfo::getName));
        int stocksPerPartition = stockList.size() / 5;
        for (int i = 0; i < 5; i++) {
            Set<String> partitionStocks = new HashSet<>();
            int start = i * stocksPerPartition;
            int end = start + stocksPerPartition;
            for (int j = start; j < end; j++) {
                partitionStocks.add(stockList.get(j).getName());
            }
            StockPartition partition = new StockPartition("Partition-" + (i + 1), partitionStocks);
            partitions.add(partition);
        }
        return partitions;
    }

    /**
     * Shuts down each StockPartition.
     *
     * @param partitions List of partitions to shut down.
     */
    private static void shutdownPartitions(List<StockPartition> partitions) {
        for (StockPartition partition : partitions) {
            partition.shutdown();
        }
    }

    /**
     * Helper class representing the structure of a stock entry from the JSON.
     */
    public static class StockInfo {
        private String name;
        private double minPrice;
        private double maxPrice;

        // Default constructor is required for Jackson.
        public StockInfo() { }

        public StockInfo(String name, double minPrice, double maxPrice) {
            this.name = name;
            this.minPrice = minPrice;
            this.maxPrice = maxPrice;
        }

        public String getName() {
            return name;
        }

        public double getMinPrice() {
            return minPrice;
        }

        public double getMaxPrice() {
            return maxPrice;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setMinPrice(double minPrice) {
            this.minPrice = minPrice;
        }

        public void setMaxPrice(double maxPrice) {
            this.maxPrice = maxPrice;
        }
    }
}
