package org.bse;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class StockPartition {
    private final String partitionName;
    private final Set<String> stockSymbols; // Stocks in this partition
    private final Map<String, OrderBook> orderBooks; // Stock symbol -> OrderBook
    private final BlockingQueue<Order> orderQueue;
    private final AtomicLong ordersRead = new AtomicLong(0);
    private volatile boolean running = true;

    // Use a fixed thread pool for consumers (readers)
    private final ExecutorService readerPool;

    public StockPartition(String partitionName, Set<String> stockSymbols) {
        this.partitionName = partitionName;
        this.stockSymbols = stockSymbols;
        this.orderBooks = new ConcurrentHashMap<>();
        for (String symbol : stockSymbols) {
            orderBooks.put(symbol, new OrderBook(symbol));
        }
        this.orderQueue = new LinkedBlockingQueue<>();
        // Create a thread pool with 5 threads for readers.
        this.readerPool = Executors.newFixedThreadPool(5, r -> {
            Thread t = new Thread(r);
            t.setName(partitionName + "-Reader");
            return t;
        });
        startReaders();
    }

    /**
     * Provides the set of stock symbols this partition is responsible for.
     */
    public Set<String> getStockSymbols() {
        return stockSymbols;
    }

    /**
     * Checks if this partition is responsible for the given stock symbol.
     */
    public boolean hasStock(String stockSymbol) {
        return stockSymbols.contains(stockSymbol);
    }

    /**
     * Submits an order to this partition's order queue.
     */
    public void submitOrder(Order order) {
        try {
            orderQueue.put(order);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts 5 reader tasks using the thread pool.
     */
    private void startReaders() {
        // Submit 5 reader tasks.
        for (int i = 0; i < 5; i++) {
            readerPool.submit(() -> {
                while (running) {
                    try {
                        Order order = orderQueue.take();
                        ordersRead.incrementAndGet();
                        // Look up (or create) the OrderBook for this stock.
                        OrderBook book = orderBooks.get(order.getStockSymbol());
                        if (book != null) {
                            book.processOrder(order);
                        } else {
                            System.err.println("No order book found for stock: " + order.getStockSymbol());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    /**
     * Shuts down this partition by stopping the reader pool.
     */
    public void shutdown() {
        running = false;
        readerPool.shutdownNow();
    }

    public long getOrdersRead() {
        return ordersRead.get();
    }

    /**
     * Returns the total trades (matches) across all order books in this partition.
     */
    public long getTotalTrades() {
        long total = 0;
        for (OrderBook book : orderBooks.values()) {
            total += book.getTotalTrades();
        }
        return total;
    }

    public void displayStats() {
        System.out.println("=== Stats for " + partitionName + " ===");
        System.out.println("Total Orders Read: " + getOrdersRead());
        System.out.println("Total Trades Executed: " + getTotalTrades());
        for (OrderBook book : orderBooks.values()) {
            book.displayStats();
        }
    }

    public void displayRemainingOrders() {
        for (OrderBook book : orderBooks.values()) {
            book.displayRemainingOrders();
        }
    }

    @Override
    public String toString() {
        return partitionName;
    }
}
