package org.bse;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class OrderBook {
    private final String stockSymbol;
    private final ConcurrentSkipListMap<Double, Queue<Order>> sellBook;
    private final ConcurrentSkipListMap<Double, Queue<Order>> buyBook;

    // Performance counters
    private final AtomicLong totalOrders = new AtomicLong(0);
    private final AtomicLong totalTrades = new AtomicLong(0);

    public OrderBook(String stockSymbol) {
        this.stockSymbol = stockSymbol;
        // Sell orders: lowest price first.
        sellBook = new ConcurrentSkipListMap<>();
        // Buy orders: highest price first.
        buyBook = new ConcurrentSkipListMap<>(Collections.reverseOrder());
    }

    /**
     * Processes an incoming order by matching it against the opposing order book.
     */
    public synchronized void processOrder(Order order) {
        totalOrders.incrementAndGet();
        matchOrder(order);
    }

    private void matchOrder(Order order) {
        if (order.getType() == Order.OrderType.BUY) {
            // For BUY orders, match with SELL orders with price <= order.getPrice()
            NavigableMap<Double, Queue<Order>> possibleMatches = sellBook.headMap(order.getPrice(), true);
            for (Map.Entry<Double, Queue<Order>> entry : possibleMatches.entrySet()) {
                if (order.getQuantity() <= 0) break;
                Queue<Order> sellQueue = entry.getValue();
                while (!sellQueue.isEmpty() && order.getQuantity() > 0) {
                    Order sellOrder = sellQueue.peek();
                    int matchQuantity = Math.min(order.getQuantity(), sellOrder.getQuantity());
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    sellOrder.setQuantity(sellOrder.getQuantity() - matchQuantity);
                    totalTrades.incrementAndGet();
                    if (sellOrder.getQuantity() == 0) {
                        sellQueue.poll();
                    }
                }
                if (sellQueue.isEmpty()) {
                    sellBook.remove(entry.getKey());
                }
            }
            // If not fully filled, add the remaining order to the BUY book.
            if (order.getQuantity() > 0) {
                addOrderToBook(buyBook, order.getPrice(), order);
            }
        } else { // SELL order
            // For SELL orders, match with BUY orders with price >= order.getPrice()
            NavigableMap<Double, Queue<Order>> possibleMatches = buyBook.headMap(order.getPrice(), true);
            for (Map.Entry<Double, Queue<Order>> entry : possibleMatches.entrySet()) {
                if (order.getQuantity() <= 0) break;
                Queue<Order> buyQueue = entry.getValue();
                while (!buyQueue.isEmpty() && order.getQuantity() > 0) {
                    Order buyOrder = buyQueue.peek();
                    int matchQuantity = Math.min(order.getQuantity(), buyOrder.getQuantity());
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    buyOrder.setQuantity(buyOrder.getQuantity() - matchQuantity);
                    totalTrades.incrementAndGet();
                    if (buyOrder.getQuantity() == 0) {
                        buyQueue.poll();
                    }
                }
                if (buyQueue.isEmpty()) {
                    buyBook.remove(entry.getKey());
                }
            }
            // If not fully filled, add the remaining order to the SELL book.
            if (order.getQuantity() > 0) {
                addOrderToBook(sellBook, order.getPrice(), order);
            }
        }
    }

    private void addOrderToBook(ConcurrentSkipListMap<Double, Queue<Order>> book, double price, Order order) {
        Queue<Order> queue = book.computeIfAbsent(price, k -> new LinkedList<>());
        queue.offer(order);
    }

    public long getTotalOrders() {
        return totalOrders.get();
    }

    public long getTotalTrades() {
        return totalTrades.get();
    }

    public void displayStats() {
        System.out.println("Order Book for " + stockSymbol);
        System.out.println("Total Orders Processed: " + totalOrders.get());
        System.out.println("Total Trades Executed: " + totalTrades.get());
    }

    public void displayRemainingOrders() {
        System.out.println("===== Remaining Sell Orders for " + stockSymbol + " =====");
        for (Map.Entry<Double, Queue<Order>> entry : sellBook.entrySet()) {
            System.out.println(String.format("Price Level: %.2f", entry.getKey()));
            for (Order order : entry.getValue()) {
                System.out.println(order);
            }
        }
        System.out.println("===== Remaining Buy Orders for " + stockSymbol + " =====");
        for (Map.Entry<Double, Queue<Order>> entry : buyBook.entrySet()) {
            System.out.println(String.format("Price Level: %.2f", entry.getKey()));
            for (Order order : entry.getValue()) {
                System.out.println(order);
            }
        }
    }
}


