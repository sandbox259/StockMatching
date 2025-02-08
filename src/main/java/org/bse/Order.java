package org.bse;

import java.time.LocalDateTime;

public class Order {
    public enum OrderType { BUY, SELL }

    private static int orderIdCounter = 0;
    private final int orderId;
    private final String stockSymbol;
    private final OrderType type;
    private final double price;
    private int quantity;
    private final LocalDateTime timestamp;

    public Order(String stockSymbol, OrderType type, double price, int quantity) {
        this.orderId = ++orderIdCounter;
        this.stockSymbol = stockSymbol;
        this.type = type;
        this.price = price;
        this.quantity = quantity;

        this.timestamp = LocalDateTime.now();
    }

    public int getOrderId() {
        return orderId;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public OrderType getType() {
        return type;
    }

    public double getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("OrderID: %d, Stock: %s, Type: %s, Price: %.2f, Quantity: %d, Timestamp: %s",
                orderId, stockSymbol, type, price, quantity, timestamp);
    }
}

