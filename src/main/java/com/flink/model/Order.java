package com.flink.model;

import java.io.Serializable;

public class Order implements Serializable {

    private String invoiceNo;
    private String stockCode;
    private String description;
    private int quantity;
    private double unitPrice;
    private String customerId;
    private String country;
    private long eventTime;

    public Order() {
    }

    public String getInvoiceNo() {
        return invoiceNo;
    }

    public void setInvoiceNo(String invoiceNo) {
        this.invoiceNo = invoiceNo;
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public double getRevenue() {
        return quantity * unitPrice;
    }

    @Override
    public String toString() {

        return "Order{" +
                "invoiceNo='" + invoiceNo + '\'' +
                ", stockCode='" + stockCode + '\'' +
                ", description='" + description + '\'' +
                ", quantity=" + quantity +
                ", unitPrice=" + unitPrice +
                ", customerId='" + customerId + '\'' +
                ", country='" + country + '\'' +
                ", revenue=" + getRevenue() +
                '}';
    }
}