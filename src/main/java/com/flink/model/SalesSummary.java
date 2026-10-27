package com.flink.model;

import java.io.Serializable;

public class SalesSummary implements Serializable {

    private String country;

    private long orderCount;

    private double totalRevenue;

    public SalesSummary() {
    }

    public SalesSummary(
            String country,
            long orderCount,
            double totalRevenue
    ) {

        this.country = country;
        this.orderCount = orderCount;
        this.totalRevenue = totalRevenue;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    @Override
    public String toString() {

        return "SALES SUMMARY | " +
                "country=" + country +
                " | orders=" + orderCount +
                " | revenue=£" +
                String.format("%.2f", totalRevenue);
    }
}