package com.flink.util;

import com.flink.model.Order;

import java.util.Locale;

/** Business validation rules for incoming retail order events. */
public final class OrderValidator {

    private OrderValidator() {
        // Utility class.
    }

    public static boolean isValid(Order order) {
        if (order == null) {
            return false;
        }

        if (order.getInvoiceNo() == null || order.getInvoiceNo().isBlank()) {
            return false;
        }

        // Invoice numbers beginning with C represent cancelled transactions.
        if (order.getInvoiceNo().toUpperCase(Locale.ROOT).startsWith("C")) {
            return false;
        }

        if (order.getQuantity() <= 0) {
            return false;
        }

        if (!Double.isFinite(order.getUnitPrice()) || order.getUnitPrice() <= 0.0d) {
            return false;
        }

        if (order.getCountry() == null || order.getCountry().isBlank()) {
            return false;
        }

        // A missing eventTime is deserialized as zero by Jackson and is not usable
        // for event-time windowing.
        return order.getEventTime() > 0L;
    }
}
