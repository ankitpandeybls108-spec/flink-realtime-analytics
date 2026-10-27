"""Replay Online Retail Excel rows to the Kafka `orders` topic."""

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


REQUIRED_COLUMNS = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="data/Online Retail.xlsx", help="Path to the Excel dataset")
    parser.add_argument("--limit", type=int, default=10_000, help="Maximum number of rows to stream")
    parser.add_argument("--delay-ms", type=int, default=20, help="Delay between events in milliseconds")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap server")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.delay_ms < 0:
        parser.error("--delay-ms cannot be negative")
    return args


def main():
    args = parse_args()
    dataset_path = Path(args.file)
    if not dataset_path.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {dataset_path.resolve()}. Run this command from the project root "
            "or provide --file with the correct path."
        )

    print(f"Loading retail dataset: {dataset_path}")
    df = pd.read_excel(dataset_path)
    df.columns = [str(column).strip() for column in df.columns]

    missing = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing:
        raise ValueError(f"Dataset is missing required columns: {', '.join(missing)}")

    df = df.dropna(subset=["InvoiceDate"]).sort_values("InvoiceDate").head(args.limit)
    print(f"Dataset loaded. Streaming {len(df)} rows to Kafka at {args.bootstrap_servers}.")

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0
    try:
        for _, row in df.iterrows():
            invoice_no = "" if pd.isna(row["InvoiceNo"]) else str(row["InvoiceNo"])
            stock_code = "" if pd.isna(row["StockCode"]) else str(row["StockCode"])
            description = "" if pd.isna(row["Description"]) else str(row["Description"])
            quantity = 0 if pd.isna(row["Quantity"]) else int(row["Quantity"])
            unit_price = 0.0 if pd.isna(row["UnitPrice"]) else float(row["UnitPrice"])
            customer_id = "" if pd.isna(row["CustomerID"]) else str(int(row["CustomerID"]))
            country = "" if pd.isna(row["Country"]) else str(row["Country"])
            event_time = int(pd.Timestamp(row["InvoiceDate"]).timestamp() * 1000)

            event = {
                "invoiceNo": invoice_no,
                "stockCode": stock_code,
                "description": description,
                "quantity": quantity,
                "unitPrice": unit_price,
                "customerId": customer_id,
                "country": country,
                "eventTime": event_time,
            }

            producer.send("orders", value=event)
            sent += 1

            if sent % 100 == 0:
                producer.flush()
                print(
                    f"Sent {sent}/{len(df)} | Invoice={invoice_no} | Country={country} | "
                    f"Amount=£{quantity * unit_price:.2f}"
                )

            if args.delay_ms:
                time.sleep(args.delay_ms / 1000.0)

        producer.flush()
        print(f"Streaming completed. Sent {sent} events to topic 'orders'.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
