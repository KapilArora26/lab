"""Build the Northwind demo warehouse.

Android checkout conversion in India falls overnight on 2026-11-20. The loss
sits in UPI payments on app version 4.2.1, which rolled out at 01:40 UTC.
Session counts are downsampled. `demo_meta.session_weight` scales revenue at
risk back toward the production volume the case is meant to represent.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

from signal_service.warehouse.duckdb import DuckDBWarehouse

START = datetime(2026, 11, 8, 0, 0, 0)
END = datetime(2026, 11, 20, 8, 0, 0)
ANOMALY_START = datetime(2026, 11, 20, 2, 0, 0)
ORDER_AMOUNT = 48.0
SESSION_WEIGHT = 15.625

# platform, country, channel, payment, sku, version, sessions per hour, anomalous
SEGMENTS: list[tuple] = [
    ("android", "IN", "app", "upi", "SKU-100", "4.2.1", 200, True),
    ("android", "IN", "app", "upi", "SKU-200", "4.2.1", 200, True),
    ("android", "IN", "paid", "upi", "SKU-100", "4.2.1", 200, True),
    ("android", "IN", "paid", "upi", "SKU-200", "4.2.1", 200, True),
    ("android", "IN", "app", "upi", "SKU-100", "4.1.0", 80, False),
    ("android", "IN", "app", "card", "SKU-100", "4.2.1", 80, False),
    ("android", "IN", "app", "card", "SKU-200", "4.1.0", 40, False),
    ("android", "US", "app", "card", "SKU-100", "4.2.1", 150, False),
    ("ios", "IN", "app", "upi", "SKU-100", "5.0.2", 150, False),
    ("web", "US", "web", "card", "SKU-100", "web-1", 150, False),
]

UPI_TEXTS = [
    "UPI payment failed after OTP on Android checkout. The order was not placed.",
    "Stuck on the UPI OTP step. Payment failed and the cart is still open.",
    "Tried twice: UPI payment failed after OTP. Please check the gateway.",
    "Android app 4.2.1: UPI payment failed after the OTP screen.",
    "UPI failure at OTP. I could not complete the purchase on Android.",
    "The UPI payment failed once the OTP arrived. Order still pending.",
]
OTHER_TEXTS = [
    ("Shipping has not moved. Tracking has been idle for six days.", "web", "US", "card", "web-1"),
    ("I cannot log in. The password reset mail never arrives.", "ios", "IN", "upi", "5.0.2"),
    ("The coupon field says the promo code is invalid.", "android", "US", "card", "4.2.1"),
    ("The item arrived damaged. Product quality is not acceptable.", "android", "IN", "card", "4.1.0"),
]
PII_TEXTS = [
    "My UPI payment failed. Email me at jane.doe@example.com when it is fixed.",
    "Payment failed. Call +91 98765 43210 so I can retry the UPI OTP.",
    "Card 4242 4242 4242 4242 was saved and the UPI payment failed after OTP.",
]


def hourly_rate(ts: datetime, anomalous: bool) -> float:
    seasonal = 1 + 0.05 * math.sin(2 * math.pi * ts.hour / 24)
    weekend = 0.98 if ts.weekday() >= 5 else 1.0
    rate = 0.10 * seasonal * weekend
    if anomalous and ts >= ANOMALY_START:
        rate *= 0.86
    return rate


def _hours() -> list[datetime]:
    cursor = START
    hours: list[datetime] = []
    while cursor < END:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    return hours


def build_demo(path: str | Path) -> Path:
    destination = Path(path)
    if destination.exists():
        destination.unlink()
    warehouse = DuckDBWarehouse(destination)
    connection = warehouse.connection
    connection.execute(
        """
        CREATE TABLE groups (
            group_id VARCHAR,
            hour TIMESTAMP,
            platform VARCHAR,
            country VARCHAR,
            channel VARCHAR,
            payment_method VARCHAR,
            sku VARCHAR,
            app_version VARCHAR,
            n_sessions INTEGER,
            n_convert INTEGER,
            n_attempt INTEGER
        )
        """
    )
    rows = []
    for hour in _hours():
        for index, segment in enumerate(SEGMENTS):
            platform, country, channel, payment, sku, version, sessions, anomalous = segment
            rate = hourly_rate(hour, anomalous)
            n_convert = int(round(sessions * rate))
            n_attempt = max(n_convert, int(round(sessions * 0.12)))
            group_id = f"{index}-{hour:%Y%m%d%H}"
            rows.append(
                (
                    group_id,
                    hour,
                    platform,
                    country,
                    channel,
                    payment,
                    sku,
                    version,
                    sessions,
                    n_convert,
                    n_attempt,
                )
            )
    connection.executemany("INSERT INTO groups VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    connection.execute(
        """
        CREATE TABLE sessions AS
        SELECT
            's-' || g.group_id || '-' || r.range::VARCHAR AS session_id,
            g.hour AS ts,
            g.platform,
            g.country,
            g.channel,
            g.payment_method,
            g.sku,
            g.app_version,
            r.range < g.n_convert AS converted
        FROM groups g, range(g.n_sessions::BIGINT) r
        """
    )
    connection.execute(
        """
        CREATE TABLE payments AS
        SELECT
            'p-' || g.group_id || '-' || r.range::VARCHAR AS payment_id,
            CASE WHEN r.range < g.n_convert
                 THEN 'o-' || g.group_id || '-' || r.range::VARCHAR
                 ELSE NULL END AS order_id,
            g.hour AS ts,
            g.payment_method AS method,
            CASE WHEN g.payment_method = 'upi' THEN 'razorpay_upi' ELSE 'stripe' END AS gateway,
            CASE WHEN r.range < g.n_convert THEN 'succeeded' ELSE 'failed' END AS status,
            CASE
                WHEN r.range < g.n_convert THEN NULL
                WHEN g.payment_method = 'upi' THEN 'UPI_OTP_TIMEOUT'
                ELSE 'CARD_DECLINED'
            END AS error_code,
            ?::DOUBLE AS amount_usd,
            g.platform,
            g.country,
            g.app_version
        FROM groups g, range(g.n_attempt::BIGINT) r
        """,
        [ORDER_AMOUNT],
    )
    connection.execute(
        """
        CREATE TABLE orders AS
        SELECT
            'o-' || g.group_id || '-' || r.range::VARCHAR AS order_id,
            's-' || g.group_id || '-' || r.range::VARCHAR AS session_id,
            g.hour AS ts,
            g.platform,
            g.country,
            g.channel,
            g.payment_method,
            g.sku,
            g.app_version,
            ?::DOUBLE AS amount_usd,
            'paid' AS status
        FROM groups g, range(g.n_convert::BIGINT) r
        """,
        [ORDER_AMOUNT],
    )
    connection.execute(
        """
        CREATE TABLE support_tickets (
            ticket_id VARCHAR,
            ts TIMESTAMP,
            platform VARCHAR,
            country VARCHAR,
            channel VARCHAR,
            payment_method VARCHAR,
            sku VARCHAR,
            app_version VARCHAR,
            product_area VARCHAR,
            text_redacted VARCHAR
        )
        """
    )
    tickets: list[tuple] = []
    ticket_number = 1
    for hour in _hours():
        if hour < ANOMALY_START or hour >= END:
            continue
        for offset in range(12):
            text = UPI_TEXTS[(ticket_number + offset) % len(UPI_TEXTS)]
            tickets.append(
                (
                    f"t-{ticket_number:05d}",
                    hour,
                    "android",
                    "IN",
                    "app" if offset % 2 == 0 else "paid",
                    "upi",
                    "SKU-100" if offset % 2 == 0 else "SKU-200",
                    "4.2.1",
                    "unspecified",
                    text,
                )
            )
            ticket_number += 1
        if hour == ANOMALY_START:
            for text in PII_TEXTS:
                tickets.append(
                    (
                        f"t-{ticket_number:05d}",
                        hour,
                        "android",
                        "IN",
                        "app",
                        "upi",
                        "SKU-100",
                        "4.2.1",
                        "unspecified",
                        text,
                    )
                )
                ticket_number += 1
            for text, platform, country, payment, version in OTHER_TEXTS:
                tickets.append(
                    (
                        f"t-{ticket_number:05d}",
                        hour,
                        platform,
                        country,
                        "app",
                        payment,
                        "SKU-100",
                        version,
                        "unspecified",
                        text,
                    )
                )
                ticket_number += 1
    # A thin baseline of unrelated tickets so the table is not only the incident.
    baseline = START
    while baseline < ANOMALY_START:
        text, platform, country, payment, version = OTHER_TEXTS[baseline.day % len(OTHER_TEXTS)]
        tickets.append(
            (
                f"t-{ticket_number:05d}",
                baseline.replace(hour=15),
                platform,
                country,
                "app",
                payment,
                "SKU-100",
                version,
                "unspecified",
                text,
            )
        )
        ticket_number += 1
        baseline += timedelta(days=1)
    connection.executemany(
        "INSERT INTO support_tickets VALUES (?,?,?,?,?,?,?,?,?,?)",
        tickets,
    )
    connection.execute(
        """
        CREATE TABLE deploys (
            deploy_id VARCHAR,
            deployed_at TIMESTAMP,
            platform VARCHAR,
            app_version VARCHAR,
            note VARCHAR
        )
        """
    )
    connection.executemany(
        "INSERT INTO deploys VALUES (?,?,?,?,?)",
        [
            ("d-410", datetime(2026, 11, 1, 9, 0, 0), "android", "4.1.0", "previous stable"),
            ("d-421", datetime(2026, 11, 20, 1, 40, 0), "android", "4.2.1", "full rollout"),
            ("d-502", datetime(2026, 11, 12, 11, 0, 0), "ios", "5.0.2", "full rollout"),
        ],
    )
    connection.execute("CREATE TABLE demo_meta (key VARCHAR, value VARCHAR)")
    connection.executemany(
        "INSERT INTO demo_meta VALUES (?, ?)",
        [
            ("session_weight", str(SESSION_WEIGHT)),
            ("aov_usd", str(ORDER_AMOUNT)),
            ("anomaly_start", ANOMALY_START.isoformat() + "Z"),
            ("seeded_segment", "android|IN"),
            ("seeded_version", "4.2.1"),
            ("seeded_payment_method", "upi"),
        ],
    )
    connection.execute("CHECKPOINT")
    warehouse.close()
    return destination
