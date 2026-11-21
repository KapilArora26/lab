"""Write data/golden/tickets.jsonl. Labels are fixed here; the classifier is not imported."""

from __future__ import annotations

import json
from pathlib import Path

# (text, topic, sentiment, severity, product_area)
ROWS: list[tuple[str, str, str, str, str]] = [
    ("UPI payment failed after OTP on Android checkout. The order was not placed.", "upi_payment_failure", "negative", "high", "payments"),
    ("Stuck on the UPI OTP step. Payment failed and the cart is still open.", "upi_payment_failure", "negative", "high", "payments"),
    ("Tried twice: UPI payment failed after OTP. Please check the gateway.", "upi_payment_failure", "negative", "high", "payments"),
    ("Android app: UPI payment failed after the OTP screen and nothing was debited.", "upi_payment_failure", "negative", "high", "payments"),
    ("UPI failure at OTP. I could not complete the purchase.", "upi_payment_failure", "negative", "high", "payments"),
    ("The UPI payment failed once the OTP arrived. Order still pending.", "upi_payment_failure", "negative", "high", "payments"),
    ("UPI declined after OTP. Checkout did not finish.", "upi_payment_failure", "negative", "high", "payments"),
    ("My card was declined at the last step. CVV was accepted and then rejected.", "card_decline", "negative", "high", "payments"),
    ("Card declined with insufficient funds even though the balance is fine.", "card_decline", "negative", "high", "payments"),
    ("The card was declined again. I did not mistype the CVV.", "card_decline", "negative", "high", "payments"),
    ("Card declined. Bank text says insufficient funds, which is wrong.", "card_decline", "negative", "high", "payments"),
    ("Visa card was declined before the order id appeared.", "card_decline", "negative", "high", "payments"),
    ("Second try: card declined, CVV screen shook and closed.", "card_decline", "negative", "high", "payments"),
    ("Card was declined on a saved card that worked yesterday.", "card_decline", "negative", "high", "payments"),
    ("I want a refund. The parcel contents were not what I bought.", "refund_request", "negative", "medium", "payments"),
    ("Please start a refund. I was charged twice for one order.", "refund_request", "negative", "medium", "payments"),
    ("Money back request: the duplicate charge is still on my statement.", "refund_request", "negative", "medium", "payments"),
    ("Refund has been pending for a week. Charged twice on Monday.", "refund_request", "negative", "medium", "payments"),
    ("Can someone refund the second payment? I was charged twice.", "refund_request", "negative", "medium", "payments"),
    ("Requesting money back because the size was wrong and I already returned it.", "refund_request", "negative", "medium", "payments"),
    ("The refund form will not submit. I need the money back.", "refund_request", "negative", "medium", "payments"),
    ("I cannot log in. The password reset mail never arrives.", "login_issue", "negative", "medium", "account"),
    ("Login spins forever. I can sign in on the website but not the app.", "login_issue", "negative", "medium", "account"),
    ("Password rejected even after a reset. Cannot log in.", "login_issue", "negative", "medium", "account"),
    ("Sign in fails with an unknown error on the account screen.", "login_issue", "negative", "medium", "account"),
    ("Login loop: I enter the password and land back on the form.", "login_issue", "negative", "medium", "account"),
    ("Cannot log in since last night. Password is definitely correct.", "login_issue", "negative", "medium", "account"),
    ("Please help me log in. The sign in button does nothing.", "login_issue", "negative", "medium", "account"),
    ("Shipping has not moved. Tracking has been idle for six days.", "shipping_delay", "negative", "medium", "shipping"),
    ("Where is the delivery? It has not arrived and tracking stopped.", "shipping_delay", "negative", "medium", "shipping"),
    ("The package hasn't arrived. Shipping label was created four days ago.", "shipping_delay", "negative", "medium", "shipping"),
    ("Tracking says in transit but there is no delivery scan.", "shipping_delay", "negative", "medium", "shipping"),
    ("Late delivery. The shipping estimate changed twice.", "shipping_delay", "negative", "medium", "shipping"),
    ("Order not arrived. Tracking number looks unused.", "shipping_delay", "negative", "medium", "shipping"),
    ("Still waiting on delivery. No shipping update this week.", "shipping_delay", "negative", "medium", "shipping"),
    ("The coupon field says the promo code is invalid.", "promo_confusion", "neutral", "low", "checkout"),
    ("Discount code from the banner is rejected at the coupon box.", "promo_confusion", "neutral", "low", "checkout"),
    ("Voucher will not apply. The promo code looks unused.", "promo_confusion", "neutral", "low", "checkout"),
    ("I copied the coupon and the promo code box stays red.", "promo_confusion", "neutral", "low", "checkout"),
    ("Which voucher works? The discount code on the homepage fails.", "promo_confusion", "neutral", "low", "checkout"),
    ("Promo code NEW10 is not recognized in the coupon field.", "promo_confusion", "neutral", "low", "checkout"),
    ("The voucher balance did not change when I entered the discount code.", "promo_confusion", "neutral", "low", "checkout"),
    ("The item arrived damaged. Product quality is not acceptable.", "product_quality", "negative", "medium", "catalog"),
    ("Defective unit. The broken item was cracked out of the box.", "product_quality", "negative", "medium", "catalog"),
    ("Product quality issue: stitching came apart on day one.", "product_quality", "negative", "medium", "catalog"),
    ("Received a damaged product. Please advise on the defective piece.", "product_quality", "negative", "medium", "catalog"),
    ("Broken item, missing a part. This is a product quality complaint.", "product_quality", "negative", "medium", "catalog"),
    ("The mug is defective and chipped. Poor product quality.", "product_quality", "negative", "medium", "catalog"),
    ("Damaged corner and a cracked screen. Defective on arrival.", "product_quality", "negative", "medium", "catalog"),
    ("Checkout button does nothing. I cannot place order.", "checkout_bug", "negative", "medium", "checkout"),
    ("Cart stuck on the review page. Pay button is disabled.", "checkout_bug", "negative", "medium", "checkout"),
    ("I tap place order and checkout reloads with an empty message.", "checkout_bug", "negative", "medium", "checkout"),
    ("The pay button spins and checkout never confirms.", "checkout_bug", "negative", "medium", "checkout"),
    ("Checkout shows a blank total and the pay button is missing.", "checkout_bug", "negative", "medium", "checkout"),
    ("Cart stuck after I edit the address. Cannot place order.", "checkout_bug", "negative", "medium", "checkout"),
    ("Place order is greyed out. Checkout will not proceed.", "checkout_bug", "negative", "medium", "checkout"),
    ("Love the new packaging, the item looks perfect.", "other", "positive", "low", "other"),
    ("Thanks, the app feels faster this week.", "other", "positive", "low", "other"),
    ("Great photos on the product page.", "other", "positive", "low", "other"),
    ("The colors look wonderful and the price is fair.", "other", "positive", "low", "other"),
    ("Where can I see the size guide?", "other", "neutral", "low", "other"),
    ("Do you stock this style in blue?", "other", "neutral", "low", "other"),
    # Paraphrases that omit the fallback keywords. The keyword classifier may miss
    # these; the golden labels stay, and the eval threshold allows that gap.
    ("The collect request never completed once the one-time code was entered.", "upi_payment_failure", "negative", "high", "payments"),
    ("BHIM timed out and the purchase did not go through on my phone.", "upi_payment_failure", "negative", "high", "payments"),
    ("The banking app showed an error at the last step and the rupees stayed put.", "upi_payment_failure", "negative", "high", "payments"),
    ("Two tries on the handset, the collect request expired before an order existed.", "upi_payment_failure", "negative", "high", "payments"),
    ("Android handset in India, the transfer never confirmed after the numeric code.", "upi_payment_failure", "negative", "high", "payments"),
    ("Nothing was debited but the screen said the transfer could not finish.", "upi_payment_failure", "negative", "high", "payments"),
    ("The in-app sheet closed itself after I typed the six digit code.", "upi_payment_failure", "negative", "high", "payments"),
    ("My purchase is missing even though I approved the request in the banking app.", "upi_payment_failure", "negative", "high", "payments"),
]


def main() -> None:
    if len(ROWS) < 50:
        raise SystemExit(f"golden set has {len(ROWS)} rows, need at least 50")
    out = Path(__file__).with_name("tickets.jsonl")
    with out.open("w", encoding="utf-8") as handle:
        for index, row in enumerate(ROWS, start=1):
            text, topic, sentiment, severity, product_area = row
            handle.write(
                json.dumps(
                    {
                        "id": f"g{index:03d}",
                        "text": text,
                        "topic": topic,
                        "sentiment": sentiment,
                        "severity": severity,
                        "product_area": product_area,
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )
    print(f"wrote {len(ROWS)} rows to {out}")


if __name__ == "__main__":
    main()
