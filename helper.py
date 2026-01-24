import random
from datetime import datetime, timedelta
from database import orders, next_sequence

STATUSES = [
    "pending",
    "confirmed",
    "packed",
    "out_for_delivery",
    "delivered",
    "canceled",
]

def random_past_datetime(days_back=60):
    return datetime.utcnow() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def generate_sample_order():
    status = random.choice(STATUSES)
    ordered_at = random_past_datetime()

    doc = {
        "order_id": next_sequence("order_id"),
        "customer_id": random.randint(1, 15),

        "order_status": status,

        "items": [
            {
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 5),
            }
            for _ in range(random.randint(1, 4))
        ],

        "subtotal": round(random.uniform(20, 150), 2),
        "discount_amount": round(random.uniform(0, 15), 2),
        "discounted_subtotal": 0,  # calculated below
        "tax": round(random.uniform(1, 10), 2),
        "shipping_fee": round(random.uniform(3, 10), 2),
        "total": 0,  # calculated below

        "coupon_code": random.choice([None, "SAVE10", "WELCOME5"]),
        "payment_method": random.choice(["cod", "online"]),
        "payment_transaction_id": None,

        "notes": random.choice([
            None,
            "Leave at door",
            "Call before delivery",
        ]),

        "shipping_address": {
            "full_name": "Test Customer",
            "phone": "+1 555 000 0000",
            "street1": "123 Main St",
            "street2": None,
            "city": "Baltimore",
            "state": "MD",
            "postal_code": "21201",
            "country": "US",
        },

        "ordered_at": ordered_at,

        "confirmed_at": None,
        "packed_at": None,
        "out_for_delivery_at": None,
        "delivered_at": None,
        "canceled_at": None,
    }

    # Calculate totals
    doc["discounted_subtotal"] = round(
        doc["subtotal"] - doc["discount_amount"], 2
    )
    doc["total"] = round(
        doc["discounted_subtotal"] + doc["tax"] + doc["shipping_fee"], 2
    )

    # Apply timestamps based on status
    if status in ["confirmed", "packed", "out_for_delivery", "delivered"]:
        doc["confirmed_at"] = ordered_at + timedelta(hours=2)

    if status in ["packed", "out_for_delivery", "delivered"]:
        doc["packed_at"] = doc["confirmed_at"] + timedelta(hours=4)

    if status in ["out_for_delivery", "delivered"]:
        doc["out_for_delivery_at"] = doc["packed_at"] + timedelta(hours=6)

    if status == "delivered":
        doc["delivered_at"] = doc["out_for_delivery_at"] + timedelta(hours=3)

    if status == "canceled":
        doc["canceled_at"] = ordered_at + timedelta(hours=1)

    return doc


def seed_orders(count=150):
    docs = [generate_sample_order() for _ in range(count)]
    orders.insert_many(docs)
    print(f"✅ Inserted {count} sample orders into 'orders' collection")


if __name__ == "__main__":
    seed_orders(150)
