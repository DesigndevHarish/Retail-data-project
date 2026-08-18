import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

MASTER_DIR = DATA_DIR / "master"
HISTORICAL_DIR = DATA_DIR / "historical"

PRODUCT_COUNT = 600
STORE_COUNT = 100
CUSTOMER_COUNT = 10_000

HISTORICAL_DAYS = 730

RANDOM_SEED = 42

random.seed(RANDOM_SEED)


# ============================================================
# MASTER DATA
# ============================================================

INDIAN_CITIES = [
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Nagpur", "Maharashtra"),
    ("Nashik", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Gurugram", "Haryana"),
    ("Noida", "Uttar Pradesh"),
    ("Lucknow", "Uttar Pradesh"),
    ("Bengaluru", "Karnataka"),
    ("Mysuru", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Coimbatore", "Tamil Nadu"),
    ("Madurai", "Tamil Nadu"),
    ("Hyderabad", "Telangana"),
    ("Visakhapatnam", "Andhra Pradesh"),
    ("Vijayawada", "Andhra Pradesh"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Surat", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Kochi", "Kerala"),
    ("Thiruvananthapuram", "Kerala"),
    ("Bhopal", "Madhya Pradesh"),
    ("Indore", "Madhya Pradesh"),
    ("Bhubaneswar", "Odisha"),
    ("Chandigarh", "Chandigarh"),
]


PRODUCT_CATALOG = [
    ("Laptops", ["HP", "Dell", "Lenovo", "ASUS", "Acer", "Apple"]),
    ("Mobile Phones", ["Apple", "Samsung", "OnePlus", "Google", "Xiaomi", "Motorola"]),
    ("TVs", ["Samsung", "LG", "Sony", "TCL", "OnePlus", "Hisense"]),
    ("Headphones", ["Sony", "Bose", "JBL", "Apple", "Sennheiser", "Boat"]),
    ("Gaming", ["Sony", "Microsoft", "Nintendo", "Logitech", "Razer"]),
    ("Refrigerators", ["LG", "Samsung", "Whirlpool", "Godrej", "Haier"]),
    ("Washing Machines", ["LG", "Samsung", "Whirlpool", "IFB", "Bosch"]),
    ("Air Conditioners", ["LG", "Samsung", "Voltas", "Daikin", "Blue Star"]),
    ("Cameras", ["Canon", "Nikon", "Sony", "Fujifilm", "GoPro"]),
    ("Smart Watches", ["Apple", "Samsung", "Garmin", "Fossil", "Noise"]),
    ("Tablets", ["Apple", "Samsung", "Lenovo", "Xiaomi", "OnePlus"]),
    ("Vacuum Cleaners", ["Dyson", "Philips", "Eureka", "Karcher", "Agaro"]),
]


PRODUCT_NAMES = {
    "Laptops": [
        "Business Laptop",
        "Gaming Laptop",
        "Ultrabook Laptop",
        "Student Laptop",
        "Professional Laptop",
    ],
    "Mobile Phones": [
        "5G Smartphone",
        "Pro Smartphone",
        "Ultra Smartphone",
        "Budget Smartphone",
        "Flagship Smartphone",
    ],
    "TVs": [
        "4K Smart TV",
        "OLED Smart TV",
        "QLED Smart TV",
        "LED Smart TV",
        "Mini LED Smart TV",
    ],
    "Headphones": [
        "Wireless Headphones",
        "Noise Cancelling Headphones",
        "Bluetooth Earbuds",
        "Gaming Headset",
        "Premium Earbuds",
    ],
    "Gaming": [
        "Gaming Console",
        "Gaming Controller",
        "Gaming Keyboard",
        "Gaming Mouse",
        "Gaming Monitor",
    ],
    "Refrigerators": [
        "Double Door Refrigerator",
        "French Door Refrigerator",
        "Side by Side Refrigerator",
        "Single Door Refrigerator",
    ],
    "Washing Machines": [
        "Front Load Washing Machine",
        "Top Load Washing Machine",
        "Fully Automatic Washing Machine",
    ],
    "Air Conditioners": [
        "1 Ton Split AC",
        "1.5 Ton Split AC",
        "2 Ton Split AC",
        "Inverter AC",
    ],
    "Cameras": [
        "Mirrorless Camera",
        "DSLR Camera",
        "Action Camera",
        "Digital Camera",
    ],
    "Smart Watches": [
        "Fitness Smart Watch",
        "Premium Smart Watch",
        "GPS Smart Watch",
        "Sports Smart Watch",
    ],
    "Tablets": [
        "Android Tablet",
        "Professional Tablet",
        "Gaming Tablet",
        "Student Tablet",
    ],
    "Vacuum Cleaners": [
        "Cordless Vacuum Cleaner",
        "Robot Vacuum Cleaner",
        "Handheld Vacuum Cleaner",
        "Upright Vacuum Cleaner",
    ],
}


CUSTOMER_SEGMENTS = [
    "STANDARD",
    "STANDARD",
    "STANDARD",
    "PREMIUM",
    "PREMIUM",
    "VIP",
]


PAYMENT_METHODS = [
    "UPI",
    "CARD",
    "CASH",
    "EMI",
    "NET_BANKING",
]


# ============================================================
# DIRECTORY SETUP
# ============================================================

def create_directories():
    MASTER_DIR.mkdir(parents=True, exist_ok=True)
    HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# PRODUCT GENERATOR
# ============================================================

def generate_products():

    products = []

    for i in range(1, PRODUCT_COUNT + 1):

        category, brands = random.choice(PRODUCT_CATALOG)

        brand = random.choice(brands)

        product_description = random.choice(PRODUCT_NAMES[category])

        price_ranges = {
            "Laptops": (30000, 180000),
            "Mobile Phones": (8000, 180000),
            "TVs": (20000, 250000),
            "Headphones": (1500, 50000),
            "Gaming": (3000, 80000),
            "Refrigerators": (20000, 150000),
            "Washing Machines": (18000, 90000),
            "Air Conditioners": (25000, 100000),
            "Cameras": (20000, 250000),
            "Smart Watches": (2000, 80000),
            "Tablets": (10000, 120000),
            "Vacuum Cleaners": (5000, 70000),
        }

        min_price, max_price = price_ranges[category]

        price = random.randint(min_price, max_price)

        products.append({
            "product_id": f"PRD{i:05d}",
            "brand": brand,
            "category": category,
            "product_name": f"{brand} {product_description} {random.randint(1, 999)}",
            "regular_price_inr": price,
            "currency": "INR",
            "active": True,
        })

    df = pd.DataFrame(products)

    df.to_csv(
        MASTER_DIR / "products.csv",
        index=False
    )

    print(f"Created {len(df)} products")


# ============================================================
# STORE GENERATOR
# ============================================================

def generate_stores():

    stores = []

    for i in range(1, STORE_COUNT + 1):

        city, state = random.choice(INDIAN_CITIES)

        stores.append({
            "store_id": f"STR{i:04d}",
            "store_name": f"Retail Store {i:03d}",
            "city": city,
            "state": state,
            "country": "India",
            "store_type": random.choice([
                "LARGE",
                "MEDIUM",
                "EXPRESS"
            ]),
        })

    df = pd.DataFrame(stores)

    df.to_csv(
        MASTER_DIR / "stores.csv",
        index=False
    )

    print(f"Created {len(df)} stores")


# ============================================================
# CUSTOMER GENERATOR
# ============================================================

def generate_customers():

    customers = []

    for i in range(1, CUSTOMER_COUNT + 1):

        city, state = random.choice(INDIAN_CITIES)

        customers.append({
            "customer_id": f"CUS{i:06d}",
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),
            "city": city,
            "state": state,
            "country": "India",
        })

    df = pd.DataFrame(customers)

    df.to_csv(
        MASTER_DIR / "customers.csv",
        index=False
    )

    print(f"Created {len(df)} customers")


# ============================================================
# INITIAL INVENTORY
# ============================================================

def generate_initial_inventory(products, stores):

    inventory = {}

    for product in products:

        product_id = product["product_id"]

        for store in stores:

            store_id = store["store_id"]

            quantity = random.randint(0, 100)

            inventory[(product_id, store_id)] = quantity

    return inventory


# ============================================================
# EVENT GENERATOR
# ============================================================

def create_event(
    event_type,
    timestamp,
    product,
    store,
    customer,
    previous_quantity,
    quantity_change,
    current_quantity,
):

    transaction_quantity = abs(quantity_change)

    unit_price = product["regular_price_inr"]

    # Small realistic price variation
    selling_price = round(
        unit_price * random.uniform(0.90, 1.05),
        2
    )

    if current_quantity == 0:
        availability = "OUT_OF_STOCK"

    elif current_quantity <= 10:
        availability = "LOW_STOCK"

    else:
        availability = "IN_STOCK"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": timestamp.isoformat(),

        "product": {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "brand": product["brand"],
            "category": product["category"],
        },

        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "city": store["city"],
            "state": store["state"],
            "country": store["country"],
        },

        "customer": {
   	 "customer_id": customer["customer_id"]
    	 if customer else None,

    	"customer_city": customer["city"]
    	if customer else None,

    	"customer_segment": customer["customer_segment"]
    	if customer else None,
	},

        "inventory": {
            "previous_quantity": previous_quantity,
            "quantity_change": quantity_change,
            "current_quantity": current_quantity,
            "availability": availability,
        },

        "transaction": {
            "quantity": transaction_quantity,
            "unit_price_inr": selling_price,
            "currency": "INR",
            "payment_method": random.choice(PAYMENT_METHODS)
            if event_type == "SALE"
            else None,
        },
    }

    return event


# ============================================================
# HISTORICAL EVENT GENERATION
# ============================================================

def generate_historical_events():

    products_df = pd.read_csv(
        MASTER_DIR / "products.csv"
    )

    stores_df = pd.read_csv(
        MASTER_DIR / "stores.csv"
    )

    customers_df = pd.read_csv(
        MASTER_DIR / "customers.csv"
    )

    products = products_df.to_dict("records")
    stores = stores_df.to_dict("records")
    customers = customers_df.to_dict("records")

    inventory = generate_initial_inventory(
        products,
        stores
    )

    start_date = (
        datetime.now()
        - timedelta(days=HISTORICAL_DAYS)
    )

    event_count = 0

    # We generate approximately 300 events per day.
    # 730 days × 300 ≈ 219,000 events.

    events_per_day = 300

    for day_number in range(HISTORICAL_DAYS):

        current_date = start_date + timedelta(
            days=day_number
        )

        date_folder = (
            HISTORICAL_DIR
            / f"date={current_date.strftime('%Y-%m-%d')}"
        )

        date_folder.mkdir(
            parents=True,
            exist_ok=True
        )

        events = []

        for _ in range(events_per_day):

            product = random.choice(products)

            store = random.choice(stores)

            customer = random.choice(customers)

            product_id = product["product_id"]

            store_id = store["store_id"]

            key = (product_id, store_id)

            previous_quantity = inventory[key]

            event_type = random.choices(
                [
                    "SALE",
                    "RETURN",
                    "INVENTORY_RECEIVED",
                    "INVENTORY_ADJUSTMENT",
                ],
                weights=[
                    65,
                    8,
                    20,
                    7,
                ],
                k=1
            )[0]

            if event_type == "SALE":

                quantity_change = -random.randint(
                    1,
                    min(5, max(1, previous_quantity))
                )

            elif event_type == "RETURN":

                quantity_change = random.randint(
                    1,
                    3
                )

            elif event_type == "INVENTORY_RECEIVED":

                quantity_change = random.randint(
                    10,
                    100
                )

            else:

                quantity_change = random.randint(
                    -5,
                    5
                )

                if quantity_change == 0:
                    quantity_change = 1

            current_quantity = max(
                0,
                previous_quantity + quantity_change
            )

            inventory[key] = current_quantity

            timestamp = current_date + timedelta(
                seconds=random.randint(
                    0,
                    86399
                )
            )

            event = create_event(
                event_type,
                timestamp,
                product,
                store,
                customer,
                previous_quantity,
                quantity_change,
                current_quantity,
            )

            # ------------------------------------------------
            # Intentionally introduce a small amount of
            # data-quality problems.
            # ------------------------------------------------

            problem = random.random()

            if problem < 0.005:

                event["product"]["product_id"] = None

            elif problem < 0.010:

                event["transaction"]["unit_price_inr"] = -100

            elif problem < 0.015:

                event["store"]["store_id"] = None

            elif problem < 0.020:

                event["transaction"]["currency"] = "XXX"

            events.append(event)

            event_count += 1

        output_file = (
            date_folder
            / f"events_{current_date.strftime('%Y%m%d')}.json"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as file:

            for event in events:

                file.write(
                    json.dumps(event)
                    + "\n"
                )

        print(
            f"{current_date.strftime('%Y-%m-%d')} "
            f"→ {len(events)} events"
        )

    print()
    print(
        f"Historical generation complete: "
        f"{event_count:,} events"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("RETAIL DATA PROJECT - DATA GENERATOR")
    print("=" * 60)

    create_directories()

    print("\nGenerating master data...")

    generate_products()

    generate_stores()

    generate_customers()

    print("\nGenerating historical events...")

    generate_historical_events()

    print("\nGeneration completed successfully.")

    print("\nGenerated structure:")
    print("data/")
    print("├── master/")
    print("│   ├── products.csv")
    print("│   ├── stores.csv")
    print("│   └── customers.csv")
    print("│")
    print("└── historical/")
    print("    └── date=YYYY-MM-DD/")
    print("        └── events_YYYYMMDD.json")


if __name__ == "__main__":
    main()