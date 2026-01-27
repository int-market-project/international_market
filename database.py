# database.py
import re
import uuid
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import certifi
from pymongo import MongoClient, ReturnDocument
from pymongo.server_api import ServerApi

from schemas import *
# ------------------------------------------------------------
# MongoDB connection
# ------------------------------------------------------------

uri = "mongodb+srv://admin:SZQbkuY5R0p7jm6F@maincluster.ytmkxin.mongodb.net/?retryWrites=true&w=majority&appName=MainCluster"
client = MongoClient(uri, tlsCAFile=certifi.where(), server_api=ServerApi('1'))
database = client["main_database"]
print("✔ Connected to:", client.address)

# ------------------------------------------------------------
# Collections
# ------------------------------------------------------------

carts = database["carts"]
categories = database["categories"]
customers = database["customers"]
orders = database["orders"]
products = database["products"]
settings = database["settings"]
transaction_logs = database["transaction_logs"]
login_codes = database["login_codes"]
sessions = database["sessions"]
wishlists = database["wishlists"]
settings = database["settings"]
ratings = database["ratings"]
coupon_codes = database["coupon_codes"]
newsletter_subscription_emails = database["newsletter_subscription_emails"]
recently_viewed_products = database["recently_viewed_products"]
admin_credentials = database["admin_credentials"]
admin_sessions = database["admin_sessions"]
checkout_drafts = database["checkout_drafts"]
counters = database["counters"]
messages = database["messages"]

# ------------------------------------------------------------
# CATEGORY HELPERS
# ------------------------------------------------------------

def create_category(category: Category) -> str:
    """
    Insert a new Category into the `categories` collection.
    Returns the inserted document's ID as a string.
    """
    data = category.dict()
    result = categories.insert_one(data)
    return str(result.inserted_id)

def get_parent_categories_with_meta() -> dict:
    """
    Fetch all parent categories (parent_id == None) and return:
    - total number of parent categories
    - list of parents with: name, slug, image_url (or None), subcategory_count
    """
    # Get all parent categories
    parent_docs = list(categories.find({"parent_id": None}))

    parents_data: list[dict] = []

    for doc in parent_docs:
        slug = doc.get("slug")

        # Count how many subcategories belong to this parent
        sub_count = categories.count_documents({"parent_id": slug})

        parents_data.append({
            "name": doc.get("name"),
            "slug": slug,
            "image_url": doc.get("image_url", None),
            "subcategory_count": sub_count,
        })

    return {
        "total_parent_categories": len(parents_data),
        "parents": parents_data,
    }

# ------------------------------------------------------------
# ID GENERATORS
# ------------------------------------------------------------

def get_next_customer_id() -> int:
    """Returns the next available integer customer_id."""
    last = customers.find_one(sort=[("customer_id", -1)])
    if last and "customer_id" in last:
        return last["customer_id"] + 1
    return 1


def get_next_product_id() -> int:
    """Returns the next available integer product_id."""
    last = products.find_one(sort=[("product_id", -1)])
    if last and "product_id" in last:
        return last["product_id"] + 1
    return 1


def get_next_order_id() -> int:
    """Returns the next available integer order_id."""
    last = orders.find_one(sort=[("order_id", -1)])
    if last and "order_id" in last:
        return last["order_id"] + 1
    return 1


# ------------------------------------------------------------
# CUSTOMERS
# ------------------------------------------------------------

def create_customer(customer: Customer) -> dict:
    """
    Create a new customer document.
    We ignore any customer.customer_id passed in and assign our own.
    """
    next_id = get_next_customer_id()
    data = customer.dict()
    data["customer_id"] = next_id

    customers.insert_one(data)
    return data


def get_customer_by_id(customer_id: int) -> dict | None:
    """Find a customer by customer_id."""
    return customers.find_one({"customer_id": customer_id})


def get_customer_by_email(email: str) -> dict | None:
    """Find a customer by email (normalized to lowercase + stripped)."""
    normalized = email.strip().lower()
    return customers.find_one({"email": normalized})


def get_customer_by_phone(phone: str) -> dict | None:
    """Find a customer by phone."""
    return customers.find_one({"phone": phone})


def list_customers(skip: int = 0, limit: int = 50) -> list[dict]:
    """Return a list of customers (paginated)."""
    cursor = customers.find().skip(skip).limit(limit)
    return list(cursor)


def update_customer(customer_id: int, updated_customer: Customer) -> bool:
    """Update an existing customer."""
    data = updated_customer.dict()
    data["customer_id"] = customer_id  # ensure ID stays same

    result = customers.update_one(
        {"customer_id": customer_id},
        {"$set": data}
    )
    return result.modified_count > 0


def delete_customer(customer_id: int) -> bool:
    """Delete a customer by customer_id."""
    result = customers.delete_one({"customer_id": customer_id})
    return result.deleted_count > 0


# Helper for OTP flow: find or create by email
def get_or_create_customer_by_email(email: str) -> dict:
    """
    Get an existing customer by normalized email, or create a new one with just email.
    Returns the customer document (dict).
    """
    normalized = email.strip().lower()

    existing = get_customer_by_email(normalized)
    if existing:
        return existing

    # Create a minimal customer with just email
    new_customer = Customer(
        customer_id=0,   # will be replaced in create_customer
        email=normalized
    )
    created = create_customer(new_customer)
    return created


# ------------------------------------------------------------
# AUTH: LOGIN CODES (OTP)
# ------------------------------------------------------------

def create_login_code(email: str, code: str, minutes_valid: int = 10) -> dict:
    """
    Create a new login code for the given email and store it in MongoDB.
    """
    login_code = LoginCode(
        email=email,
        code=code,
        expires_at=datetime.utcnow() + timedelta(minutes=minutes_valid),
        used=False
    )
    data = login_code.dict()
    login_codes.insert_one(data)
    return data


def get_valid_login_code(email: str, code: str) -> dict | None:
    """
    Return a login code document that is valid:
    - matches email and code
    - not used
    - not expired
    """
    now = datetime.utcnow()
    return login_codes.find_one({
        "email": email,
        "code": code,
        "used": False,
        "expires_at": {"$gt": now}
    })


def mark_login_code_used(email: str, code: str) -> None:
    """
    Mark a login code as used for this email & code combination.
    """
    login_codes.update_many(
        {"email": email, "code": code, "used": False},
        {"$set": {"used": True}}
    )


# ------------------------------------------------------------
# AUTH: SESSIONS (COOKIE-BASED LOGIN)
# ------------------------------------------------------------

def create_session(customer_id: int, days_valid: int = 1) -> str:
    """
    Create a new session for a customer and return the session_id.
    This session_id is what you will set as a cookie.
    """
    session_id = str(uuid4())
    session = Session(
        session_id=session_id,
        customer_id=customer_id,
        expires_at=datetime.utcnow() + timedelta(days=days_valid)
    )
    sessions.insert_one(session.dict())
    return session_id


def get_session_by_id(session_id: str) -> dict | None:
    """
    Return a session document that is still valid (not expired).
    """
    now = datetime.utcnow()
    return sessions.find_one({
        "session_id": session_id,
        "expires_at": {"$gt": now}
    })


def delete_session(session_id: str) -> None:
    """
    Delete a session (for logout).
    """
    sessions.delete_many({"session_id": session_id})

# ------------------------------------------------------------
# CART & WISHLIST COUNTS
# ------------------------------------------------------------
def get_cart_and_wishlist_counts(customer_id: int) -> tuple[int, int]:
    """
    Returns:
      cart_qty     -> number of cart line items (len(items))
      wishlist_qty -> number of wishlist items (len(items))
    """
    cart_doc = carts.find_one({"customer_id": customer_id})
    wishlist_doc = wishlists.find_one({"customer_id": customer_id})

    # For cart, count DISTINCT line items, not total quantity
    if cart_doc:
        cart_items = cart_doc.get("items", [])
        cart_qty = len(cart_items)
    else:
        cart_qty = 0

    # For wishlist, just count how many product IDs are in the list
    if wishlist_doc:
        wishlist_items = wishlist_doc.get("items", [])
        wishlist_qty = len(wishlist_items)
    else:
        wishlist_qty = 0

    return cart_qty, wishlist_qty

# database.py
# ------------------------------------------------------------
# MongoDB connection
# ------------------------------------------------------------

uri = "mongodb+srv://admin:SZQbkuY5R0p7jm6F@maincluster.ytmkxin.mongodb.net/?retryWrites=true&w=majority&appName=MainCluster"
client = MongoClient(uri, tlsCAFile=certifi.where(), server_api=ServerApi('1'))
database = client["main_database"]
print("✔ Connected to:", client.address)

# ------------------------------------------------------------
# Collections
# ------------------------------------------------------------

carts = database["carts"]
categories = database["categories"]
customers = database["customers"]
orders = database["orders"]
products = database["products"]
settings = database["settings"]
transaction_logs = database["transaction_logs"]
login_codes = database["login_codes"]
sessions = database["sessions"]
wishlists = database["wishlists"]

# ------------------------------------------------------------
# CATEGORY HELPERS
# ------------------------------------------------------------

def create_category(category: Category) -> str:
    """
    Insert a new Category into the `categories` collection.
    Returns the inserted document's ID as a string.
    """
    data = category.dict()
    result = categories.insert_one(data)
    return str(result.inserted_id)

def get_parent_categories_with_meta() -> dict:
    """
    Fetch all parent categories (parent_id == None) and return:
    - total number of parent categories
    - list of parents with: name, slug, image_url (or None), subcategory_count
    """
    # Get all parent categories
    parent_docs = list(categories.find({"parent_id": None}))

    parents_data: list[dict] = []

    for doc in parent_docs:
        slug = doc.get("slug")

        # Count how many subcategories belong to this parent
        sub_count = categories.count_documents({"parent_id": slug})

        parents_data.append({
            "name": doc.get("name"),
            "slug": slug,
            "image_url": doc.get("image_url", None),
            "subcategory_count": sub_count,
        })

    return {
        "total_parent_categories": len(parents_data),
        "parents": parents_data,
    }

# ------------------------------------------------------------
# ID GENERATORS
# ------------------------------------------------------------

def get_next_customer_id() -> int:
    """Returns the next available integer customer_id."""
    last = customers.find_one(sort=[("customer_id", -1)])
    if last and "customer_id" in last:
        return last["customer_id"] + 1
    return 1


def get_next_product_id() -> int:
    """Returns the next available integer product_id."""
    last = products.find_one(sort=[("product_id", -1)])
    if last and "product_id" in last:
        return last["product_id"] + 1
    return 1


def get_next_order_id() -> int:
    """Returns the next available integer order_id."""
    last = orders.find_one(sort=[("order_id", -1)])
    if last and "order_id" in last:
        return last["order_id"] + 1
    return 1


# ------------------------------------------------------------
# CUSTOMERS
# ------------------------------------------------------------

def create_customer(customer: Customer) -> dict:
    """
    Create a new customer document.
    We ignore any customer.customer_id passed in and assign our own.
    """
    next_id = get_next_customer_id()
    data = customer.dict()
    data["customer_id"] = next_id

    customers.insert_one(data)
    return data


def get_customer_by_id(customer_id: int) -> dict | None:
    """Find a customer by customer_id."""
    return customers.find_one({"customer_id": customer_id})


def get_customer_by_email(email: str) -> dict | None:
    """Find a customer by email (normalized to lowercase + stripped)."""
    normalized = email.strip().lower()
    return customers.find_one({"email": normalized})


def get_customer_by_phone(phone: str) -> dict | None:
    """Find a customer by phone."""
    return customers.find_one({"phone": phone})


def list_customers(skip: int = 0, limit: int = 50) -> list[dict]:
    """Return a list of customers (paginated)."""
    cursor = customers.find().skip(skip).limit(limit)
    return list(cursor)


def update_customer(customer_id: int, updated_customer: Customer) -> bool:
    """Update an existing customer."""
    data = updated_customer.dict()
    data["customer_id"] = customer_id  # ensure ID stays same

    result = customers.update_one(
        {"customer_id": customer_id},
        {"$set": data}
    )
    return result.modified_count > 0


def delete_customer(customer_id: int) -> bool:
    """Delete a customer by customer_id."""
    result = customers.delete_one({"customer_id": customer_id})
    return result.deleted_count > 0


# Helper for OTP flow: find or create by email
def get_or_create_customer_by_email(email: str) -> dict:
    """
    Get an existing customer by normalized email, or create a new one with just email.
    Returns the customer document (dict).
    """
    normalized = email.strip().lower()

    existing = get_customer_by_email(normalized)
    if existing:
        return existing

    # Create a minimal customer with just email
    new_customer = Customer(
        customer_id=0,   # will be replaced in create_customer
        email=normalized
    )
    created = create_customer(new_customer)
    return created


# ------------------------------------------------------------
# AUTH: LOGIN CODES (OTP)
# ------------------------------------------------------------

def create_login_code(email: str, code: str, minutes_valid: int = 10) -> dict:
    """
    Create a new login code for the given email and store it in MongoDB.
    """
    login_code = LoginCode(
        email=email,
        code=code,
        expires_at=datetime.utcnow() + timedelta(minutes=minutes_valid),
        used=False
    )
    data = login_code.dict()
    login_codes.insert_one(data)
    return data


def get_valid_login_code(email: str, code: str) -> dict | None:
    """
    Return a login code document that is valid:
    - matches email and code
    - not used
    - not expired
    """
    now = datetime.utcnow()
    return login_codes.find_one({
        "email": email,
        "code": code,
        "used": False,
        "expires_at": {"$gt": now}
    })


def mark_login_code_used(email: str, code: str) -> None:
    """
    Mark a login code as used for this email & code combination.
    """
    login_codes.update_many(
        {"email": email, "code": code, "used": False},
        {"$set": {"used": True}}
    )


# ------------------------------------------------------------
# AUTH: SESSIONS (COOKIE-BASED LOGIN)
# ------------------------------------------------------------

def create_session(customer_id: int, days_valid: int = 1) -> str:
    """
    Create a new session for a customer and return the session_id.
    This session_id is what you will set as a cookie.
    """
    session_id = str(uuid4())
    session = Session(
        session_id=session_id,
        customer_id=customer_id,
        expires_at=datetime.utcnow() + timedelta(days=days_valid)
    )
    sessions.insert_one(session.dict())
    return session_id


def get_session_by_id(session_id: str) -> dict | None:
    """
    Return a session document that is still valid (not expired).
    """
    now = datetime.utcnow()
    return sessions.find_one({
        "session_id": session_id,
        "expires_at": {"$gt": now}
    })


def delete_session(session_id: str) -> None:
    """
    Delete a session (for logout).
    """
    sessions.delete_many({"session_id": session_id})

# ------------------------------------------------------------
# CART & WISHLIST COUNTS
# ------------------------------------------------------------
def get_cart_and_wishlist_counts(customer_id: int) -> tuple[int, int]:
    """
    Returns:
      cart_qty     -> number of cart line items (len(items))
      wishlist_qty -> number of wishlist items (len(items))
    """
    cart_doc = carts.find_one({"customer_id": customer_id})
    wishlist_doc = wishlists.find_one({"customer_id": customer_id})

    # For cart, count DISTINCT line items, not total quantity
    if cart_doc:
        cart_items = cart_doc.get("items", [])
        cart_qty = len(cart_items)
    else:
        cart_qty = 0

    # For wishlist, just count how many product IDs are in the list
    if wishlist_doc:
        wishlist_items = wishlist_doc.get("items", [])
        wishlist_qty = len(wishlist_items)
    else:
        wishlist_qty = 0

    return cart_qty, wishlist_qty

def get_mega_menu_categories(limit: int = 10) -> dict:
    """
    For the Shop mega menu:
      - parent_categories: first `limit` parent categories (parent_id == None/missing/empty)
      - featured_subcategories: first `limit` featured subcategories
        (parent_id != None/empty AND is_featured == True)

    Each featured subcategory will ALSO include:
      - parent_slug: the parent category slug (stored in parent_id)
    """

    # Treat parent_id None/missing/"" as parent categories (matches your other helper style)
    parent_query = {
        "$or": [
            {"parent_id": None},
            {"parent_id": ""},
            {"parent_id": {"$exists": False}},
        ]
    }

    parent_cursor = categories.find(
        parent_query,
        {"_id": 0, "name": 1, "slug": 1}
    ).limit(limit)
    parent_categories = list(parent_cursor)

    # Featured subcategories: must have parent_id (parent slug) and is_featured True
    featured_query = {
        "is_featured": True,
        "parent_id": {"$nin": [None, "", False]},
    }

    featured_cursor = categories.find(
        featured_query,
        {"_id": 0, "name": 1, "slug": 1, "parent_id": 1}
    ).limit(limit)
    featured_raw = list(featured_cursor)

    # Add parent_slug field (this is the key fix)
    featured_subcategories = []
    for s in featured_raw:
        parent_slug = (s.get("parent_id") or "").strip()
        featured_subcategories.append({
            "name": (s.get("name") or "").strip(),
            "slug": (s.get("slug") or "").strip(),
            "parent_slug": parent_slug,  # ✅ parent category slug for routing
        })

    return {
        "parent_categories": parent_categories,
        "featured_subcategories": featured_subcategories,
    }

# ------------------------------------------------------------
# APP SETTINGS
# ------------------------------------------------------------

def get_app_settings() -> dict:
    """
    Fetch the single app settings document from `settings` collection.
    Expected _id: "app_settings"
    """
    doc = settings.find_one({"_id": "app_settings"})
    if not doc:
        # safe fallback so templates don't break
        return {
            "number_to_show": "",
            "number_to_give": "",
            "address": "",
            "email": "",
            "hours": "",
        }

    # Optional: remove _id so it doesn't get in the way
    doc.pop("_id", None)
    return doc

def add_newsletter_subscription_email(email: str) -> bool:
    """
    Adds an email to the newsletter_subscription_emails document's `emails` array.

    Uses:
      - normalization (strip + lowercase)
      - basic validation
      - $addToSet (prevents duplicates)
      - upsert (creates doc if missing)

    Returns True if inserted (or doc created), False otherwise.
    """
    if not email:
        return False

    normalized = email.strip().lower()

    # Basic email validation (simple but effective)
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", normalized):
        return False

    result = newsletter_subscription_emails.update_one(
        {"_id": "newsletter_subscription_emails"},
        {"$addToSet": {"emails": normalized}},
        upsert=True
    )

    # modified_count = 1 if it added a new email
    # upserted_id not None if doc did not exist and got created
    return (result.modified_count > 0) or (result.upserted_id is not None)

def get_parent_category_name_by_product_id(product_id: int) -> Optional[str]:
    """
    Given a product_id, return the *parent category name* using:
    products.category_id (slug) -> categories.slug -> categories.name
    Returns None if product/category not found.
    """
    product = products.find_one(
        {"product_id": product_id},
        {"category_id": 1, "_id": 0}
    )
    if not product:
        return None

    parent_slug = product.get("category_id")
    if not parent_slug:
        return None

    category = categories.find_one(
        {"slug": parent_slug},
        {"name": 1, "_id": 0}
    )
    if not category:
        return None

    return category.get("name")

def get_average_rating(product_id: int, round_to: int = 1) -> float:
    """
    Returns the average rating for a product_id.
    If no ratings exist, returns 0.0
    """
    pipeline = [
        {"$match": {"product_id": product_id}},
        {"$group": {"_id": "$product_id", "avg_rating": {"$avg": "$rating"}}},
    ]

    result = list(ratings.aggregate(pipeline))
    if not result:
        return 0.0

    avg = result[0].get("avg_rating", 0.0) or 0.0
    return round(float(avg), round_to)

def get_featured_products_summary(limit: int | None = None, customer_id: int | None = None):
    query = {"is_featured": True}
    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products.find(query, projection)
    if limit is not None:
        cursor = cursor.limit(int(limit))

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()  # strings

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name", "") if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids)

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,   # ✅ ONLY THIS KEY
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results

def get_hot_deals_products_summary(limit: int | None = None, customer_id: int | None = None):
    products_duplicate = database["products"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit is not None:
        cursor = cursor.limit(int(limit))

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()  # strings

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name", "") if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids)

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,   # ✅ ONLY THIS KEY
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit is not None:
        cursor = cursor.limit(int(limit))

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0) or 0
        discounted = p.get("discounted_price", price)
        discounted = discounted if discounted is not None else price

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = (
            bool(customer_id) and product_id is not None and int(product_id) in cart_product_ids
        )
        in_wishlist = (
            bool(customer_id) and product_id is not None and str(product_id) in wishlist_product_ids
        )

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit:
        cursor = cursor.limit(int(limit))

    # ✅ cart + wishlist lookup once
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()

    results = []
    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0) or 0
        discounted = p.get("discounted_price", price)
        discounted = discounted if discounted is not None else price

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False
        in_wishlist = bool(customer_id) and (int(product_id) in wishlist_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit:
        cursor = cursor.limit(int(limit))

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()

    results = []
    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,  # ✅ NEW NAME
            "in_cart": in_cart,
        })

    return results
    # ✅ Use products_duplicate because that's where is_hot_deal exists
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit:
        cursor = cursor.limit(int(limit))

    # ✅ cart lookup once (only if logged in)
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0)
        discounted = p.get("discounted_price", price)

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        # ✅ NEW: in_cart flag
        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,   # ✅ NEW
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products_duplicate.find({"is_hot_deal": True}, projection)
    if limit:
        cursor = cursor.limit(int(limit))

    results = []

    for p in cursor:
        price = p.get("price", 0)
        discounted = p.get("discounted_price", price)

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        results.append({
            "product_id": p.get("product_id"),
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(p.get("product_id")),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
        })

    return results
    """
    Fetch products with is_hot_deal == True and return a list of dictionaries:
    product_id, name, category_slug, category_name, rating, brand, cover_image, price, discounted_price

    - category_slug is the parent category slug stored in product.category_id
    - category_name is fetched from categories collection
    - rating is computed from ratings collection (average)
    - cover_image is the first element of product.image_urls
    """

    # ✅ Use products_duplicate because that's where you added is_hot_deal via script
    products_duplicate = database["products_duplicate"]

    query = {"is_hot_deal": True}
    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1
    }

    cursor = products_duplicate.find(query, projection)

    if limit is not None:
        cursor = cursor.limit(int(limit))

    results: List[Dict[str, Any]] = []

    for p in cursor:
        product_id = p.get("product_id")
        category_slug = p.get("category_id")

        # Parent category name (by slug)
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name") if cat else ""

        # Average rating (using your existing function)
        avg_rating = get_average_rating(product_id) if isinstance(product_id, int) else 0.0

        # Brand (supports either "Brand" or "brand")
        brand = p.get("Brand") or p.get("brand") or ""

        # Cover image (first image_urls)
        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and len(image_urls) > 0 else ""

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug or "",
            "category_name": category_name,
            "rating": avg_rating,
            "brand": brand,
            "cover_image": cover_image,
            "price": p.get("price", 0.0),
            "discounted_price": p.get("discounted_price", p.get("price", 0.0)),
        })

    return results

def get_latest_products_summary(limit: int = 20, customer_id: int | None = None):
    products_duplicate = database["products"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()  # strings

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name", "") if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids)

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,   # ✅ ONLY THIS KEY
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0) or 0
        discounted = p.get("discounted_price", price)
        discounted = discounted if discounted is not None else price

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = (
            bool(customer_id) and product_id is not None and int(product_id) in cart_product_ids
        )
        in_wishlist = (
            bool(customer_id) and product_id is not None and str(product_id) in wishlist_product_ids
        )

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    # ✅ cart + wishlist lookup once
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()

    results = []
    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0) or 0
        discounted = p.get("discounted_price", price)
        discounted = discounted if discounted is not None else price

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False
        in_wishlist = bool(customer_id) and (int(product_id) in wishlist_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()

    results = []
    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,  # ✅ NEW NAME
            "in_cart": in_cart,
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    # ✅ cart lookup once (only if logged in)
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()

    results = []

    for p in cursor:
        product_id = p.get("product_id")

        price = p.get("price", 0)
        discounted = p.get("discounted_price", price)

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        # ✅ NEW: in_cart flag
        in_cart = bool(customer_id) and (int(product_id) in cart_product_ids) if product_id is not None else False

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
            "in_cart": in_cart,   # ✅ NEW
        })

    return results
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)
        .limit(int(limit))
    )

    results = []

    for p in cursor:
        price = p.get("price", 0)
        discounted = p.get("discounted_price", price)

        discount_percentage = (
            int(((price - discounted) / price) * 100)
            if price and discounted < price else 0
        )

        category_slug = p.get("category_id", "")
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat["name"] if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if image_urls else ""

        results.append({
            "product_id": p.get("product_id"),
            "name": p.get("name", ""),
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(p.get("product_id")),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "discount_percentage": discount_percentage,
        })

    return results
    """
    Fetch latest products sorted by created_at (DESC) and return a list of dictionaries:
    product_id, name, category_slug, category_name, rating, brand, cover_image, price, discounted_price
    """

    # Use products_duplicate (same collection where brand / hot deal exists)
    products_duplicate = database["products_duplicate"]

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "created_at": 1,
        "Brand": 1,
        "brand": 1
    }

    cursor = (
        products_duplicate
        .find({}, projection)
        .sort("created_at", -1)   # 🔑 SORT BY CREATED_AT
        .limit(int(limit))
    )

    results: List[Dict[str, Any]] = []

    for p in cursor:
        product_id = p.get("product_id")
        category_slug = p.get("category_id")

        # Parent category name
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name") if cat else ""

        # Average rating
        avg_rating = get_average_rating(product_id) if isinstance(product_id, int) else 0.0

        # Brand (supports both Brand / brand)
        brand = p.get("Brand") or p.get("brand") or ""

        # Cover image
        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        results.append({
            "product_id": product_id,
            "name": p.get("name", ""),
            "category_slug": category_slug or "",
            "category_name": category_name,
            "rating": avg_rating,
            "brand": brand,
            "cover_image": cover_image,
            "price": p.get("price", 0.0),
            "discounted_price": p.get("discounted_price", p.get("price", 0.0)),
        })

    return results

def get_cart_qty_by_customer_id(customer_id: int) -> int:
    cart = carts.find_one({"customer_id": customer_id}, {"items": 1, "_id": 0})
    if not cart:
        return 0
    items = cart.get("items") or []
    return len(items)

def get_cart_product_ids_set(customer_id: int) -> set[int]:
    doc = carts.find_one({"customer_id": customer_id}, {"items": 1, "_id": 0})
    if not doc:
        return set()

    items = doc.get("items") or []
    out = set()
    for pid in items:
        try:
            out.add(int(pid))
        except Exception:
            pass
    return out

def is_product_in_cart(customer_id: int, product_id: int) -> bool:
    doc = carts.find_one(
        {"customer_id": customer_id, "items": int(product_id)},
        {"_id": 0, "customer_id": 1}
    )
    return doc is not None

def add_product_to_cart(customer_id: int, product_id: int) -> dict:
    # ensure cart doc exists
    carts.update_one(
        {"customer_id": customer_id},
        {"$setOnInsert": {"customer_id": customer_id, "items": [], "created_at": datetime.utcnow()}},
        upsert=True
    )

    # If already in cart, we keep it "Added" (no change)
    if is_product_in_cart(customer_id, product_id):
        return {"in_cart": True, "cart_qty": get_cart_qty_by_customer_id(customer_id)}

    carts.update_one(
        {"customer_id": customer_id},
        {
            "$push": {"items": int(product_id)},
            "$set": {"updated_at": datetime.utcnow()}
        }
    )

    return {"in_cart": True, "cart_qty": get_cart_qty_by_customer_id(customer_id)}

def remove_product_from_cart(customer_id: int, product_id: int) -> dict:
    carts.update_one(
        {"customer_id": customer_id},
        {
            "$pull": {"items": int(product_id)},  # removes ALL occurrences
            "$set": {"updated_at": datetime.utcnow()}
        }
    )
    return {"in_cart": False, "cart_qty": get_cart_qty_by_customer_id(customer_id)}

def get_cart_product_ids_set(customer_id: int) -> set[int]:
    """
    Your cart schema:
    { customer_id: 1, items: [8,5,5,8,...] }

    This returns a set of product_ids that exist in the cart at least once.
    """
    doc = carts.find_one({"customer_id": int(customer_id)}, {"items": 1, "_id": 0})
    if not doc:
        return set()

    items = doc.get("items") or []
    out = set()
    for pid in items:
        try:
            out.add(int(pid))
        except Exception:
            pass
    return out


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except (TypeError, ValueError):
        return float(default)

def _percentage_discount(price: Any, discounted: Any) -> int:
    p = _to_float(price, 0.0)
    d = _to_float(discounted, p)

    if p <= 0:
        return 0

    # only show discount if discounted is actually less
    if d >= p:
        return 0

    return int(((p - d) / p) * 100)

def get_wishlist_product_ids_set(customer_id: int) -> set[str]:
    doc = wishlists.find_one({"customer_id": int(customer_id)}, {"items": 1, "_id": 0}) or {}
    items = doc.get("items", [])
    # DB stores strings
    return set(str(x) for x in items)

def add_item_to_wishlist(customer_id: int, product_id: int) -> int:
    customer_id = int(customer_id)
    pid = str(int(product_id))

    wishlists.update_one(
        {"customer_id": customer_id},
        {"$addToSet": {"items": pid}},
        upsert=True
    )

    doc = wishlists.find_one({"customer_id": customer_id}, {"items": 1, "_id": 0}) or {}
    return len(doc.get("items", []))

def remove_item_from_wishlist(customer_id: int, product_id: int) -> int:
    customer_id = int(customer_id)
    pid = str(int(product_id))

    wishlists.update_one(
        {"customer_id": customer_id},
        {"$pull": {"items": pid}}
    )

    doc = wishlists.find_one({"customer_id": customer_id}, {"items": 1, "_id": 0}) or {}
    return len(doc.get("items", []))

def get_wishlist_qty(customer_id: int) -> int:
    doc = wishlists.find_one({"customer_id": int(customer_id)}, {"items": 1, "_id": 0}) or {}
    return len(doc.get("items", []))

def get_featured_categories_with_counts() -> List[Dict[str, Any]]:
    """
    Returns featured categories with product counts.

    Category doc example:
      { slug: "fresh-produce", is_featured: True, image_url: "..." }

    Product doc example:
      { sub_category_id: "fresh-produce", ... }

    So product count uses: products.sub_category_id == category.slug
    """

    featured_cursor = categories.find(
        {"is_featured": True},
        {"_id": 0, "name": 1, "slug": 1, "image_url": 1}
    )

    results: List[Dict[str, Any]] = []

    for c in featured_cursor:
        slug = (c.get("slug") or "").strip()
        if not slug:
            continue

        items_count = products.count_documents({"sub_category_id": slug})

        results.append({
            "name": c.get("name", "") or "",
            "slug": slug,
            "items_count": int(items_count),
            "image_url": c.get("image_url", "") or "",
        })

    return results
    """
    Returns a list of featured categories with product counts.

    Output fields:
      - name
      - slug
      - items_count
      - image_url
    """

    # 1) get featured categories
    featured_cats_cursor = categories.find(
        {"is_featured": True},
        {"_id": 0, "name": 1, "slug": 1, "image_url": 1}
    )

    results: List[Dict[str, Any]] = []

    for cat in featured_cats_cursor:
        slug = (cat.get("slug") or "").strip()
        if not slug:
            continue

        # 2) count products in this category (products.category_id is the category slug)
        items_count = products.count_documents({"category_id": slug})

        results.append({
            "name": cat.get("name", "") or "",
            "slug": slug,
            "items_count": int(items_count),
            "image_url": cat.get("image_url", "") or "",
        })

    return results

def get_subcategories_with_counts(parent_category_slug: str) -> List[Dict[str, Any]]:
    """
    Returns subcategories of a given parent category with product counts.

    Subcategory doc example:
      { slug: "fruits", parent_id: "fresh-produce", image_url: "..." }

    Product doc example:
      { sub_category_id: "fruits", ... }

    So product count uses: products.sub_category_id == subcategory.slug
    """

    subcategories_cursor = categories.find(
        {"parent_id": parent_category_slug},
        {"_id": 0, "name": 1, "slug": 1, "image_url": 1}
    )

    results: List[Dict[str, Any]] = []

    for subcat in subcategories_cursor:
        slug = (subcat.get("slug") or "").strip()
        if not slug:
            continue

        items_count = products.count_documents({"sub_category_id": slug})

        results.append({
            "name": subcat.get("name", "") or "",
            "slug": slug,
            "items_count": int(items_count),
            "image_url": subcat.get("image_url", "") or "",
        })

    return results

def get_category_name_by_slug(category_slug: str) -> str:
    """
    Given a category slug, return the category name.
    Returns "" if not found.
    """

    slug = (category_slug or "").strip()
    if not slug:
        return ""

    doc = categories.find_one(
        {"slug": slug},
        {"_id": 0, "name": 1}
    )

    return (doc.get("name") if doc else "") or ""

def get_categories_with_subcategories() -> List[Dict[str, Any]]:
    """
    Returns a list of parent categories, each with its subcategories.
    Parent category: parent_id is None / missing / empty
    Subcategory: parent_id == parent category slug

    Output shape:
    [
      {"name": str, "slug": str, "subcategories": [{"name": str, "slug": str}, ...]},
      ...
    ]
    """

    # Pull only what we need (faster + cleaner)
    docs = list(
        categories.find(
            {},
            {"_id": 0, "name": 1, "slug": 1, "parent_id": 1}
        )
    )

    # Group subcategories by parent_id (which is the parent's slug)
    children_by_parent: Dict[str, List[Dict[str, str]]] = {}
    parents: List[Dict[str, str]] = []

    for d in docs:
        name = (d.get("name") or "").strip()
        slug = (d.get("slug") or "").strip()
        parent_id = d.get("parent_id", None)

        # skip broken entries
        if not name or not slug:
            continue

        # Parent category if parent_id is None/missing/empty
        if parent_id is None or (isinstance(parent_id, str) and parent_id.strip() == ""):
            parents.append({"name": name, "slug": slug})
        else:
            parent_key = str(parent_id).strip()
            children_by_parent.setdefault(parent_key, []).append({"name": name, "slug": slug})

    # Build final response
    result: List[Dict[str, Any]] = []
    for p in parents:
        subs = children_by_parent.get(p["slug"], [])
        # optional: sort subcategories by name
        subs.sort(key=lambda x: x["name"].lower())

        result.append(
            {
                "name": p["name"],
                "slug": p["slug"],
                "subcategories": subs,
            }
        )

    # optional: sort parent categories by name
    result.sort(key=lambda x: x["name"].lower())
    return result

def search_products_advanced(
    keyword: str | None = None,
    category: str = "all",
    subcategory: str = "all",
    min_price: float = 0,
    max_price: float = 1000,
    hot_deal: bool = True,
    popular: bool = True,
    sort: str | None = None,  # None/"default", "price_low", "price_high", "rating_high"
    page: int = 1,
    per_page: int = 20,
    customer_id: int | None = None,
):
    """
    Advanced product search with pagination.

    Returns:
      (results, total_count)

    results is the SAME style as get_featured_products_summary():
    [
      {
        "product_id": ...,
        "name": ...,
        "category_slug": ...,
        "category_name": ...,
        "rating": ...,
        "brand": ...,
        "cover_image": ...,
        "price": ...,
        "discounted_price": ...,
        "percentage_discount": ...,
        "in_cart": ...,
        "in_wishlist": ...,
      },
      ...
    ]
    """

    # -----------------------------
    # Normalize inputs
    # -----------------------------
    kw = (keyword or "").strip()
    cat = (category or "all").strip()
    sub = (subcategory or "all").strip()

    if cat == "all":
        sub = "all"

    min_p = _to_float(min_price)
    max_p = _to_float(max_price)
    if min_p is None:
        min_p = 0.0
    if max_p is None:
        max_p = 1000.0

    if float(min_p) > float(max_p):
        min_p, max_p = max_p, min_p

    s = (sort or "").strip().lower()
    if s in ("default", "relevance", ""):
        s = ""

    # pagination normalize
    try:
        page_i = int(page)
    except Exception:
        page_i = 1
    if page_i < 1:
        page_i = 1

    try:
        per_page_i = int(per_page)
    except Exception:
        per_page_i = 20
    if per_page_i < 1:
        per_page_i = 20
    if per_page_i > 100:  # safety cap
        per_page_i = 100

    skip_n = (page_i - 1) * per_page_i

    # -----------------------------
    # Build Mongo query
    # -----------------------------
    query: Dict[str, Any] = {}

    if kw:
        # AND across tokens, each token can match in ANY of the allowed fields
        tokens = [t for t in re.split(r"\s+", kw) if t]

        and_conditions: List[Dict[str, Any]] = []
        for token in tokens:
            safe = re.escape(token)
            and_conditions.append({
                "$or": [
                    {"name": {"$regex": safe, "$options": "i"}},
                    {"description": {"$regex": safe, "$options": "i"}},
                    {"Brand": {"$regex": safe, "$options": "i"}},
                    {"brand": {"$regex": safe, "$options": "i"}},
                    {"long_description": {"$regex": safe, "$options": "i"}},
                ]
            })

        # If tokens exist, enforce ALL tokens
        if and_conditions:
            query["$and"] = and_conditions

    if cat != "all" and cat != "":
        query["category_id"] = cat
        if sub != "all" and sub != "":
            query["sub_category_id"] = sub

    query["discounted_price"] = {"$gte": float(min_p), "$lte": float(max_p)}

    if not hot_deal:
        query["is_hot_deal"] = {"$ne": True}

    if not popular:
        query["is_featured"] = {"$ne": True}

    # ✅ total count for pagination UI
    total_count = int(products.count_documents(query))

    # -----------------------------
    # Projection
    # -----------------------------
    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    # -----------------------------
    # Cursor + Sorting
    # -----------------------------
    cursor = products.find(query, projection)

    if s in ("price_low", "price_low_to_high", "low_to_high"):
        cursor = cursor.sort("discounted_price", 1)
    elif s in ("price_high", "price_high_to_low", "high_to_low"):
        cursor = cursor.sort("discounted_price", -1)

    rating_sort = s in ("rating_high", "ratings_high", "rating_high_to_low", "ratings_high_to_low")

    # ✅ Efficient pagination for non-rating sorts
    if not rating_sort:
        cursor = cursor.skip(skip_n).limit(per_page_i)

    # -----------------------------
    # cart + wishlist lookup once
    # -----------------------------
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()

    # -----------------------------
    # Cache categories (avoid N queries)
    # -----------------------------
    category_name_cache: Dict[str, str] = {}

    def get_category_name(slug: str) -> str:
        if not slug:
            return ""
        if slug in category_name_cache:
            return category_name_cache[slug]
        doc = categories.find_one({"slug": slug}, {"name": 1, "_id": 0})
        name = doc.get("name", "") if doc else ""
        category_name_cache[slug] = name
        return name

    # -----------------------------
    # Build results
    # -----------------------------
    results: List[Dict[str, Any]] = []

    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = get_category_name(category_slug)

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids)

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    # -----------------------------
    # Rating sort (correct but heavier)
    # -----------------------------
    if rating_sort:
        # We fetched ALL matches (no skip/limit yet), sort then slice
        results.sort(key=lambda x: _to_float(x.get("rating", 0)), reverse=True)
        results = results[skip_n: skip_n + per_page_i]

    return results, total_count

def get_wishlist_products_summary(limit: int | None = None, customer_id: int | None = None):
    """
    Return wishlist products for a customer in the SAME output format as get_featured_products_summary().
    """
    if not customer_id:
        return []

    # 1) Get wishlist product ids (stored as strings in wishlists.items)
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id)  # strings
    if not wishlist_product_ids:
        return []

    # Convert to ints for querying products.product_id (which is stored as int in your products collection)
    wishlist_ids_int = []
    for pid in wishlist_product_ids:
        try:
            wishlist_ids_int.append(int(pid))
        except (TypeError, ValueError):
            continue

    if not wishlist_ids_int:
        return []

    # 2) Query products that are in wishlist
    query = {"product_id": {"$in": wishlist_ids_int}}
    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products.find(query, projection)

    # Note: MongoDB won't preserve the same order as wishlist_ids_int by default.
    # We'll reorder after fetching (like a real wishlist should).
    if limit is not None:
        # We'll apply limit after ordering, not here.
        pass

    # 3) One-time lookups for flags
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids_set = wishlist_product_ids  # strings

    # 4) Fetch all, build a map by product_id for ordering
    by_id = {}

    for p in cursor:
        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name", "") if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids_set)

        by_id[int(product_id)] = {
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,  # ✅ SAME KEY as your latest version
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        }

    # 5) Re-order to match wishlist order (and keep only items that still exist)
    ordered_results = []
    for pid in wishlist_ids_int:
        if pid in by_id:
            ordered_results.append(by_id[pid])

    # 6) Apply limit (after ordering)
    if limit is not None:
        ordered_results = ordered_results[: int(limit)]

    return ordered_results

def get_cart_items_basic(customer_id: int) -> list[dict]:
    """
    Cart page rows:
    - One row per UNIQUE product in cart
    - qty is derived from duplicates in carts.items

    Returns list of dicts:
      product_id, name, slug, cover_image, price, qty
    """
    if not customer_id:
        return []

    cart_doc = carts.find_one({"customer_id": customer_id}, {"items": 1, "_id": 0})
    items = cart_doc.get("items", []) if cart_doc else []
    if not items:
        return []

    # qty map (items in your cart doc are ints)
    qty_map: dict[int, int] = {}
    order_unique: list[int] = []
    seen = set()

    for pid in items:
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            continue

        qty_map[pid_int] = qty_map.get(pid_int, 0) + 1
        if pid_int not in seen:
            order_unique.append(pid_int)
            seen.add(pid_int)

    if not order_unique:
        return []

    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "sub_category_id": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
    }

    cursor = products.find({"product_id": {"$in": order_unique}}, projection)

    by_id: dict[int, dict] = {}
    for p in cursor:
        product_id = p.get("product_id")
        if product_id is None:
            continue

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        base_price = _to_float(p.get("price", 0))
        discounted = p.get("discounted_price", None)
        final_price = _to_float(discounted) if discounted is not None else base_price

        slug = p.get("sub_category_id") or p.get("category_id") or ""

        by_id[int(product_id)] = {
            "product_id": int(product_id),
            "name": p.get("name", "") or "",
            "slug": slug,
            "cover_image": cover_image,
            "price": final_price,
            "qty": qty_map.get(int(product_id), 0),
        }

    # preserve "first appearance" order from cart.items
    results: list[dict] = []
    for pid in order_unique:
        row = by_id.get(pid)
        if row and row["qty"] > 0:
            results.append(row)

    return results

def empty_cart(customer_id: int) -> bool:
    """
    Empties the customer's cart (sets items = []).
    Returns True if cart was updated/exists, else False.
    """
    if not customer_id:
        return False

    res = carts.update_one(
        {"customer_id": int(customer_id)},
        {"$set": {"items": [], "updated_at": datetime.utcnow()}},
        upsert=True
    )
    # upsert=True ensures a cart doc exists even if it didn’t before
    return True

TAX_RATE_MD = 0.06

def compute_totals(subtotal: float, discount_amount: float) -> dict:
    subtotal = float(max(0.0, _to_float(subtotal)))
    discount_amount = float(max(0.0, _to_float(discount_amount)))

    if discount_amount > subtotal:
        discount_amount = subtotal

    discounted_subtotal = subtotal - discount_amount
    tax = discounted_subtotal * TAX_RATE_MD
    total = discounted_subtotal + tax

    return {
        "subtotal": round(subtotal, 2),
        "discount_amount": round(discount_amount, 2),
        "discounted_subtotal": round(discounted_subtotal, 2),
        "tax": round(tax, 2),
        "total": round(total, 2),
    }

def validate_coupon_for_subtotal(code: str, customer_id: int, subtotal: float) -> dict:
    """
    Validates coupon WITHOUT marking it as used.
    One-time-per-customer rule is enforced using customer_ids_who_used list.
    """
    code = (code or "").strip().upper()
    subtotal = float(max(0.0, _to_float(subtotal)))

    if not code:
        return {"ok": False, "message": "Please enter a coupon code."}

    coupon = coupon_codes.find_one({"code": code}, {"_id": 0})
    if not coupon:
        return {"ok": False, "message": "Invalid coupon code."}

    now = datetime.utcnow()

    starts_at = coupon.get("starts_at")
    ends_at = coupon.get("ends_at")

    if starts_at and isinstance(starts_at, datetime) and now < starts_at:
        return {"ok": False, "message": "This coupon is not active yet."}

    if ends_at and isinstance(ends_at, datetime) and now > ends_at:
        return {"ok": False, "message": "This coupon has expired."}

    min_subtotal = _to_float(coupon.get("min_order_subtotal", 0))
    if subtotal < min_subtotal:
        return {"ok": False, "message": f"Minimum order subtotal is ${min_subtotal:.2f} for this coupon."}

    audience = (coupon.get("audience") or "all").strip().lower()
    eligible_ids = coupon.get("eligible_customer_ids") or []

    if audience == "customers":
        eligible_ids_int = []
        for x in eligible_ids:
            try:
                eligible_ids_int.append(int(x))
            except (TypeError, ValueError):
                continue
        if int(customer_id) not in eligible_ids_int:
            return {"ok": False, "message": "This coupon is not available for your account."}

    max_uses_total = int(coupon.get("max_uses_total", 0) or 0)
    uses_total = int(coupon.get("uses_total", 0) or 0)
    if max_uses_total > 0 and uses_total >= max_uses_total:
        return {"ok": False, "message": "This coupon has reached its maximum number of uses."}

    used_ids = coupon.get("customer_ids_who_used") or []
    used_ids_int = []
    for x in used_ids:
        try:
            used_ids_int.append(int(x))
        except (TypeError, ValueError):
            continue

    # ✅ one-time per customer
    if int(customer_id) in used_ids_int:
        return {"ok": False, "message": "You have already used this coupon."}

    discount_type = (coupon.get("discount_type") or "").strip().lower()
    discount_value = _to_float(coupon.get("discount_value", 0))

    if discount_type not in {"amount", "percent"} or discount_value <= 0:
        return {"ok": False, "message": "This coupon is configured incorrectly. Please contact support."}

    if discount_type == "amount":
        discount_amount = discount_value
    else:
        discount_amount = subtotal * (discount_value / 100.0)

    if discount_amount > subtotal:
        discount_amount = subtotal

    return {
        "ok": True,
        "message": f"Coupon applied: {code}",
        "discount_amount": round(float(discount_amount), 2),
        "coupon": {
            "code": coupon.get("code"),
            "title": coupon.get("title", ""),
            "description": coupon.get("description", ""),
            "discount_type": discount_type,
            "discount_value": float(discount_value),
            "min_order_subtotal": float(min_subtotal),
            "audience": coupon.get("audience", "all"),
        }
    }

def get_product_details(product_id: int, customer_id: int | None = None) -> dict | None:
    """
    Return ONE product details document for product-details page.
    - Includes all product fields except created_at/updated_at
    - Adds: category_name, subcategory_name, cover_image
    - Adds: price, discounted_price as floats + percentage_discount
    - Adds: rating (average) + ratings_list (all ratings for this product)
    - Adds: in_cart, in_wishlist (based on customer_id)
    - Adds: is_new (product is in latest 20 by created_at)
    - Ratings list is sorted best->worst and includes created_at_formatted
    """

    # --- fetch product WITHOUT created_at/updated_at in output ---
    projection = {"_id": 0, "created_at": 0, "updated_at": 0}
    p = products.find_one({"product_id": int(product_id)}, projection)
    if not p:
        return None

    pid = int(p.get("product_id"))

    # --- compute is_new (latest 20 products by created_at) ---
    latest_cursor = products.find(
        {},
        {"_id": 0, "product_id": 1}
    ).sort("created_at", -1).limit(20)

    latest_ids = {int(d["product_id"]) for d in latest_cursor if d.get("product_id") is not None}
    p["is_new"] = pid in latest_ids

    # --- normalize prices + discount percentage ---
    price = _to_float(p.get("price", 0))
    discounted = _to_float(p.get("discounted_price", price))
    percentage_discount = _percentage_discount(price, discounted)

    p["price"] = price
    p["discounted_price"] = discounted
    p["percentage_discount"] = percentage_discount  # ✅ same key as your cards

    # --- category/subcategory names ---
    category_slug = (p.get("category_id") or "").strip()
    subcategory_slug = (p.get("sub_category_id") or "").strip()

    category_name = ""
    if category_slug:
        cat = categories.find_one({"slug": category_slug}, {"_id": 0, "name": 1})
        category_name = cat.get("name", "") if cat else ""

    subcategory_name = ""
    if subcategory_slug:
        sub = categories.find_one({"slug": subcategory_slug}, {"_id": 0, "name": 1})
        subcategory_name = sub.get("name", "") if sub else ""

    p["category_name"] = category_name
    p["subcategory_name"] = subcategory_name

    # --- cover image ---
    image_urls = p.get("image_urls") or []
    cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""
    p["cover_image"] = cover_image

    # --- brand normalization ---
    p["brand"] = p.get("Brand") or p.get("brand") or ""

    # --- cart/wishlist flags ---
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()  # strings

    p["in_cart"] = bool(customer_id) and (pid in cart_product_ids)
    p["in_wishlist"] = bool(customer_id) and (str(pid) in wishlist_product_ids)

    # --- ratings: average ---
    p["rating"] = get_average_rating(pid)

    # --- ratings list: best rating first + formatted date ---
    ratings_cursor = ratings.find(
        {"product_id": pid},
        {"_id": 0, "name": 1, "rating": 1, "review": 1, "created_at": 1}
    ).sort([("rating", -1), ("created_at", -1)])  # ✅ best -> worst, then newest

    ratings_list = []
    for r in ratings_cursor:
        dt = r.get("created_at")

        # format like: June 8, 2023
        if isinstance(dt, datetime):
            r["created_at_formatted"] = dt.strftime("%B %d, %Y").replace(" 0", " ")
        else:
            r["created_at_formatted"] = ""

        ratings_list.append(r)

    p["ratings_list"] = ratings_list

    return p

def add_recently_viewed_product(customer_id: int, product_id: int, max_items: int = 10) -> None:
    """
    Store up to `max_items` recently viewed products per customer.
    - Each entry keeps product_id + viewed_at
    - If product already exists, move it to most-recent and update viewed_at
    - If list exceeds max_items, drop the oldest (last after ordering)
    """

    if not customer_id or not product_id:
        return

    customer_id = int(customer_id)
    product_id = int(product_id)

    now = datetime.utcnow()

    doc = recently_viewed_products.find_one(
        {"customer_id": customer_id},
        {"_id": 0, "customer_id": 1, "product_ids": 1}
    )

    items = []
    if doc and isinstance(doc.get("product_ids"), list):
        items = doc["product_ids"]

    # Normalize + remove duplicates of this product_id
    normalized = []
    for it in items:
        try:
            pid = int(it.get("product_id"))
        except Exception:
            continue
        if pid == product_id:
            continue
        normalized.append({
            "product_id": pid,
            "viewed_at": it.get("viewed_at", now),
        })

    # Insert current view at the front
    normalized.insert(0, {"product_id": product_id, "viewed_at": now})

    # Keep only most recent max_items
    normalized = normalized[:max_items]

    # Upsert
    recently_viewed_products.update_one(
        {"customer_id": customer_id},
        {"$set": {"customer_id": customer_id, "product_ids": normalized}},
        upsert=True
    )

def add_product_rating(product_id: int, name: str, email: str, review: str, rating: int) -> dict:
    product_id = int(product_id)
    rating = int(rating)

    now = datetime.utcnow()

    doc = {
        "product_id": product_id,
        "name": (name or "").strip(),
        "email": (email or "").strip(),
        "review": (review or "").strip(),
        "rating": rating,
        "created_at": now,
        "updated_at": now,
    }

    ratings.insert_one(doc)
    return doc

def get_admin_by_username(username: str) -> dict | None:
    username = (username or "").strip()
    if not username:
        return None
    return admin_credentials.find_one({"username": username})

def create_admin_session(admin_id: str, days_valid: int = 1) -> str:
    """
    Create an admin session and return session_id.
    """
    session_id = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires_at = now + timedelta(days=days_valid)

    admin_sessions.insert_one({
        "session_id": session_id,
        "admin_id": admin_id,
        "created_at": now,
        "expires_at": expires_at,
    })
    return session_id

def get_admin_session_by_id(session_id: str) -> dict | None:
    if not session_id:
        return None
    doc = admin_sessions.find_one({"session_id": session_id})
    if not doc:
        return None

    # expire check
    expires_at = doc.get("expires_at")
    if expires_at and isinstance(expires_at, datetime) and expires_at < datetime.utcnow():
        admin_sessions.delete_one({"_id": doc["_id"]})
        return None

    return doc

def delete_admin_session(session_id: str) -> None:
    if session_id:
        admin_sessions.delete_one({"session_id": session_id})



# ------------------------------------------------------------
# Coupon Codes: Helpers
# ------------------------------------------------------------

def _normalize_coupon_code(code: str) -> str:
    return (code or "").strip().upper()


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or val == "":
            return default
        return int(val)
    except Exception:
        return default


def parse_csv_int_list(csv_text: str) -> List[int]:
    """
    Convert "1, 2, 3" -> [1,2,3]. Empty -> []
    Removes invalid items safely.
    """
    if not csv_text:
        return []
    parts = [p.strip() for p in csv_text.split(",")]
    out: List[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue
    return out


def parse_datetime_local(dt_str: str) -> Optional[datetime]:
    """
    Accepts HTML <input type="datetime-local"> value like:
    "2026-01-10T13:45"
    Returns datetime or None.
    """
    if not dt_str:
        return None
    dt_str = dt_str.strip()
    if not dt_str:
        return None
    try:
        # datetime-local format
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def get_all_coupon_codes_summary() -> List[Dict[str, Any]]:
    """
    Returns a simple list for manage page.
    Includes: code, title, discount_type, discount_value, audience, uses_total, max_uses_total
    Sorted by created_at desc then code asc.
    """
    projection = {
        "_id": 0,
        "code": 1,
        "title": 1,
        "discount_type": 1,
        "discount_value": 1,
        "audience": 1,
        "uses_total": 1,
        "max_uses_total": 1,
        "created_at": 1,
    }

    docs = list(
        coupon_codes.find({}, projection).sort(
            [("created_at", -1), ("code", 1)]
        )
    )

    # normalize code in output (safety)
    for d in docs:
        d["code"] = _normalize_coupon_code(d.get("code", ""))
    return docs


def get_coupon_code_by_code(code: str) -> Optional[Dict[str, Any]]:
    """
    Returns full coupon code doc (excluding Mongo _id in output).
    """
    code = _normalize_coupon_code(code)
    if not code:
        return None

    doc = coupon_codes.find_one({"code": code}, {"_id": 0})
    if not doc:
        return None

    doc["code"] = _normalize_coupon_code(doc.get("code", ""))
    return doc


def coupon_code_exists(code: str) -> bool:
    code = _normalize_coupon_code(code)
    if not code:
        return False
    return coupon_codes.count_documents({"code": code}, limit=1) > 0


def create_coupon_code(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a coupon code doc.
    Expects normalized + validated data by endpoint (but we still sanitize).
    Returns inserted doc (without _id).
    """
    now = datetime.utcnow()

    code = _normalize_coupon_code(data.get("code", ""))
    if not code:
        raise ValueError("Coupon code is required.")

    # enforce uniqueness
    if coupon_code_exists(code):
        raise ValueError("Coupon code already exists.")

    discount_type = (data.get("discount_type") or "").strip().lower()
    if discount_type not in ("amount", "percent"):
        raise ValueError("Invalid discount_type.")

    audience = (data.get("audience") or "all").strip().lower()
    if audience not in ("all", "customers"):
        raise ValueError("Invalid audience.")

    eligible_ids = data.get("eligible_customer_ids") or []
    if audience == "all":
        eligible_ids = []  # force empty
    else:
        eligible_ids = [int(x) for x in eligible_ids if str(x).strip() != ""]

    doc = {
        "code": code,
        "title": (data.get("title") or "").strip(),
        "description": (data.get("description") or "").strip(),

        "discount_type": discount_type,
        "discount_value": _safe_float(data.get("discount_value"), 0.0),

        "min_order_subtotal": _safe_float(data.get("min_order_subtotal"), 0.0),

        "audience": audience,
        "eligible_customer_ids": eligible_ids,

        "max_uses_total": _safe_int(data.get("max_uses_total"), 0),
        "uses_total": _safe_int(data.get("uses_total"), 0),

        "customer_ids_who_used": [int(x) for x in (data.get("customer_ids_who_used") or [])],

        "starts_at": data.get("starts_at"),
        "ends_at": data.get("ends_at"),

        "created_at": now,
        "updated_at": now,
    }

    # basic guardrails
    if doc["discount_value"] <= 0:
        raise ValueError("discount_value must be greater than 0.")
    if doc["min_order_subtotal"] < 0:
        raise ValueError("min_order_subtotal must be >= 0.")
    if doc["max_uses_total"] < 0 or doc["uses_total"] < 0:
        raise ValueError("max_uses_total / uses_total must be >= 0.")
    if doc["title"] == "":
        raise ValueError("title is required.")

    coupon_codes.insert_one(doc)

    # return the inserted doc (without _id)
    return get_coupon_code_by_code(code)


def update_coupon_code_by_code(code: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Updates an existing coupon code doc by its code.
    Returns updated doc (without _id) or None if not found.
    """
    code = _normalize_coupon_code(code)
    if not code:
        return None

    current = coupon_codes.find_one({"code": code})
    if not current:
        return None

    # never allow changing code (treat it like immutable ID)
    if "code" in updates:
        updates.pop("code", None)

    if "discount_type" in updates:
        dt = (updates.get("discount_type") or "").strip().lower()
        if dt not in ("amount", "percent"):
            raise ValueError("Invalid discount_type.")
        updates["discount_type"] = dt

    if "audience" in updates:
        aud = (updates.get("audience") or "").strip().lower()
        if aud not in ("all", "customers"):
            raise ValueError("Invalid audience.")
        updates["audience"] = aud
        if aud == "all":
            updates["eligible_customer_ids"] = []

    # sanitize numeric fields if present
    if "discount_value" in updates:
        updates["discount_value"] = _safe_float(updates.get("discount_value"), 0.0)
        if updates["discount_value"] <= 0:
            raise ValueError("discount_value must be greater than 0.")

    if "min_order_subtotal" in updates:
        updates["min_order_subtotal"] = _safe_float(updates.get("min_order_subtotal"), 0.0)
        if updates["min_order_subtotal"] < 0:
            raise ValueError("min_order_subtotal must be >= 0.")

    if "max_uses_total" in updates:
        updates["max_uses_total"] = _safe_int(updates.get("max_uses_total"), 0)
        if updates["max_uses_total"] < 0:
            raise ValueError("max_uses_total must be >= 0.")

    # eligible ids sanitize
    if "eligible_customer_ids" in updates:
        updates["eligible_customer_ids"] = [int(x) for x in (updates.get("eligible_customer_ids") or [])]

    updates["updated_at"] = datetime.utcnow()

    coupon_codes.update_one({"code": code}, {"$set": updates})
    return get_coupon_code_by_code(code)


def delete_coupon_code_by_code(code: str) -> bool:
    """
    Deletes coupon by code. Returns True if deleted else False.
    """
    code = _normalize_coupon_code(code)
    if not code:
        return False

    result = coupon_codes.delete_one({"code": code})
    return result.deleted_count == 1




def safe_str(val: Any) -> str:
    return (val or "").strip()


def safe_int_or_none(val: Any) -> Optional[int]:
    try:
        s = safe_str(val)
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def regex_contains(text: str) -> Dict[str, Any]:
    """
    Case-insensitive 'contains' regex.
    Escapes user input to prevent regex injection.
    """
    text = safe_str(text)
    return {"$regex": re.escape(text), "$options": "i"}


def search_customers(
    customer_id: Any = None,
    email: Any = None,
    first_name: Any = None,
    last_name: Any = None,
    phone: Any = None,
    limit: int = 200
) -> List[Dict[str, Any]]:
    """
    Search customers by ANY combination of fields.
    All provided fields are AND'ed together.
    """

    cid = safe_int_or_none(customer_id)
    email = safe_str(email)
    first_name = safe_str(first_name)
    last_name = safe_str(last_name)
    phone = safe_str(phone)

    # Safety: require at least one filter
    if cid is None and not email and not first_name and not last_name and not phone:
        return []

    query: Dict[str, Any] = {}

    if cid is not None:
        query["customer_id"] = cid

    if email:
        query["email"] = regex_contains(email)

    if first_name:
        query["first_name"] = regex_contains(first_name)

    if last_name:
        query["last_name"] = regex_contains(last_name)

    if phone:
        query["phone"] = regex_contains(phone)

    projection = {
        "_id": 0,
        "customer_id": 1,
        "email": 1,
        "first_name": 1,
        "last_name": 1,
        "phone": 1,
    }

    cursor = (
        customers
        .find(query, projection)
        .sort("customer_id", 1)
        .limit(int(limit))
    )

    return list(cursor)



# -----------------------------
# Safe helpers (NO underscores)
# -----------------------------

def safe_str(val: Any) -> str:
    return (val or "").strip()


def normalize_slug(val: Any) -> str:
    return safe_str(val).lower()


def regex_contains(text: str) -> Dict[str, Any]:
    """
    Safe contains regex (case-insensitive).
    Escapes input to avoid regex injection.
    """
    t = safe_str(text)
    return {"$regex": re.escape(t), "$options": "i"}


def parse_parent_id(val: Any) -> Optional[str]:
    """
    - '' / 'null' / None -> None
    - otherwise returns normalized string (slug)
    """
    s = safe_str(val)
    if not s:
        return None
    if s.lower() == "null":
        return None
    return normalize_slug(s)


def parse_checkbox_bool(val: Any) -> bool:
    """
    HTML checkbox usually sends "on" when checked, missing when unchecked.
    Accepts common truthy values.
    """
    if val is None:
        return False
    s = safe_str(val).lower()
    return s in ("1", "true", "on", "yes")


# -----------------------------
# Categories: List / Get
# -----------------------------

def get_all_categories_raw() -> List[Dict[str, Any]]:
    """
    Returns ALL docs from categories collection (parents + subs),
    sorted: parents first (parent_id None), then name.
    """
    projection = {"_id": 0}
    docs = list(
        categories.find({}, projection).sort(
            [("parent_id", 1), ("name", 1)]
        )
    )
    return docs


def get_category_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    slug = normalize_slug(slug)
    if not slug:
        return None
    doc = categories.find_one({"slug": slug}, {"_id": 0})
    return doc


def category_slug_exists(slug: str) -> bool:
    slug = normalize_slug(slug)
    if not slug:
        return False
    return categories.count_documents({"slug": slug}, limit=1) > 0


# -----------------------------
# Categories: View model for "All Categories" page
# -----------------------------

def get_all_categories_grouped() -> List[Dict[str, Any]]:
    """
    Returns a grouped structure:
    [
      {
        parent: {name, slug, image_url, is_featured, parent_id: None},
        subcategories: [{...}, {...}]
      },
      ...
    ]

    Parent categories are those with parent_id == None.
    Subcategories are those with parent_id == parent.slug.
    """
    docs = get_all_categories_raw()

    parents: Dict[str, Dict[str, Any]] = {}
    grouped: List[Dict[str, Any]] = []

    # collect parents
    for d in docs:
        if d.get("parent_id") is None:
            parents[d["slug"]] = d

    # init grouped output
    for pslug, pdoc in parents.items():
        grouped.append({"parent": pdoc, "subcategories": []})

    # map slug -> group index
    group_index = {g["parent"]["slug"]: i for i, g in enumerate(grouped)}

    # attach subcategories
    for d in docs:
        pid = d.get("parent_id")
        if pid is None:
            continue
        pid = normalize_slug(pid)
        if pid in group_index:
            grouped[group_index[pid]]["subcategories"].append(d)
        else:
            # orphan subcategory (parent deleted) — show under a synthetic group
            # so admin can still delete/fix it.
            orphan_slug = "__orphan__"
            if orphan_slug not in group_index:
                grouped.append({
                    "parent": {
                        "name": "(Orphans)",
                        "slug": orphan_slug,
                        "is_featured": False,
                        "image_url": "",
                        "parent_id": None
                    },
                    "subcategories": []
                })
                group_index[orphan_slug] = len(grouped) - 1
            grouped[group_index[orphan_slug]]["subcategories"].append(d)

    # sort subcategories by name
    for g in grouped:
        g["subcategories"] = sorted(g["subcategories"], key=lambda x: (x.get("name") or "").lower())

    # sort groups by parent name (orphans last)
    def group_sort_key(g):
        p = g["parent"]
        if p["slug"] == "__orphan__":
            return ("zzzzzz",)
        return ((p.get("name") or "").lower(),)

    grouped.sort(key=group_sort_key)
    return grouped


# -----------------------------
# Categories: Search
# -----------------------------

def search_categories(
    name: Any = None,
    slug: Any = None,
    parent_id: Any = None,
    is_featured: Any = None,
    limit: int = 5000
) -> List[Dict[str, Any]]:
    """
    AND-based search across provided filters.
    - name/slug/parent_id: contains match (case-insensitive) except parent_id special:
        parent_id = None means parent categories only (parent_id null)
        parent_id = "fresh-produce" means subcats under that parent
    - is_featured: if provided True/False filters exact
    """
    name_s = safe_str(name)
    slug_s = safe_str(slug)

    # parent_id is tricky
    pid_raw = safe_str(parent_id)
    pid = parse_parent_id(pid_raw)  # None if blank/"null"
    pid_was_provided = pid_raw != ""  # detect if admin typed anything

    query: Dict[str, Any] = {}

    if name_s:
        query["name"] = regex_contains(name_s)

    if slug_s:
        query["slug"] = regex_contains(slug_s)

    if pid_was_provided:
        # if admin explicitly searched "null" or left blank? (blank won't be submitted usually)
        # - "null" => parent categories
        if pid is None:
            query["parent_id"] = None
        else:
            query["parent_id"] = pid

    # is_featured: if checkbox on results page, you will pass true/false explicitly
    if is_featured is not None:
        query["is_featured"] = bool(is_featured)

    projection = {"_id": 0}
    cursor = categories.find(query, projection).sort([("parent_id", 1), ("name", 1)]).limit(int(limit))
    return list(cursor)


# -----------------------------
# Categories: Create / Update
# -----------------------------

def create_category(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a category/subcategory doc.
    - slug must be unique
    - parent_id stored as None for parent category
    """
    now = datetime.utcnow()

    name = safe_str(data.get("name"))
    slug = normalize_slug(data.get("slug"))
    image_url = safe_str(data.get("image_url"))
    is_featured = bool(data.get("is_featured", False))

    parent_id = parse_parent_id(data.get("parent_id"))

    if not name:
        raise ValueError("Name is required.")
    if not slug:
        raise ValueError("Slug is required.")
    if category_slug_exists(slug):
        raise ValueError("Slug already exists.")

    # Parent categories should never be featured
    if parent_id is None:
        is_featured = False

    doc = {
        "name": name,
        "slug": slug,
        "is_featured": is_featured,
        "image_url": image_url,
        "parent_id": parent_id,
        "created_at": now,
        "updated_at": now,
    }

    categories.insert_one(doc)
    return get_category_by_slug(slug)


def update_category_by_slug(slug: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Update category by slug.
    IMPORTANT:
    - slug is the identifier (cannot change it here)
    - if parent_id becomes None => force is_featured False
    """
    slug = normalize_slug(slug)
    if not slug:
        return None

    existing = categories.find_one({"slug": slug})
    if not existing:
        return None

    # never allow changing slug via update
    if "slug" in updates:
        updates.pop("slug", None)

    if "name" in updates:
        updates["name"] = safe_str(updates.get("name"))
        if not updates["name"]:
            raise ValueError("Name is required.")

    if "image_url" in updates:
        updates["image_url"] = safe_str(updates.get("image_url"))

    if "parent_id" in updates:
        updates["parent_id"] = parse_parent_id(updates.get("parent_id"))

    if "is_featured" in updates:
        updates["is_featured"] = bool(updates.get("is_featured"))

    # if parent => force is_featured false
    if updates.get("parent_id", existing.get("parent_id")) is None:
        updates["is_featured"] = False

    updates["updated_at"] = datetime.utcnow()

    categories.update_one({"slug": slug}, {"$set": updates})
    return get_category_by_slug(slug)


# -----------------------------
# Categories: Delete
# -----------------------------

def delete_category_by_slug(slug: str) -> Dict[str, Any]:
    """
    Deletes a category by slug.

    IMPORTANT decision:
    - If deleting a parent category => cascade delete its subcategories too,
      to avoid orphan subcategories.
    Returns:
      {"ok": True, "deleted": <count>}
    """
    slug = normalize_slug(slug)
    if not slug:
        return {"ok": False, "deleted": 0}

    # if this is a parent, delete its subcategories too
    doc = categories.find_one({"slug": slug})
    if not doc:
        return {"ok": False, "deleted": 0}

    deleted = 0

    # cascade: delete children where parent_id == this slug
    if doc.get("parent_id") is None:
        res_children = categories.delete_many({"parent_id": slug})
        deleted += int(res_children.deleted_count)

    res_self = categories.delete_one({"slug": slug})
    deleted += int(res_self.deleted_count)

    return {"ok": True, "deleted": deleted}




def search_products_admin(
    product_id: int | None = None,
    name: str | None = None,
    category_id: str | None = None,
    sub_category_id: str | None = None,
    is_featured: bool | None = None,
    is_hot_deal: bool | None = None,
):
    """
    Admin-only structured product search.
    All provided filters are AND'ed together.
    """
    query: Dict[str, Any] = {}

    if product_id is not None:
        query["product_id"] = int(product_id)

    if name:
        query["name"] = {"$regex": name.strip(), "$options": "i"}

    if category_id and category_id != "all":
        query["category_id"] = category_id.strip()

        if sub_category_id and sub_category_id != "all":
            query["sub_category_id"] = sub_category_id.strip()

    # ✅ Optional checkbox filters (only apply when not None)
    if is_featured is True:
        query["is_featured"] = True

    if is_hot_deal is True:
        query["is_hot_deal"] = True

    return list(products.find(query, {"_id": 0}).sort("product_id", 1))


def get_product_by_id_admin(product_id: int) -> Dict[str, Any] | None:
    return products.find_one(
        {"product_id": int(product_id)},
        {"_id": 0}
    )


def delete_product_by_id_admin(product_id: int) -> bool:
    res = products.delete_one({"product_id": int(product_id)})
    return res.deleted_count == 1


def create_product_admin(product_data: Dict[str, Any]) -> Dict[str, Any]:
    product_data["created_at"] = datetime.utcnow()
    product_data["updated_at"] = datetime.utcnow()

    products.insert_one(product_data)
    return product_data


def update_product_admin(product_id: int, product_data: Dict[str, Any]) -> bool:
    product_data["updated_at"] = datetime.utcnow()

    res = products.update_one(
        {"product_id": int(product_id)},
        {"$set": product_data}
    )

    return res.matched_count == 1



def get_next_product_id() -> int:
    doc = products.find_one(
        {},
        {"product_id": 1},
        sort=[("product_id", -1)]
    )
    return (doc["product_id"] + 1) if doc else 1



# database.py

def _utcnow():
    return datetime.utcnow()

def next_sequence(name: str) -> int:
    doc = counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True
    )
    return int(doc["seq"])

# -------------------------------
# Checkout Draft (temporary)
# -------------------------------

def upsert_checkout_draft(customer_id: int, draft: dict) -> None:
    """
    Stores the cart->checkout payload temporarily for this customer.
    We keep it simple: one active draft per customer.
    """
    checkout_drafts.update_one(
        {"customer_id": int(customer_id)},
        {"$set": {"draft": draft, "updated_at": _utcnow()}},
        upsert=True
    )

def get_checkout_draft(customer_id: int) -> dict | None:
    doc = checkout_drafts.find_one({"customer_id": int(customer_id)}, {"_id": 0})
    if not doc:
        return None
    return doc.get("draft")

def delete_checkout_draft(customer_id: int) -> None:
    checkout_drafts.delete_one({"customer_id": int(customer_id)})

# -------------------------------
# Orders
# -------------------------------

# -------------------------------
# Orders
# -------------------------------



def create_order_from_draft(
    customer_id: int,
    draft: dict,
    payment_method: str,
    shipping_address: dict,
    notes: str | None = None
) -> int:
    order_id = next_sequence("order_id")
    now = datetime.utcnow()

    doc = {
        "order_id": int(order_id),
        "customer_id": int(customer_id),

        "order_status": "pending",

        "items": draft.get("items", []),

        "subtotal": float(draft.get("subtotal", 0)),
        "discount_amount": float(draft.get("discount_amount", 0)),
        "discounted_subtotal": float(draft.get("discounted_subtotal", 0)),
        "tax": float(draft.get("tax", 0)),
        "shipping_fee": float(draft.get("shipping_fee", 0)),
        "total": float(draft.get("total", 0)),

        "coupon_code": draft.get("coupon_code"),
        "payment_method": payment_method,  # "cod" | "online"
        "payment_transaction_id": None,

        "notes": notes,
        "shipping_address": shipping_address,

        # NEW timeline fields (you already added these ✅)
        "ordered_at": now,
        "confirmed_at": None,
        "packed_at": None,
        "out_for_delivery_at": None,
        "delivered_at": None,
        "canceled_at": None,
    }

    orders.insert_one(doc)
    return order_id


def attach_transaction_to_order(order_id: int, transaction_id: str) -> None:
    orders.update_one(
        {"order_id": int(order_id)},
        {"$set": {"payment_transaction_id": str(transaction_id)}}
    )


def mark_order_paid(order_id: int) -> None:
    """
    UPDATED:
    paid_at no longer exists in the Order schema.

    Keep this function so your code doesn't break if it's still called somewhere,
    but make it harmless (no-op) OR optionally store a generic timestamp if you want.
    """
    # Option A (recommended): no-op
    return

    # Option B (if you want to store something): uncomment and use this instead
    # orders.update_one(
    #     {"order_id": int(order_id)},
    #     {"$set": {"updated_at": _utcnow()}}
    # )


def get_order_for_customer(order_id: int, customer_id: int) -> dict | None:
    return orders.find_one(
        {"order_id": int(order_id), "customer_id": int(customer_id)},
        {"_id": 0}
    )
# -------------------------------
# Transaction Logs
# -------------------------------

def create_transaction_log(order_id: int, customer_id: int, payment_method: str, amount: float,
                           status: str = "created", provider: str | None = None,
                           provider_payment_intent_id: str | None = None) -> str:
    tx_id = str(uuid.uuid4())
    doc = {
        "transaction_id": tx_id,
        "order_id": int(order_id),
        "customer_id": int(customer_id),
        "payment_method": payment_method,  # "cod" | "online"
        "status": status,                  # "created" | "pending" | "succeeded" | ...
        "amount": float(amount),
        "provider": provider,              # "stripe" or None
        "provider_payment_intent_id": provider_payment_intent_id,
        "created_at": _utcnow(),
    }
    transaction_logs.insert_one(doc)
    return tx_id

def mark_transaction_succeeded(transaction_id: str) -> None:
    transaction_logs.update_one(
        {"transaction_id": str(transaction_id)},
        {"$set": {"status": "succeeded"}}
    )

def mark_transaction_failed(transaction_id: str) -> None:
    transaction_logs.update_one(
        {"transaction_id": str(transaction_id)},
        {"$set": {"status": "failed"}}
    )

def set_transaction_payment_intent(transaction_id: str, payment_intent_id: str) -> None:
    transaction_logs.update_one(
        {"transaction_id": str(transaction_id)},
        {"$set": {"provider": "stripe", "provider_payment_intent_id": str(payment_intent_id)}}
    )

def get_transaction_by_payment_intent(payment_intent_id: str) -> dict | None:
    return transaction_logs.find_one(
        {"provider_payment_intent_id": str(payment_intent_id)},
        {"_id": 0}
    )





def get_shipping_fee() -> float:
    """
    Fetch fixed shipping fee from settings collection doc: {_id: "shipping_fee", value: <number>}
    Returns default 4.95 if not found or invalid.
    """
    doc = settings.find_one({"_id": "shipping_fee"}, {"_id": 0, "value": 1})
    if not doc:
        return 4.95

    try:
        val = float(doc.get("value", 4.95))
        if val < 0:
            return 4.95
        return val
    except (TypeError, ValueError):
        return 4.95




def get_latest_order_id_for_customer(customer_id: int) -> int | None:
    doc = orders.find_one(
        {"customer_id": int(customer_id)},
        sort=[("ordered_at", -1)],
        projection={"_id": 0, "order_id": 1}
    )
    return doc.get("order_id") if doc else None





# Assumes you already have:
# - transaction_logs = db["transaction_logs"]
# - safe_str(...) helper (optional)
# - and standard imports

def get_recent_transaction_logs(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Return latest transaction logs sorted by created_at desc.
    Projection excludes _id.
    """
    projection = {"_id": 0}
    cursor = (
        transaction_logs
        .find({}, projection)
        .sort([("created_at", -1)])
        .limit(int(limit))
    )
    return list(cursor)


def get_transaction_logs_by_customer_id(customer_id: int, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Return transaction logs for a single customer, newest first.
    """
    projection = {"_id": 0}
    cursor = (
        transaction_logs
        .find({"customer_id": int(customer_id)}, projection)
        .sort([("created_at", -1)])
        .limit(int(limit))
    )
    return list(cursor)



# expects you already have these collections in database.py
# orders = db["orders"]
# transaction_logs = db["transaction_logs"]

OPEN_STATUSES = {"pending", "confirmed", "packed", "out_for_delivery"}
CLOSED_STATUSES = {"delivered", "canceled"}

# maps status -> timestamp field to set when entering that status
STATUS_TS_FIELD = {
    "confirmed": "confirmed_at",
    "packed": "packed_at",
    "out_for_delivery": "out_for_delivery_at",
    "delivered": "delivered_at",
    "canceled": "canceled_at",
}

# allowed transitions
ALLOWED_TRANSITIONS = {
    "pending": {"confirmed", "canceled"},
    "confirmed": {"packed"},
    "packed": {"out_for_delivery"},
    "out_for_delivery": {"delivered"},
    "delivered": set(),
    "canceled": set(),
}


# -----------------------------
# Orders: Get / List / Search
# -----------------------------

def get_order_by_order_id(order_id: int) -> Optional[Dict[str, Any]]:
    """
    Return one order doc by order_id (projection excludes _id).
    """
    try:
        oid = int(order_id)
    except Exception:
        return None

    return orders.find_one({"order_id": oid}, {"_id": 0})


def get_open_orders_by_status() -> Dict[str, List[Dict[str, Any]]]:
    """
    Return ALL open orders grouped into 4 lists:
      pending / confirmed / packed / out_for_delivery
    Sorted newest first (ordered_at desc).

    Returns:
      {
        "pending": [...],
        "confirmed": [...],
        "packed": [...],
        "out_for_delivery": [...]
      }
    """
    projection = {"_id": 0}
    cursor = (
        orders.find({"order_status": {"$in": list(OPEN_STATUSES)}}, projection)
        .sort([("ordered_at", -1), ("order_id", -1)])
    )
    docs = list(cursor)

    grouped = {"pending": [], "confirmed": [], "packed": [], "out_for_delivery": []}
    for o in docs:
        st = (o.get("order_status") or "").strip()
        if st in grouped:
            grouped[st].append(o)

    return grouped


def search_orders_by_customer_id(customer_id: int, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Return ALL orders for a customer (open + closed), newest first.
    """
    cid = int(customer_id)
    projection = {"_id": 0}
    cursor = (
        orders.find({"customer_id": cid}, projection)
        .sort([("ordered_at", -1), ("order_id", -1)])
        .limit(int(limit))
    )
    return list(cursor)


# -----------------------------
# Orders: Status + timestamps
# -----------------------------

def can_transition_order_status(current_status: str, new_status: str) -> bool:
    current_status = (current_status or "").strip()
    new_status = (new_status or "").strip()
    allowed = ALLOWED_TRANSITIONS.get(current_status, set())
    return new_status in allowed


def update_transaction_status_by_transaction_id(transaction_id: str, new_status: str) -> bool:
    """
    Update transaction_logs.status for a given transaction_id.
    Returns True if modified.
    """
    tid = (transaction_id or "").strip()
    if not tid:
        return False

    res = transaction_logs.update_one(
        {"transaction_id": tid},
        {"$set": {"status": new_status}}
    )
    return int(res.modified_count) > 0


def update_order_status_with_message(
    order_id: int,
    new_status: str,
    admin_message: str = "",
) -> Dict[str, Any]:
    """
    Core mutation used by admin controls.

    - Validates transition rules:
        pending -> confirmed/canceled
        confirmed -> packed
        packed -> out_for_delivery
        out_for_delivery -> delivered
      delivered/canceled terminal

    - Sets timestamp field based on new status:
        confirmed_at / packed_at / out_for_delivery_at / delivered_at / canceled_at

    - Stores admin_message into order.notes (append style) so you have an audit trail.

    - If new_status == delivered and payment_method == "cod":
        and payment_transaction_id exists:
          transaction_logs.status -> "succeeded"

    Returns:
      {"ok": True, "order": <updated order>}
      or {"ok": False, "detail": "..."} on failure.
    """
    try:
        oid = int(order_id)
    except Exception:
        return {"ok": False, "detail": "Invalid order id."}

    new_status = (new_status or "").strip()
    if new_status not in OPEN_STATUSES and new_status not in CLOSED_STATUSES:
        return {"ok": False, "detail": "Invalid status."}

    existing = orders.find_one({"order_id": oid})
    if not existing:
        return {"ok": False, "detail": "Order not found."}

    current_status = (existing.get("order_status") or "pending").strip()

    if not can_transition_order_status(current_status, new_status):
        return {
            "ok": False,
            "detail": f"Invalid transition: {current_status} -> {new_status}"
        }

    now = datetime.utcnow()

    updates: Dict[str, Any] = {"order_status": new_status}

    # set timestamp for this status
    ts_field = STATUS_TS_FIELD.get(new_status)
    if ts_field:
        updates[ts_field] = now

    # append admin message into notes (audit trail)
    msg = (admin_message or "").strip()
    if msg:
        prev_notes = existing.get("notes") or ""
        stamp = now.strftime("%Y-%m-%d %H:%M UTC")
        block = f"[ADMIN {new_status.upper()} @ {stamp}]\n{msg}".strip()
        if prev_notes.strip():
            updates["notes"] = prev_notes.rstrip() + "\n\n" + block
        else:
            updates["notes"] = block

    orders.update_one({"order_id": oid}, {"$set": updates})

    # COD delivered => mark transaction succeeded (if linked)
    try:
        if new_status == "delivered":
            pm = (existing.get("payment_method") or "").strip()
            if pm == "cod":
                tid = (existing.get("payment_transaction_id") or "").strip()
                if tid:
                    update_transaction_status_by_transaction_id(tid, "succeeded")
    except Exception:
        # do not fail the order update because of tx log update
        pass

    updated = get_order_by_order_id(oid)
    return {"ok": True, "order": updated}


# -----------------------------
# Orders: Helper to determine "open" vs "past"
# -----------------------------

def is_open_order(order_doc: Dict[str, Any]) -> bool:
    return (order_doc.get("order_status") or "").strip() in OPEN_STATUSES


def is_past_order(order_doc: Dict[str, Any]) -> bool:
    return (order_doc.get("order_status") or "").strip() in CLOSED_STATUSES





def get_shipping_fee_setting() -> dict:
    """
    Returns the shipping_fee setting document.
    Ensures it exists (creates default if missing).
    """
    doc = settings.find_one({"_id": "shipping_fee"}, {"_id": 1, "value": 1, "updated_at": 1})
    if doc:
        return doc

    default_doc = {
        "_id": "shipping_fee",
        "value": 0.0,
        "updated_at": datetime.utcnow(),
    }
    settings.insert_one(default_doc)
    return default_doc


def get_shipping_fee_value() -> float:
    """
    Returns shipping fee as float.
    """
    doc = get_shipping_fee_setting()
    try:
        return float(doc.get("value", 0) or 0)
    except Exception:
        return 0.0


def update_shipping_fee_value(new_value: float) -> dict:
    """
    Updates shipping fee value (float) + updated_at.
    Returns the updated doc.
    """
    fee = float(new_value)
    if fee < 0:
        raise ValueError("Shipping fee cannot be negative.")

    now = datetime.utcnow()

    settings.update_one(
        {"_id": "shipping_fee"},
        {"$set": {"value": fee, "updated_at": now}},
        upsert=True
    )

    return settings.find_one({"_id": "shipping_fee"}, {"_id": 1, "value": 1, "updated_at": 1})





def get_app_settings() -> dict:
    """
    Returns the app_settings document.
    Creates default if missing.
    """
    doc = settings.find_one({"_id": "app_settings"}, {"_id": 0})
    if doc:
        return doc

    default_doc = {
        "_id": "app_settings",
        "number_to_show": "+1 (240) 555-0123",
        "number_to_give": "+12405550456",
        "address": "123 Main St, Rockville, MD 20850",
        "email": "support@yourstore.com",
        "hours": "Mon–Sat 9am–9pm, Sun 10am–6pm",
        "updated_at": datetime.utcnow(),
    }

    settings.insert_one(default_doc)
    default_doc.pop("_id", None)
    return default_doc


def update_app_settings(data: dict) -> dict:
    """
    Updates app-wide settings.
    """
    now = datetime.utcnow()

    update = {
        "number_to_show": data.get("number_to_show", "").strip(),
        "number_to_give": data.get("number_to_give", "").strip(),
        "address": data.get("address", "").strip(),
        "email": data.get("email", "").strip(),
        "hours": data.get("hours", "").strip(),
        "updated_at": now,
    }

    settings.update_one(
        {"_id": "app_settings"},
        {"$set": update},
        upsert=True
    )

    return get_app_settings()




# ---------- helpers ----------

def _start_of_day_utc(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day)

def _start_of_week_utc(dt: datetime) -> datetime:
    # Monday as start of week
    start = _start_of_day_utc(dt)
    return start - timedelta(days=start.weekday())

def _start_of_month_utc(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1)


# ---------- main analytics function ----------

def get_admin_analytics_snapshot(top_n: int = 10) -> Dict[str, Any]:
    """
    Returns a single analytics snapshot dict that your admin analytics page can render.
    Uses Mongo aggregation where it matters.

    Notes:
    - Revenue uses orders with order_status == "delivered" (real revenue).
    - Orders today/week/month uses ordered_at time window (all orders created).
    - Payments split uses transaction_logs.payment_method (all logs), not order.payment_method.
      (If you prefer orders.payment_method instead, tell me and I'll switch it.)
    """

    now = datetime.utcnow()
    day_start = _start_of_day_utc(now)
    week_start = _start_of_week_utc(now)
    month_start = _start_of_month_utc(now)

    # -----------------------------
    # Orders + Revenue + AOV
    # -----------------------------

    total_orders = orders.count_documents({})

    # delivered revenue + delivered orders count
    delivered_group = list(orders.aggregate([
        {"$match": {"order_status": "delivered"}},
        {"$group": {
            "_id": None,
            "revenue": {"$sum": "$total"},
            "count": {"$sum": 1}
        }}
    ]))
    delivered_revenue = float(delivered_group[0]["revenue"]) if delivered_group else 0.0
    delivered_count = int(delivered_group[0]["count"]) if delivered_group else 0

    avg_order_value = (delivered_revenue / delivered_count) if delivered_count > 0 else 0.0

    orders_today = orders.count_documents({"ordered_at": {"$gte": day_start}})
    orders_this_week = orders.count_documents({"ordered_at": {"$gte": week_start}})
    orders_this_month = orders.count_documents({"ordered_at": {"$gte": month_start}})

    # revenue windows (delivered only)
    revenue_today = _sum_orders_total_by_status_since("delivered", day_start)
    revenue_this_week = _sum_orders_total_by_status_since("delivered", week_start)
    revenue_this_month = _sum_orders_total_by_status_since("delivered", month_start)

    total_canceled_orders = orders.count_documents({"order_status": "canceled"})

    # -----------------------------
    # Customers
    # -----------------------------

    total_customers = customers.count_documents({})

    customers_with_orders = _count_customers_with_at_least_one_order()

    # created_at-based (as per your customer docs)
    new_customers_this_week = customers.count_documents({"created_at": {"$gte": week_start}})
    new_customers_this_month = customers.count_documents({"created_at": {"$gte": month_start}})

    # -----------------------------
    # Payments (COD vs Online)
    # -----------------------------

    payment_split = list(transaction_logs.aggregate([
        {"$group": {"_id": "$payment_method", "count": {"$sum": 1}}}
    ]))
    cod_count = 0
    online_count = 0
    for row in payment_split:
        pm = (row.get("_id") or "").strip().lower()
        c = int(row.get("count") or 0)
        if pm == "cod":
            cod_count = c
        elif pm == "online":
            online_count = c

    total_payments = cod_count + online_count
    cod_pct = (cod_count / total_payments * 100.0) if total_payments else 0.0
    online_pct = (online_count / total_payments * 100.0) if total_payments else 0.0

    # -----------------------------
    # Products: top sold by quantity + top ordered by order count
    # -----------------------------

    top_sold_by_qty = _top_products_sold_by_quantity(top_n=top_n)
    top_ordered_by_order_count = _top_products_by_order_count(top_n=top_n)

    # unique product_ids sold (across all orders)
    unique_sold_product_ids = _unique_sold_product_ids_set()
    sold_unique_count = len(unique_sold_product_ids)

    total_products = products.count_documents({})
    never_sold_count = max(0, int(total_products) - int(sold_unique_count))

    # -----------------------------
    # Ratings: top rated products
    # (min 3 ratings to avoid 1 review dominating)
    # -----------------------------

    top_rated_products = _top_rated_products(top_n=top_n, min_ratings=3)

    # -----------------------------
    # Recently viewed products (global popularity)
    # Count how many times product_id appears across all customers' lists
    # -----------------------------

    top_viewed_products = _top_viewed_products(top_n=top_n)

    return {
        "generated_at": now.isoformat(),

        "orders": {
            "total_orders": int(total_orders),
            "orders_today": int(orders_today),
            "orders_this_week": int(orders_this_week),
            "orders_this_month": int(orders_this_month),
            "canceled_orders": int(total_canceled_orders),
        },

        "revenue": {
            "total_revenue": float(delivered_revenue),
            "avg_order_value": float(avg_order_value),
            "revenue_today": float(revenue_today),
            "revenue_this_week": float(revenue_this_week),
            "revenue_this_month": float(revenue_this_month),
            "delivered_orders_count": int(delivered_count),
        },

        "customers": {
            "total_customers": int(total_customers),
            "customers_with_orders": int(customers_with_orders),
            "new_customers_this_week": int(new_customers_this_week),
            "new_customers_this_month": int(new_customers_this_month),
        },

        "payments": {
            "cod_count": int(cod_count),
            "online_count": int(online_count),
            "cod_pct": float(cod_pct),
            "online_pct": float(online_pct),
            "total_transactions": int(total_payments),
        },

        "products": {
            "total_products": int(total_products),
            "unique_products_sold": int(sold_unique_count),
            "never_sold_count": int(never_sold_count),
            "top_sold_by_qty": top_sold_by_qty,  # list of {product_id, qty, name?}
            "top_ordered_by_order_count": top_ordered_by_order_count,  # list of {product_id, order_count, name?}
            "top_rated": top_rated_products,  # list of {product_id, avg_rating, rating_count, name?}
            "top_viewed": top_viewed_products,  # list of {product_id, views, name?}
        }
    }


# ---------- internal analytics helpers ----------

def _sum_orders_total_by_status_since(status: str, since_dt: datetime) -> float:
    rows = list(orders.aggregate([
        {"$match": {"order_status": status, "ordered_at": {"$gte": since_dt}}},
        {"$group": {"_id": None, "sum_total": {"$sum": "$total"}}}
    ]))
    return float(rows[0]["sum_total"]) if rows else 0.0


def _count_customers_with_at_least_one_order() -> int:
    rows = list(orders.aggregate([
        {"$group": {"_id": "$customer_id"}},
        {"$count": "count"}
    ]))
    return int(rows[0]["count"]) if rows else 0


def _top_products_sold_by_quantity(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Sum quantity per product_id across all orders.items.
    Returns top N with product name if found.
    """
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.product_id", "qty": {"$sum": "$items.quantity"}}},
        {"$sort": {"qty": -1}},
        {"$limit": int(top_n)},
    ]
    rows = list(orders.aggregate(pipeline))

    # attach names
    out = []
    for r in rows:
        pid = int(r["_id"])
        name = _get_product_name(pid)
        out.append({"product_id": pid, "qty": int(r.get("qty") or 0), "name": name})
    return out


def _top_products_by_order_count(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Count how many orders each product_id appears in (order count).
    If a product is in one order with quantity 5, it counts as 1 order occurrence.
    """
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": {"order_id": "$order_id", "product_id": "$items.product_id"}
        }},
        {"$group": {"_id": "$_id.product_id", "order_count": {"$sum": 1}}},
        {"$sort": {"order_count": -1}},
        {"$limit": int(top_n)},
    ]
    rows = list(orders.aggregate(pipeline))

    out = []
    for r in rows:
        pid = int(r["_id"])
        name = _get_product_name(pid)
        out.append({"product_id": pid, "order_count": int(r.get("order_count") or 0), "name": name})
    return out


def _unique_sold_product_ids_set() -> set:
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.product_id"}},
    ]
    rows = list(orders.aggregate(pipeline))
    return {int(r["_id"]) for r in rows}


def _top_rated_products(top_n: int = 10, min_ratings: int = 3) -> List[Dict[str, Any]]:
    pipeline = [
        {"$group": {
            "_id": "$product_id",
            "avg_rating": {"$avg": "$rating"},
            "rating_count": {"$sum": 1}
        }},
        {"$match": {"rating_count": {"$gte": int(min_ratings)}}},
        {"$sort": {"avg_rating": -1, "rating_count": -1}},
        {"$limit": int(top_n)},
    ]
    rows = list(ratings.aggregate(pipeline))

    out = []
    for r in rows:
        pid = int(r["_id"])
        name = _get_product_name(pid)
        out.append({
            "product_id": pid,
            "avg_rating": float(r.get("avg_rating") or 0),
            "rating_count": int(r.get("rating_count") or 0),
            "name": name
        })
    return out


def _top_viewed_products(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    recently_viewed_products doc format:
      { customer_id, product_ids: [ {product_id, viewed_at}, ... ] }

    Count occurrences of product_id across all customers lists.
    """
    pipeline = [
        {"$unwind": "$product_ids"},
        {"$group": {"_id": "$product_ids.product_id", "views": {"$sum": 1}}},
        {"$sort": {"views": -1}},
        {"$limit": int(top_n)},
    ]
    rows = list(recently_viewed_products.aggregate(pipeline))

    out = []
    for r in rows:
        pid = int(r["_id"])
        name = _get_product_name(pid)
        out.append({"product_id": pid, "views": int(r.get("views") or 0), "name": name})
    return out


def _get_product_name(product_id: int) -> str:
    doc = products.find_one({"product_id": int(product_id)}, {"_id": 0, "name": 1})
    return (doc.get("name") if doc else "") or ""




# database.py



# Assuming you already have db set up like:
# client = MongoClient(MONGO_URI)
# db = client["your_db_name"]

def _messages_col():
    return database["messages"]

def _counters_col():
    return database["counters"]


def get_next_message_id() -> int:
    """
    Atomic auto-increment message_id.
    Creates counter doc if it doesn't exist.
    """
    doc = _counters_col().find_one_and_update(
        {"_id": "messages_message_id"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])


def create_message(payload: dict) -> int:
    """
    Insert a new message into `messages` collection.
    Returns the generated message_id.
    payload keys expected:
      source, full_name, email, phone_number, message
    """
    message_id = get_next_message_id()

    doc = {
        "message_id": message_id,

        "source": payload["source"],  # "contact" | "suggest_product"
        "full_name": payload["full_name"].strip(),
        "email": payload["email"].strip().lower(),
        "phone_number": payload["phone_number"].strip(),
        "message": payload["message"].strip(),

        "sent_at": datetime.utcnow(),

        "is_replied": False,
        "admin_reply_subject": None,
        "admin_reply_message": None,
        "admin_replied_at": None,
    }

    _messages_col().insert_one(doc)
    return message_id


def get_message_by_message_id(message_id: int) -> dict | None:
    return _messages_col().find_one({"message_id": int(message_id)})


def list_unreplied_messages(source: str | None = None, limit: int = 200) -> list[dict]:
    """
    For admin panel later: show only messages not replied yet.
    Optional filter by source.
    """
    q = {"is_replied": False}
    if source:
        q["source"] = source  # "contact" or "suggest_product"

    cursor = _messages_col().find(q).sort("sent_at", -1).limit(int(limit))
    return list(cursor)


def set_admin_reply(
    message_id: int,
    subject: str,
    reply_message: str,
) -> bool:
    """
    Save admin reply + mark replied. (Email sending will be done in admin endpoint later.)
    Returns True if updated.
    """
    res = _messages_col().update_one(
        {"message_id": int(message_id)},
        {
            "$set": {
                "is_replied": True,
                "admin_reply_subject": subject.strip(),
                "admin_reply_message": reply_message.strip(),
                "admin_replied_at": datetime.utcnow(),
            }
        }
    )
    return res.modified_count == 1






def get_unreplied_messages(limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Returns all messages where admin has NOT replied yet.
    Sorted newest first.
    """
    projection = {"_id": 0}
    cursor = (
        messages.find({"is_replied": False}, projection)
        .sort([("sent_at", -1), ("message_id", -1)])
        .limit(int(limit))
    )
    return list(cursor)


def get_message_by_id(message_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a single message by message_id.
    """
    try:
        mid = int(message_id)
    except Exception:
        return None

    return messages.find_one({"message_id": mid}, {"_id": 0})


def mark_message_replied(
    message_id: int,
    admin_reply_subject: str,
    admin_reply_message: str
) -> Dict[str, Any]:
    """
    Marks a message as replied and stores admin reply data.

    Returns:
      {"ok": True, "message": <updated_doc>}
      {"ok": False, "detail": "..."} on failure
    """
    try:
        mid = int(message_id)
    except Exception:
        return {"ok": False, "detail": "Invalid message id."}

    subj = (admin_reply_subject or "").strip()
    body = (admin_reply_message or "").strip()

    if len(subj) < 2:
        return {"ok": False, "detail": "Reply subject is too short."}
    if len(body) < 2:
        return {"ok": False, "detail": "Reply message is too short."}
    if len(subj) > 150:
        return {"ok": False, "detail": "Reply subject is too long (max 150)."}
    if len(body) > 8000:
        return {"ok": False, "detail": "Reply message is too long (max 8000)."}

    existing = messages.find_one({"message_id": mid})
    if not existing:
        return {"ok": False, "detail": "Message not found."}

    # prevent double-reply
    if bool(existing.get("is_replied")) is True:
        return {"ok": False, "detail": "This message was already replied to."}

    now = datetime.utcnow()

    updates = {
        "is_replied": True,
        "admin_reply_subject": subj,
        "admin_reply_message": body,
        "admin_replied_at": now,
    }

    messages.update_one({"message_id": mid}, {"$set": updates})
    updated = get_message_by_id(mid)
    return {"ok": True, "message": updated}




def get_recently_viewed_products_summary(
    customer_id: int,
    limit: int | None = 12
):
    """
    Returns a list of product summary dicts (same keys/shape as get_featured_products_summary),
    but based on `recently_viewed_products` collection for the given customer.
    Most recent first (viewed_at desc).
    """

    if not customer_id:
        return []

    # 1) Pull recently viewed list for this customer
    doc = recently_viewed_products.find_one(
        {"customer_id": int(customer_id)},
        {"_id": 0, "product_ids": 1}
    )

    product_items = (doc or {}).get("product_ids") or []
    if not isinstance(product_items, list) or len(product_items) == 0:
        return []

    # 2) Normalize + sort by viewed_at desc + de-dupe product ids preserving recency
    normalized = []
    for it in product_items:
        try:
            pid = int((it or {}).get("product_id"))
        except Exception:
            continue

        viewed_at = (it or {}).get("viewed_at")
        # Mongo returns datetime already; if not, try parsing fallback
        if isinstance(viewed_at, datetime):
            dt = viewed_at
        else:
            dt = datetime.min

        normalized.append((pid, dt))

    normalized.sort(key=lambda x: x[1], reverse=True)

    ordered_ids = []
    seen = set()
    for pid, _dt in normalized:
        if pid in seen:
            continue
        seen.add(pid)
        ordered_ids.append(pid)
        if limit is not None and len(ordered_ids) >= int(limit):
            break

    if not ordered_ids:
        return []

    # 3) Preload cart/wishlist sets (same logic as featured)
    cart_product_ids = get_cart_product_ids_set(customer_id) if customer_id else set()
    wishlist_product_ids = get_wishlist_product_ids_set(customer_id) if customer_id else set()  # strings

    # 4) Fetch all products in one query
    query = {"product_id": {"$in": ordered_ids}}
    projection = {
        "_id": 0,
        "product_id": 1,
        "name": 1,
        "category_id": 1,
        "price": 1,
        "discounted_price": 1,
        "image_urls": 1,
        "Brand": 1,
        "brand": 1,
    }

    cursor = products.find(query, projection)

    # Map product_id -> product doc
    by_id = {}
    for p in cursor:
        pid = p.get("product_id")
        if pid is None:
            continue
        try:
            by_id[int(pid)] = p
        except Exception:
            continue

    # 5) Build results in the exact same output shape as featured (preserve order)
    results = []

    for pid in ordered_ids:
        p = by_id.get(int(pid))
        if not p:
            continue

        product_id = p.get("product_id")

        price = _to_float(p.get("price", 0))
        discounted = _to_float(p.get("discounted_price", price))
        percentage_discount = _percentage_discount(price, discounted)

        category_slug = p.get("category_id", "") or ""
        category_name = ""
        if category_slug:
            cat = categories.find_one({"slug": category_slug}, {"name": 1, "_id": 0})
            category_name = cat.get("name", "") if cat else ""

        image_urls = p.get("image_urls") or []
        cover_image = image_urls[0] if isinstance(image_urls, list) and image_urls else ""

        in_cart = bool(customer_id) and (product_id is not None) and (int(product_id) in cart_product_ids)
        in_wishlist = bool(customer_id) and (product_id is not None) and (str(product_id) in wishlist_product_ids)

        results.append({
            "product_id": product_id,
            "name": p.get("name", "") or "",
            "category_slug": category_slug,
            "category_name": category_name,
            "rating": get_average_rating(product_id),
            "brand": p.get("Brand") or p.get("brand") or "",
            "cover_image": cover_image,
            "price": price,
            "discounted_price": discounted,
            "percentage_discount": percentage_discount,
            "in_cart": in_cart,
            "in_wishlist": in_wishlist,
        })

    return results





# NOTE:
# This assumes you already have a global `db` (Mongo database handle) in database.py
# like: db = client["international_market"]
# If your variable name differs, change `db[...]` accordingly.

def _strip_mongo_id(doc: dict | None) -> dict | None:
    """Remove Mongo _id so Jinja doesn't choke on ObjectId."""
    if not doc:
        return None
    doc = dict(doc)
    doc.pop("_id", None)
    return doc


def get_orders_collection():
    return database["orders"]


def get_products_collection():
    return database["products"]


def get_customer_orders_summary(customer_id: int, limit: int = 50) -> list[dict]:
    """
    Return a lightweight list of orders for the My Account page.
    Sorted newest first.
    """
    col = get_orders_collection()

    cursor = (
        col.find({"customer_id": int(customer_id)}, {"_id": 0})
        .sort("ordered_at", -1)
        .limit(int(limit))
    )

    orders = []
    for o in cursor:
        # safe defaults
        items = o.get("items") or []
        item_count = sum(int(i.get("quantity", 0) or 0) for i in items)

        orders.append(
            {
                "order_id": o.get("order_id"),
                "order_status": o.get("order_status", "pending"),
                "payment_method": o.get("payment_method"),
                "total": float(o.get("total") or 0),
                "item_count": int(item_count),
                "ordered_at": o.get("ordered_at"),
            }
        )

    return orders


def get_order_by_id_for_customer(order_id: int, customer_id: int) -> dict | None:
    """
    Secure fetch:
    Only return the order if it belongs to the logged-in customer.
    """
    col = get_orders_collection()
    doc = col.find_one(
        {"order_id": int(order_id), "customer_id": int(customer_id)},
        {"_id": 0},
    )
    return doc


def get_products_by_ids_map(product_ids: list[int]) -> dict[int, dict]:
    """
    Fetch product docs for a list of product_ids and return:
      { product_id: product_doc }
    """
    product_ids = [int(x) for x in product_ids if x is not None]
    if not product_ids:
        return {}

    col = get_products_collection()
    cursor = col.find({"product_id": {"$in": product_ids}}, {"_id": 0})

    out: dict[int, dict] = {}
    for p in cursor:
        pid = int(p.get("product_id"))
        out[pid] = p
    return out


def build_order_details_view(order_doc: dict) -> dict:
    """
    Convert an order document into a template-friendly enriched structure:
    - Expands items with product details
    - Adds per-item line totals
    """
    order_doc = dict(order_doc or {})
    items = order_doc.get("items") or []

    product_ids = [int(i.get("product_id")) for i in items if i.get("product_id") is not None]
    products_map = get_products_by_ids_map(product_ids)

    enriched_items: list[dict] = []
    for i in items:
        pid = int(i.get("product_id"))
        qty = int(i.get("quantity") or 0)

        p = products_map.get(pid) or {}
        # choose discounted_price if exists, else price
        unit_price = float(
            (p.get("discounted_price") if p.get("discounted_price") is not None else p.get("price")) or 0
        )

        enriched_items.append(
            {
                "product_id": pid,
                "quantity": qty,
                "name": p.get("name", f"Product #{pid}"),
                "unit": p.get("unit"),
                "size": p.get("size"),
                "image_url": (p.get("image_urls") or [None])[0],
                "unit_price": unit_price,
                "line_total": float(unit_price * qty),
                "category_id": p.get("category_id"),
                "sub_category_id": p.get("sub_category_id"),
            }
        )

    # attach
    order_doc["enriched_items"] = enriched_items
    order_doc["items_count"] = sum(int(x["quantity"]) for x in enriched_items)

    return order_doc



def delete_session_by_id(session_id: str) -> None:
    database["sessions"].delete_one({"session_id": session_id})





