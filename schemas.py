# schemas.py

from datetime import datetime
from typing import List, Optional, Literal

from pydantic import BaseModel, Field, EmailStr

# ------------------------------------------------------------
# Shared Sub-Documents
# ------------------------------------------------------------


class CartItem(BaseModel):
    product_id: int
    quantity: int

# ------------------------------------------------------------
# Customers
# ------------------------------------------------------------

class Customer(BaseModel):
    """
    Customer without password (passwordless auth via email + code).
    We can create a customer with just email at first login,
    and later fill in first_name, last_name, phone, addresses.
    """
    customer_id: int
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None

# ------------------------------------------------------------
# Auth: Login Codes (OTP)
# ------------------------------------------------------------

class LoginCode(BaseModel):
    """
    Stores a 1-time login code for email-based login.
    """
    email: EmailStr
    code: str
    expires_at: datetime
    used: bool = False

# ------------------------------------------------------------
# Auth: Sessions (for cookies)
# ------------------------------------------------------------

class Session(BaseModel):
    """
    Session document used for cookie-based login.
    session_id is what we store in the browser cookie.
    """
    session_id: str
    customer_id: int
    expires_at: datetime

# ------------------------------------------------------------
# Products
# ------------------------------------------------------------

class Product(BaseModel):
    product_id: int
    name: str
    description: str
    stock_qty: int
    sub_category_id: str
    category_id: str
    price: float
    discounted_price: Optional[float] = None
    unit: str
    size: int
    is_featured: bool = False
    image_urls: List[str] = []

# ------------------------------------------------------------
# Categories
# ------------------------------------------------------------

class Category(BaseModel):
    name: str
    slug: str
    is_featured: bool = False
    image_url: Optional[str] = None
    parent_id: Optional[str] = None

# ------------------------------------------------------------
# Orders
# ------------------------------------------------------------
class Address(BaseModel):
    full_name: str
    phone: str

    street1: str
    street2: Optional[str] = None
    city: str
    state: str
    postal_code: str
    country: str = "US"


class OrderItem(BaseModel):
    product_id: int
    quantity: int


class Order(BaseModel):
    order_id: int
    customer_id: int

    # one status only (no duplicates)
    order_status: Literal[
        "pending",          # created, not yet processed
        "confirmed",        # accepted by store (optional)
        "packed",           # packed and ready
        "out_for_delivery", # on the way
        "delivered",
        "canceled"
    ] = "pending"

    items: List[OrderItem]

    # pricing breakdown (stored as numbers from cart/checkout)
    subtotal: float
    discount_amount: float = 0.0
    discounted_subtotal: float
    tax: float
    shipping_fee: float
    total: float

    # coupon tracking
    coupon_code: Optional[str] = None

    # payment method chosen at checkout
    payment_method: Literal["cod", "online"]

    # links to TransactionLog.transaction_id (None for COD until you create one)
    payment_transaction_id: Optional[str] = None

    notes: Optional[str] = None
    shipping_address: Address

    # timestamps
    ordered_at: datetime
    paid_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None

# ------------------------------------------------------------
# Carts
# ------------------------------------------------------------

class Cart(BaseModel):
    customer_id: int
    items: List[CartItem]

# ------------------------------------------------------------
# Wishlist
# ------------------------------------------------------------
class Wishlist(BaseModel):
    customer_id: int
    items: List[str] = []

# ------------------------------------------------------------
# Transaction Logs
# ------------------------------------------------------------

class Order(BaseModel):
    order_id: int
    customer_id: int

    order_status: Literal[
        "pending",
        "confirmed",
        "packed",
        "out_for_delivery",
        "delivered",
        "canceled"
    ] = "pending"

    items: List[OrderItem]

    # pricing breakdown
    subtotal: float
    discount_amount: float = 0.0
    discounted_subtotal: float
    tax: float
    shipping_fee: float
    total: float

    # coupon tracking
    coupon_code: Optional[str] = None

    # payment method chosen at checkout
    payment_method: Literal["cod", "online"]

    # link to TransactionLog.transaction_id (can be None)
    payment_transaction_id: Optional[str] = None

    notes: Optional[str] = None
    shipping_address: Address

    # ==========================
    # Timeline timestamps (NEW)
    # ==========================
    ordered_at: datetime

    confirmed_at: Optional[datetime] = None
    packed_at: Optional[datetime] = None
    out_for_delivery_at: Optional[datetime] = None

    delivered_at: Optional[datetime] = None
    canceled_at: Optional[datetime] = None

class AddressIn(BaseModel):
    full_name: str
    phone: str
    street1: str
    street2: Optional[str] = None
    city: str
    state: str
    postal_code: str
    country: str = "US"

class OrderItemIn(BaseModel):
    product_id: int
    quantity: int = Field(ge=1, le=9999)

class CheckoutDraftIn(BaseModel):
    items: List[OrderItemIn]

    subtotal: float
    discount_amount: float = 0.0
    discounted_subtotal: float
    tax: float
    shipping_fee: float
    total: float

    coupon_code: Optional[str] = None

class ShippingSubmitIn(BaseModel):
    shipping_address: AddressIn
    notes: Optional[str] = None
    payment_method: Literal["cod", "online"]


# ------------------------------------------------------------
# Settings
# ------------------------------------------------------------

class Setting(BaseModel):
    pass

class Rating(BaseModel):
    name: str
    email: EmailStr
    review: str
    rating: int
    product_id: int

class CartAction(BaseModel):
    product_id: int


class WishlistAction(BaseModel):
    product_id: int

class CouponCode(BaseModel):
    # user-facing
    code: str = Field(..., min_length=2, max_length=32)          # e.g. "SAVE10"
    title: str = Field(..., min_length=1, max_length=80)         # e.g. "Save 10%"
    description: str = Field(default="", max_length=250)         # e.g. "10% off orders over $20"

    # discount rules
    discount_type: Literal["amount", "percent"]                  # amount = dollars off, percent = percentage off
    discount_value: float = Field(..., gt=0)                     # e.g. 10 (means $10 or 10%)

    # order conditions
    min_order_subtotal: float = Field(default=0, ge=0)           # applies if subtotal >= this

    # audience rules
    audience: Literal["all", "customers"] = "all"
    eligible_customer_ids: List[int] = Field(default_factory=list)  # used only when audience="customers"

    # usage limits
    max_uses_total: int = Field(default=0, ge=0)                 # 0 = unlimited
    uses_total: int = Field(default=0, ge=0)

    # who used it (simple approach)
    customer_ids_who_used: List[int] = Field(default_factory=list)

    # timing
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class RecentlyViewedItem(BaseModel):
    product_id: int
    viewed_at: datetime


class RecentlyViewedProducts(BaseModel):
    customer_id: int
    product_ids: List[RecentlyViewedItem] = Field(default_factory=list)

class RatingCreate(BaseModel):
    product_id: int
    name: str
    email: str
    review: str
    rating: int



class MessageCreate(BaseModel):
    """
    Incoming payload from Contact Us / Suggest a Product form.
    Stored as a new message document with message_id generated server-side.
    """
    source: Literal["contact", "suggest_product"] = Field(
        ...,
        description='Where the message came from: "contact" or "suggest_product".'
    )
    full_name: str = Field(..., min_length=2, max_length=80)
    email: EmailStr
    phone_number: str = Field(..., min_length=7, max_length=30)
    message: str = Field(..., min_length=5, max_length=4000)


class MessageInDB(BaseModel):
    """
    Full message document as stored in the database.
    """
    message_id: int = Field(..., ge=1)

    source: Literal["contact", "suggest_product"]
    full_name: str
    email: EmailStr
    phone_number: str
    message: str

    sent_at: datetime = Field(default_factory=datetime.utcnow)

    is_replied: bool = False
    admin_reply_subject: Optional[str] = Field(default=None, max_length=150)
    admin_reply_message: Optional[str] = Field(default=None, max_length=8000)
    admin_replied_at: Optional[datetime] = None


class AdminReplyUpdate(BaseModel):
    """
    Admin panel payload when replying to a message.
    This reply will be saved AND emailed to the user.
    """
    admin_reply_subject: str = Field(..., min_length=2, max_length=150)
    admin_reply_message: str = Field(..., min_length=2, max_length=8000)



