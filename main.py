import os
import math
import secrets
import string
import hashlib
from datetime import datetime, timedelta

import stripe
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    Request,
    Form,
    Query,
    Header,
    status,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    JSONResponse,
    PlainTextResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_404_NOT_FOUND

from schemas import *
from database import *
from methods import send_email

from fastapi import UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List
from bson import ObjectId

load_dotenv()

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8001").strip()
print("WEBHOOK SECRET LOADED:", bool(STRIPE_WEBHOOK_SECRET))


def build_header_footer_context(request: Request) -> dict:
    # browse categories dropdown
    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    # mega menu
    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    # logged-in customer + badge counts
    current_customer = get_current_customer(request)

    cart_qty = 0
    wishlist_qty = 0
    if current_customer:
        customer_id = current_customer["customer_id"]
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    # app settings for header/footer
    app_settings = get_app_settings()

    return {
        "parent_categories": parent_categories,
        "mega_parent_categories": mega_parent_categories,
        "mega_featured_subcategories": mega_featured_subcategories,
        "app_settings": app_settings,
        "cart_qty": cart_qty,
        "wishlist_qty": wishlist_qty,
        "current_customer": current_customer,
    }

def get_current_customer(request: Request) -> dict | None:
    """
    Read session_id from cookie and return the corresponding customer document,
    or None if not logged in / session expired.
    """
    session_id = request.cookies.get("session_id")
    if not session_id:
        return None

    session_doc = get_session_by_id(session_id)
    if not session_doc:
        return None

    customer = get_customer_by_id(session_doc["customer_id"])
    return customer

# ------------------------------------------------------------
# App & Templates
# ------------------------------------------------------------

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Static files
app.mount("/css", StaticFiles(directory="templates/css"), name="css")
app.mount("/images", StaticFiles(directory="templates/images"), name="images")
app.mount("/fonts", StaticFiles(directory="templates/fonts"), name="fonts")

# ------------------------------------------------------------
# Error Handling
# ------------------------------------------------------------

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == HTTP_404_NOT_FOUND:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    return HTMLResponse(content=str(exc.detail), status_code=exc.status_code)

# ------------------------------------------------------------
# Home Page
# ------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    # ✅ unified header/footer
    header_footer = build_header_footer_context(request)

    # product sections (include: in_cart, in_wishlist, discount_percentage, rating)
    featured_products = get_featured_products_summary(limit=20, customer_id=customer_id)
    hot_deals_products = get_hot_deals_products_summary(limit=20, customer_id=customer_id)
    latest_products = get_latest_products_summary(limit=20, customer_id=customer_id)

    featured_categories = get_featured_categories_with_counts()

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "title": "Home | International Market",

            # ✅ everything header/footer needs is here
            **header_footer,

            # page-specific
            "featured_products": featured_products,
            "hot_deals_products": hot_deals_products,
            "latest_products": latest_products,
            "featured_categories": featured_categories,
        }
    )

# ------------------------------------------------------------
# LOGIN: Step 1 – Email Page (GET)
# ------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """
    Show the email-only login page (login.html).
    If the user already has a valid session cookie, send them home.
    """
    current_customer = get_current_customer(request)
    if current_customer:
        return RedirectResponse(url="/", status_code=302)

    # Optional: if login page uses header dropdown / mega menu too
    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    # Header badges (likely 0 on login page)
    cart_qty = 0
    wishlist_qty = 0

    # ✅ FIX: fetch settings for template
    app_settings = get_app_settings()

    title = "Login | International Market"
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "page_title": title,
            "current_customer": current_customer,

            # Optional but recommended if your login.html includes header
            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,
            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,

            # ✅ REQUIRED because template uses it
            "app_settings": app_settings,
        }
    )

# ------------------------------------------------------------
# LOGIN: Step 2 – Start Login (POST /login/start)
# ------------------------------------------------------------

@app.post("/login/start")
async def login_start(request: Request, email: str = Form(...)):
    # 0) If already logged in → no need to start login again
    current_customer = get_current_customer(request)
    if current_customer:
        return RedirectResponse(url="/", status_code=302)

    # normalize email once, everywhere
    email = email.strip().lower()

    digits = string.digits
    code = "".join(secrets.choice(digits) for _ in range(6))

    login_codes.insert_one({
        "email": email,
        "code": code,
        "used": False,
    })

    html_message = f"""
    <html>
      <body>
        <h2>Your login code for International Market</h2>
        <p>Your one-time login code is:</p>
        <p style="font-size: 24px; font-weight: bold; letter-spacing: 3px;">
          {code}
        </p>
        <p>If you did not request this code, you can ignore this email.</p>
      </body>
    </html>
    """

    send_email(
        subject="Your login code",
        html_message=html_message,
        receiver_email=email
    )

    return RedirectResponse(
        url=f"/login/verify?email={email}",
        status_code=302
    )

# ------------------------------------------------------------
# LOGIN: Step 3 – Show Verify Page (GET /login/verify)
# ------------------------------------------------------------

@app.get("/login/verify", response_class=HTMLResponse)
async def login_verify_page(request: Request, email: str | None = None):
    shared = build_header_footer_context(request)

    if shared["current_customer"]:
        return RedirectResponse(url="/", status_code=302)

    if not email:
        return RedirectResponse(url="/login", status_code=302)

    email = email.strip().lower()

    title = "Verify Login | International Market"
    return templates.TemplateResponse(
        "login_verify.html",
        {
            "request": request,
            "page_title": title,
            "email": email,
            "error": None,
            **shared,
        }
    )

# ------------------------------------------------------------
# LOGIN: Step 4 – Verify Code (POST /login/verify)
# ------------------------------------------------------------

@app.post("/login/verify")
async def login_verify_submit(
    request: Request,
    email: str = Form(...),
    code: str = Form(...)
):
    # If already logged in
    current_customer = get_current_customer(request)
    if current_customer:
        return RedirectResponse(url="/", status_code=302)

    email = email.strip().lower()
    code = code.strip()

    login_code_doc = login_codes.find_one({
        "email": email,
        "code": code,
        "used": False
    })

    if not login_code_doc:
        shared = build_header_footer_context(request)  # ✅ THIS FIXES THE 500
        title = "Verify Login | International Market"
        return templates.TemplateResponse(
            "login_verify.html",
            {
                "request": request,
                "page_title": title,
                "email": email,
                "error": "Invalid or expired code. Please try again.",
                **shared,
            }
        )

    login_codes.update_one(
        {"_id": login_code_doc["_id"]},
        {"$set": {"used": True}}
    )

    existing_customer = get_customer_by_email(email)

    if existing_customer:
        customer_doc = existing_customer
        is_new_customer = False
    else:
        new_customer = Customer(customer_id=0, email=email)
        customer_doc = create_customer(new_customer)
        is_new_customer = True

    customer_id = customer_doc["customer_id"]
    session_id = create_session(customer_id=customer_id, days_valid=1)

    response = RedirectResponse(
        url="/account/setup" if is_new_customer else "/",
        status_code=302
    )

    response.set_cookie(
        key="session_id",
        value=session_id,
        max_age=24 * 60 * 60,
        httponly=True,
        secure=False,
        samesite="lax",
    )

    return response

# ------------------------------------------------------------
# ACCOUNT SETUP – first-time profile completion
# ------------------------------------------------------------

@app.get("/account/setup", response_class=HTMLResponse)
async def account_setup_page(request: Request):
    shared = build_header_footer_context(request)

    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    first_name = (current_customer.get("first_name") or "").strip()
    last_name = (current_customer.get("last_name") or "").strip()
    phone = (current_customer.get("phone") or "").strip()

    if first_name and last_name and phone:
        return RedirectResponse(url="/", status_code=302)

    title = "Complete Your Profile | International Market"
    return templates.TemplateResponse(
        "account_setup.html",
        {
            "request": request,
            "page_title": title,
            "customer": current_customer,
            "error": None,
            **shared,
        }
    )

@app.post("/account/setup")
async def account_setup_submit(
    request: Request,
    first_name: str = Form(...),
    last_name: str = Form(...),
    phone: str = Form(...)
):
    current_customer = get_current_customer(request)
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = current_customer["customer_id"]
    email = current_customer["email"]

    # build updated Customer model
    updated_customer = Customer(
        customer_id=customer_id,
        email=email,
        first_name=first_name.strip() or None,
        last_name=last_name.strip() or None,
        phone=phone.strip() or None,
    )

    # update in DB
    update_customer(customer_id, updated_customer)

    # after saving, go home
    return RedirectResponse(url="/", status_code=302)

@app.get("/cart/", response_class=HTMLResponse)
async def cart_page(request: Request):
    shared = build_header_footer_context(request)

    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = current_customer["customer_id"]

    # ✅ grouped cart rows for table
    cart = get_cart_items_basic(customer_id=customer_id)

    # ✅ recently viewed products for the new section
    recently_viewed_products_list = get_recently_viewed_products_summary(
        customer_id=customer_id,
        limit=12
    )

    title = "My Cart | International Market"
    return templates.TemplateResponse(
        "cart.html",
        {
            "request": request,
            "page_title": title,
            "cart": cart,
            "cart_is_empty": (len(cart) == 0),

            # ✅ NEW
            "recently_viewed_products": recently_viewed_products_list,

            **shared,
        }
    )

@app.get("/wishlist/", response_class=HTMLResponse)
async def wishlist_page(request: Request):
    shared = build_header_footer_context(request)

    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = current_customer["customer_id"]

    # ✅ Fetch wishlist products using the new summary function
    wishlist = get_wishlist_products_summary(
        customer_id=customer_id
    )

    title = "My Wishlist | International Market"
    return templates.TemplateResponse(
        "wishlist.html",
        {
            "request": request,
            "page_title": title,
            "wishlist": wishlist,   # ✅ PASSED AS `wishlist`
            **shared,
        }
    )

@app.get("/my-account/", response_class=HTMLResponse)
async def my_account_page(request: Request):
    shared = build_header_footer_context(request)

    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = int(current_customer["customer_id"])

    # ✅ orders list for this customer
    orders = get_customer_orders_summary(customer_id=customer_id, limit=100)

    title = "My Account | International Market"
    return templates.TemplateResponse(
        "my_account.html",
        {
            "request": request,
            "page_title": title,
            **shared,

            # page-specific
            "orders": orders,
        }
    )

@app.get("/my-account/order-details/", response_class=HTMLResponse)
async def order_details_page(
    request: Request,
    order_id: int = Query(...),
):
    shared = build_header_footer_context(request)

    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = int(current_customer["customer_id"])

    order_doc = get_order_by_id_for_customer(order_id=order_id, customer_id=customer_id)
    if not order_doc:
        # if someone tries to access an order that isn't theirs (or doesn't exist)
        return RedirectResponse(url="/my-account/", status_code=302)

    order_view = build_order_details_view(order_doc)

    title = f"Order #{order_id} | International Market"
    return templates.TemplateResponse(
        "order_details.html",
        {
            "request": request,
            "page_title": title,
            **shared,

            # page-specific
            "order": order_view,
        }
    )

@app.get("/logout", response_class=HTMLResponse)
async def logout(request: Request):
    session_id = request.cookies.get("session_id")

    if session_id:
        # ✅ delete session from DB
        delete_session_by_id(session_id)

    # ✅ redirect to home and clear cookie
    resp = RedirectResponse(url="/", status_code=302)
    resp.delete_cookie("session_id")

    return resp

@app.post("/api/newsletter/subscribe")
async def newsletter_subscribe(request: Request):
    data = {}

    # Try JSON first
    try:
        data = await request.json()
    except:
        # If not JSON, try form
        form = await request.form()
        data = dict(form)

    email = (data.get("email") or "").strip()

    ok = add_newsletter_subscription_email(email)

    if not ok:
        return JSONResponse(
            {"ok": False, "message": "Invalid email or already subscribed."},
            status_code=400
        )

    return JSONResponse({"ok": True, "message": "Subscribed successfully."})

@app.post("/api/cart/add")
async def api_cart_add(request: Request, payload: CartAction):
    current_customer = get_current_customer(request)
    if not current_customer:
        # frontend will redirect user to /login
        return JSONResponse(
            status_code=401,
            content={"ok": False, "redirect": "/login"}
        )

    customer_id = int(current_customer["customer_id"])
    product_id = int(payload.product_id)

    result = add_product_to_cart(customer_id, product_id)
    return {"ok": True, "action": "added", "product_id": product_id, **result}

@app.post("/api/cart/remove")
async def api_cart_remove(request: Request, payload: CartAction):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(
            status_code=401,
            content={"ok": False, "redirect": "/login"}
        )

    customer_id = int(current_customer["customer_id"])
    product_id = int(payload.product_id)

    result = remove_product_from_cart(customer_id, product_id)
    return {"ok": True, "action": "removed", "product_id": product_id, **result}

@app.post("/api/wishlist/add")
async def api_wishlist_add(request: Request, payload: WishlistAction):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(
            {"ok": False, "redirect": "/login"},
            status_code=status.HTTP_401_UNAUTHORIZED
        )

    customer_id = int(current_customer.get("customer_id"))
    product_id = int(payload.product_id)

    wishlist_qty = add_item_to_wishlist(customer_id, product_id)

    return {"ok": True, "in_wishlist": True, "wishlist_qty": wishlist_qty}

@app.post("/api/wishlist/remove")
async def api_wishlist_remove(request: Request, payload: WishlistAction):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(
            {"ok": False, "redirect": "/login"},
            status_code=status.HTTP_401_UNAUTHORIZED
        )

    customer_id = int(current_customer.get("customer_id"))
    product_id = int(payload.product_id)

    wishlist_qty = remove_item_from_wishlist(customer_id, product_id)

    return {"ok": True, "in_wishlist": False, "wishlist_qty": wishlist_qty}

@app.get("/subcategories/", response_class=HTMLResponse)
async def subcategories_page(
    request: Request,
    category_name: str = Query(default="")
):
    parent_slug = (category_name or "").strip()

    # Header/Footer data (same as other pages)
    context = build_header_footer_context(request)

    # Page-specific data
    category_display_name = get_category_name_by_slug(parent_slug)
    subcategories = get_subcategories_with_counts(parent_slug)

    # If slug is invalid, you can still render page nicely
    # (or you can return 404 if you prefer)
    title_name = category_display_name or "Subcategories"

    return templates.TemplateResponse(
        "subcategories.html",
        {
            "request": request,
            "title": f"{title_name} | International Market",

            # page specific
            "category_slug": parent_slug,
            "category_name": category_display_name,
            "subcategories": subcategories,

            # header/footer common
            **context,
        }
    )

# ------------------------------------------------------------
# Search Results (Advanced Search)
# ------------------------------------------------------------

@app.get("/search-results/", response_class=HTMLResponse)
async def search_results_page(
    request: Request,

    keyword: str | None = Query(default=None),
    category: str = Query(default="all"),
    subcategory: str = Query(default="all"),

    min_price: float = Query(default=0),
    max_price: float = Query(default=1000),

    hot_deal: bool = Query(default=True),
    popular: bool = Query(default=True),

    sort: str = Query(default="default"),

    # ✅ pagination
    page: int = Query(default=1, ge=1),
):
    PER_PAGE = 20

    # current customer
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    # header/footer context
    header_footer = build_header_footer_context(request)

    # categories + subcategories for dropdowns
    categories_with_subcategories = get_categories_with_subcategories()

    # normalize category/subcategory
    category = (category or "all").strip()
    subcategory = (subcategory or "all").strip()
    if category == "all":
        subcategory = "all"

    # normalize keyword
    keyword_clean = (keyword or "").strip()
    keyword_arg = keyword_clean if keyword_clean else None

    # normalize price range
    try:
        min_price_f = float(min_price)
    except Exception:
        min_price_f = 0.0

    try:
        max_price_f = float(max_price)
    except Exception:
        max_price_f = 1000.0

    if min_price_f > max_price_f:
        min_price_f, max_price_f = max_price_f, min_price_f

    # normalize sort
    sort_clean = (sort or "default").strip().lower()
    sort_arg = None if sort_clean == "default" else sort_clean

    # ✅ fetch paginated results + total_count
    search_results, total_count = search_products_advanced(
        keyword=keyword_arg,
        category=category,
        subcategory=subcategory,
        min_price=min_price_f,
        max_price=max_price_f,
        hot_deal=bool(hot_deal),
        popular=bool(popular),
        sort=sort_arg,
        page=page,
        per_page=PER_PAGE,
        customer_id=customer_id,
    )

    # ✅ pagination math
    total_pages = max(1, math.ceil(total_count / PER_PAGE)) if total_count else 1

    # clamp page if someone types a huge page number manually
    if page > total_pages:
        page = total_pages
        # re-fetch correct last page results (optional but better)
        search_results, total_count = search_products_advanced(
            keyword=keyword_arg,
            category=category,
            subcategory=subcategory,
            min_price=min_price_f,
            max_price=max_price_f,
            hot_deal=bool(hot_deal),
            popular=bool(popular),
            sort=sort_arg,
            page=page,
            per_page=PER_PAGE,
            customer_id=customer_id,
        )

    start_index = 0
    end_index = 0
    if total_count > 0:
        start_index = (page - 1) * PER_PAGE + 1
        end_index = min(page * PER_PAGE, total_count)

    # pass ALL filter values to template (including defaults)
    filters = {
        "keyword": keyword_clean,
        "category": category,
        "subcategory": subcategory,
        "min_price": min_price_f,
        "max_price": max_price_f,
        "hot_deal": bool(hot_deal),
        "popular": bool(popular),
        "sort": sort_clean,  # keep "default" so dropdown can select it

        # ✅ keep page in filters if you want it accessible
        "page": page,
    }

    page_title = "Search Results | International Market"

    return templates.TemplateResponse(
        "search_results.html",
        {
            "request": request,

            "title": page_title,
            "page_title": page_title,

            **header_footer,

            "categories_with_subcategories": categories_with_subcategories,
            "filters": filters,

            # ✅ results
            "search_results": search_results,

            # ✅ pagination data for template
            "total_count": total_count,
            "page": page,
            "per_page": PER_PAGE,
            "total_pages": total_pages,
            "start_index": start_index,
            "end_index": end_index,
        }
    )

@app.post("/api/cart/empty")
async def api_cart_empty(request: Request):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(
            status_code=401,
            content={"ok": False, "redirect": "/login"}
        )

    customer_id = int(current_customer["customer_id"])

    empty_cart(customer_id)

    # after emptying, cart qty is 0
    cart_qty = 0

    return JSONResponse(
        content={
            "ok": True,
            "cart_qty": cart_qty
        }
    )

@app.post("/api/coupons/apply")
async def api_apply_coupon(request: Request):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(status_code=401, content={"ok": False, "redirect": "/login"})

    body = await request.json()
    code = (body.get("code") or "").strip()

    subtotal_raw = body.get("subtotal", 0)
    try:
        subtotal = float(subtotal_raw)
    except (TypeError, ValueError):
        subtotal = 0.0

    # ✅ NEW: shipping fee from client (cart page already knows it)
    shipping_raw = body.get("shipping_fee", 0)
    try:
        shipping_fee = float(shipping_raw)
    except (TypeError, ValueError):
        shipping_fee = 0.0

    subtotal = max(0.0, subtotal)
    shipping_fee = max(0.0, shipping_fee)

    customer_id = int(current_customer["customer_id"])

    result = validate_coupon_for_subtotal(code=code, customer_id=customer_id, subtotal=subtotal)

    # Always return totals so UI can update/reset cleanly
    if not result.get("ok"):
        totals = compute_totals(subtotal=subtotal, discount_amount=0.0, shipping_fee=shipping_fee)
        return JSONResponse(
            content={
                "ok": False,
                "message": result.get("message", "Invalid coupon."),
                **totals
            }
        )

    discount_amount = float(result["discount_amount"])
    totals = compute_totals(subtotal=subtotal, discount_amount=discount_amount, shipping_fee=shipping_fee)

    return JSONResponse(
        content={
            "ok": True,
            "message": result.get("message", "Coupon applied."),
            "coupon": result.get("coupon", {}),
            **totals
        }
    )

@app.get("/product-details", response_class=HTMLResponse)
async def product_details_page(
    request: Request,
    product_id: int = Query(...)
):
    header_footer = build_header_footer_context(request)
    current_customer = header_footer.get("current_customer")

    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    # ✅ Track recently viewed (only for logged-in users)
    if customer_id:
        add_recently_viewed_product(customer_id=customer_id, product_id=product_id)

    product = get_product_details(product_id=product_id, customer_id=customer_id)
    if not product:
        raise StarletteHTTPException(status_code=HTTP_404_NOT_FOUND, detail="Product not found")

    return templates.TemplateResponse(
        "product-details.html",
        {
            "request": request,
            "title": f"{product.get('name', 'Product')} | International Market",
            "product": product,
            **header_footer,
        }
    )

@app.post("/api/ratings/add")
async def api_add_rating(payload: RatingCreate):
    # basic validation
    if payload.rating < 1 or payload.rating > 5:
        return JSONResponse({"ok": False, "error": "Rating must be between 1 and 5."}, status_code=400)

    if not payload.name.strip() or not payload.email.strip() or not payload.review.strip():
        return JSONResponse({"ok": False, "error": "All fields are required."}, status_code=400)

    # optional: ensure product exists
    exists = products.find_one({"product_id": int(payload.product_id)}, {"_id": 1})
    if not exists:
        return JSONResponse({"ok": False, "error": "Product not found."}, status_code=404)

    add_product_rating(
        product_id=payload.product_id,
        name=payload.name,
        email=payload.email,
        review=payload.review,
        rating=payload.rating,
    )

    return {"ok": True}

# -------------------------------
# Admin Auth: current admin
# -------------------------------
def get_current_admin(request: Request) -> dict | None:
    """
    Read admin_session_id from cookie and return admin doc, else None.
    """
    session_id = request.cookies.get("admin_session_id")
    if not session_id:
        return None

    session_doc = get_admin_session_by_id(session_id)
    if not session_doc:
        return None

    admin_id = session_doc.get("admin_id")
    if not admin_id:
        return None

    # You have only one admin doc, but keep it generic:
    admin_doc = admin_credentials.find_one({"_id": admin_id})
    return admin_doc

def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ============================================================
# ADMIN: Login Page (GET)
# ============================================================

@app.get("/core/ops/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    current_admin = get_current_admin(request)
    if current_admin:
        return RedirectResponse(url="/core/ops/admin/dashboard", status_code=302)

    return templates.TemplateResponse(
        "admin_login.html",
        {
            "request": request,
            "page_title": "Admin Login | International Market",
            "error": None,
        }
    )


# ============================================================
# ADMIN: Login Submit (POST)
# - Accepts username + (password_hash from JS) OR raw password fallback
# ============================================================

@app.post("/core/ops/admin/login")
async def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(""),
    password_hash: str = Form(""),
):
    current_admin = get_current_admin(request)
    if current_admin:
        return RedirectResponse(url="/core/ops/admin/dashboard", status_code=302)

    username = (username or "").strip()
    password = password or ""
    password_hash = (password_hash or "").strip().lower()

    if not username:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "page_title": "Admin Login | International Market",
                "error": "Username is required.",
            },
            status_code=400
        )

    admin_doc = get_admin_by_username(username)
    if not admin_doc:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "page_title": "Admin Login | International Market",
                "error": "Invalid username or password.",
            },
            status_code=401
        )

    stored_hash = (admin_doc.get("password_hash") or "").strip().lower()
    if not stored_hash:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "page_title": "Admin Login | International Market",
                "error": "Admin credentials misconfigured.",
            },
            status_code=500
        )

    # If JS sent hash -> verify that
    # If not -> hash raw password on server (fallback)
    candidate_hash = password_hash if password_hash else sha256_hex(password).lower()

    if candidate_hash != stored_hash:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "page_title": "Admin Login | International Market",
                "error": "Invalid username or password.",
            },
            status_code=401
        )

    # Create admin session + cookie
    admin_id = str(admin_doc["_id"])
    session_id = create_admin_session(admin_id=admin_id, days_valid=1)

    response = RedirectResponse(url="/core/ops/admin/dashboard", status_code=302)

    response.set_cookie(
        key="admin_session_id",
        value=session_id,
        max_age=24 * 60 * 60,
        httponly=True,
        secure=False,  # set True in production (https)
        samesite="lax",
    )

    return response


# ============================================================
# ADMIN: Dashboard (GET)
# ============================================================

@app.get("/core/ops/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "page_title": "Admin Dashboard | International Market",
            "admin": current_admin,
        }
    )


# ============================================================
# ADMIN: Logout (GET)
# ============================================================

@app.get("/core/ops/admin/logout")
async def admin_logout(request: Request):
    session_id = request.cookies.get("admin_session_id")
    if session_id:
        delete_admin_session(session_id)

    response = RedirectResponse(url="/core/ops/admin/login", status_code=302)
    response.delete_cookie("admin_session_id")
    return response



@app.get("/core/ops/admin/api/auth/check")
async def admin_auth_check(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return JSONResponse({"ok": False}, status_code=status.HTTP_401_UNAUTHORIZED)
    return {"ok": True}

# ============================================================
# ADMIN DASHBOARD: Manage Coupon Codes (PAGE)
# /core/ops/admin/dashboard/manage-coupon-codes
# ============================================================

@app.get("/core/ops/admin/dashboard/manage-coupon-codes", response_class=HTMLResponse)
async def admin_manage_coupon_codes_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    coupons = get_all_coupon_codes_summary()

    return templates.TemplateResponse(
        "admin_coupon_codes.html",
        {
            "request": request,
            "page_title": "Manage Coupon Codes | Admin",
            "heading": "Manage Coupon Codes",
            "coupons": coupons,
        }
    )


# ============================================================
# ADMIN DASHBOARD: Add Coupon Code (PAGE)
# /core/ops/admin/dashboard/add-coupon-code
# ============================================================

@app.get("/core/ops/admin/dashboard/add-coupon-code", response_class=HTMLResponse)
async def admin_add_coupon_code_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_add_or_update_coupon_codes.html",
        {
            "request": request,
            "page_title": "Add Coupon Code | Admin",
            "heading": "Add Coupon Code",
            "mode": "add",              # used by template/js
            "coupon": None,             # no defaults
            "error": None,
        }
    )


# ============================================================
# ADMIN DASHBOARD: Edit Coupon Code (PAGE)
# /core/ops/admin/dashboard/edit-coupon-code/{code}
# ============================================================

@app.get("/core/ops/admin/dashboard/edit-coupon-code/{code}", response_class=HTMLResponse)
async def admin_edit_coupon_code_page(request: Request, code: str):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    coupon = get_coupon_code_by_code(code)
    if not coupon:
        return RedirectResponse(url="/core/ops/admin/dashboard/manage-coupon-codes", status_code=302)

    return templates.TemplateResponse(
        "admin_add_or_update_coupon_codes.html",
        {
            "request": request,
            "page_title": f"Edit Coupon Code {coupon['code']} | Admin",
            "heading": f"Edit Coupon Code: {coupon['code']}",
            "mode": "edit",
            "coupon": coupon,
            "error": None,
        }
    )


# ============================================================
# ADMIN API: Create Coupon Code (POST)
# form submits here
# /core/ops/admin/api/coupon-codes/create
# ============================================================

@app.post("/core/ops/admin/api/coupon-codes/create")
async def admin_api_create_coupon_code(
    request: Request,

    code: str = Form(...),
    title: str = Form(...),
    description: str = Form(""),

    discount_type: str = Form(...),
    discount_value: str = Form(...),

    min_order_subtotal: str = Form("0"),

    audience: str = Form("all"),
    eligible_customer_ids_csv: str = Form(""),

    max_uses_total: str = Form("0"),

    starts_at: str = Form(""),
    ends_at: str = Form(""),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    try:
        # parse tricky fields
        eligible_ids = parse_csv_int_list(eligible_customer_ids_csv)

        starts_dt = parse_datetime_local(starts_at)
        ends_dt = parse_datetime_local(ends_at)

        data = {
            "code": code,
            "title": title,
            "description": description,

            "discount_type": discount_type,
            "discount_value": discount_value,

            "min_order_subtotal": min_order_subtotal,

            "audience": audience,
            "eligible_customer_ids": eligible_ids,

            "max_uses_total": max_uses_total,

            # create-only
            "uses_total": 0,
            "customer_ids_who_used": [],

            "starts_at": starts_dt,
            "ends_at": ends_dt,
        }

        created = create_coupon_code(data)
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/edit-coupon-code/{created['code']}",
            status_code=302
        )

    except Exception as e:
        return templates.TemplateResponse(
            "admin_add_or_update_coupon_codes.html",
            {
                "request": request,
                "page_title": "Add Coupon Code | Admin",
                "heading": "Add Coupon Code",
                "mode": "add",
                "coupon": None,
                "error": str(e),
            },
            status_code=400
        )


# ============================================================
# ADMIN API: Update Coupon Code (POST)
# form submits here
# /core/ops/admin/api/coupon-codes/update/{code}
# ============================================================

@app.post("/core/ops/admin/api/coupon-codes/update/{code}")
async def admin_api_update_coupon_code(
    request: Request,
    code: str,

    title: str = Form(...),
    description: str = Form(""),

    discount_type: str = Form(...),
    discount_value: str = Form(...),

    min_order_subtotal: str = Form("0"),

    audience: str = Form("all"),
    eligible_customer_ids_csv: str = Form(""),

    max_uses_total: str = Form("0"),

    starts_at: str = Form(""),
    ends_at: str = Form(""),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    try:
        existing = get_coupon_code_by_code(code)
        if not existing:
            return RedirectResponse(url="/core/ops/admin/dashboard/manage-coupon-codes", status_code=302)

        eligible_ids = parse_csv_int_list(eligible_customer_ids_csv)
        starts_dt = parse_datetime_local(starts_at)
        ends_dt = parse_datetime_local(ends_at)

        updates = {
            "title": title,
            "description": description,

            "discount_type": discount_type,
            "discount_value": discount_value,

            "min_order_subtotal": min_order_subtotal,

            "audience": audience,
            "eligible_customer_ids": eligible_ids,

            "max_uses_total": max_uses_total,

            "starts_at": starts_dt,
            "ends_at": ends_dt,
        }

        updated = update_coupon_code_by_code(code, updates)
        if not updated:
            return RedirectResponse(url="/core/ops/admin/dashboard/manage-coupon-codes", status_code=302)

        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/edit-coupon-code/{updated['code']}",
            status_code=302
        )

    except Exception as e:
        # reload edit page with error + existing values
        coupon = get_coupon_code_by_code(code)
        return templates.TemplateResponse(
            "admin_add_or_update_coupon_codes.html",
            {
                "request": request,
                "page_title": f"Edit Coupon Code {code} | Admin",
                "heading": f"Edit Coupon Code: {code}",
                "mode": "edit",
                "coupon": coupon,
                "error": str(e),
            },
            status_code=400
        )


# ============================================================
# ADMIN API: Delete Coupon Code (DELETE)
# /core/ops/admin/api/coupon-codes/delete/{code}
# Called via fetch() so we can remove from DOM without reload
# ============================================================

@app.delete("/core/ops/admin/api/coupon-codes/delete/{code}")
async def admin_api_delete_coupon_code(request: Request, code: str):
    current_admin = get_current_admin(request)
    if not current_admin:
        return JSONResponse({"ok": False, "detail": "Unauthorized"}, status_code=401)

    ok = delete_coupon_code_by_code(code)
    if not ok:
        return JSONResponse({"ok": False, "detail": "Not found"}, status_code=404)

    return {"ok": True, "code": code}


# ============================================================
# ADMIN DASHBOARD: Look Up Customer (PAGE)
# /core/ops/admin/dashboard/look-up-customer
# ============================================================

@app.get("/core/ops/admin/dashboard/look-up-customer", response_class=HTMLResponse)
async def admin_lookup_customer_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_customer_lookup.html",
        {
            "request": request,
            "page_title": "Look Up Customer | Admin",
            "heading": "Look Up Customer",
            "error": None,
            # defaults (so form can repopulate if needed)
            "filters": {
                "customer_id": "",
                "email": "",
                "first_name": "",
                "last_name": "",
                "phone": "",
            },
        }
    )


# ============================================================
# ADMIN DASHBOARD: Customer Search Results (PAGE)
# /core/ops/admin/dashboard/customer-search-results
# ============================================================

@app.get("/core/ops/admin/dashboard/customer-search-results", response_class=HTMLResponse)
async def admin_customer_search_results_page(
    request: Request,
    customer_id: str | None = Query(default=None),
    email: str | None = Query(default=None),
    first_name: str | None = Query(default=None),
    last_name: str | None = Query(default=None),
    phone: str | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    # normalize
    customer_id = (customer_id or "").strip()
    email = (email or "").strip()
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    phone = (phone or "").strip()

    # must have at least one field
    if not any([customer_id, email, first_name, last_name, phone]):
        return templates.TemplateResponse(
            "admin_customer_lookup.html",
            {
                "request": request,
                "page_title": "Look Up Customer | Admin",
                "heading": "Look Up Customer",
                "error": "Please enter at least one field to search.",
                "filters": {
                    "customer_id": customer_id,
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "phone": phone,
                },
            },
            status_code=400
        )

    # DB search (AND logic across provided fields)
    customers_found = search_customers(
        customer_id=customer_id,
        email=email,
        first_name=first_name,
        last_name=last_name,
        phone=phone,
        limit=200
    )

    return templates.TemplateResponse(
        "admin_customer_search_results.html",
        {
            "request": request,
            "page_title": "Customer Search Results | Admin",
            "heading": "Customer Search Results",
            "filters": {
                "customer_id": customer_id,
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
            },
            "customers": customers_found,
            "count": len(customers_found),
        }
    )



# ============================================================
# ADMIN: All Categories (PAGE)
# /core/ops/admin/dashboard/all-categories
# ============================================================

@app.get("/core/ops/admin/dashboard/all-categories", response_class=HTMLResponse)
async def admin_all_categories_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    grouped = get_all_categories_grouped()

    return templates.TemplateResponse(
        "admin_all_categories.html",
        {
            "request": request,
            "page_title": "All Categories | Admin",
            "heading": "All Categories",
            "grouped": grouped,
        }
    )


# ============================================================
# ADMIN: Look Up Categories (PAGE)
# /core/ops/admin/dashboard/look-up-categories
# ============================================================

@app.get("/core/ops/admin/dashboard/look-up-categories", response_class=HTMLResponse)
async def admin_lookup_categories_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_look_up_categories.html",
        {
            "request": request,
            "page_title": "Look Up Categories | Admin",
            "heading": "Look Up Categories",
            "error": None,
            "filters": {
                "name": "",
                "slug": "",
                "parent_id": "",
                "is_featured": False,
            },
        }
    )


# ============================================================
# ADMIN: Search Results Categories (PAGE)
# /core/ops/admin/dashboard/search-results-categories
# ============================================================

@app.get("/core/ops/admin/dashboard/search-results-categories", response_class=HTMLResponse)
async def admin_search_results_categories_page(
    request: Request,
    name: str | None = Query(default=None),
    slug: str | None = Query(default=None),
    parent_id: str | None = Query(default=None),
    is_featured: bool | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    name = (name or "").strip()
    slug = (slug or "").strip()
    parent_id = (parent_id or "").strip()

    # require at least one search field
    # is_featured alone also counts as a filter if explicitly provided
    if not any([name, slug, parent_id]) and is_featured is None:
        return templates.TemplateResponse(
            "admin_look_up_categories.html",
            {
                "request": request,
                "page_title": "Look Up Categories | Admin",
                "heading": "Look Up Categories",
                "error": "Please enter at least one search field.",
                "filters": {
                    "name": name,
                    "slug": slug,
                    "parent_id": parent_id,
                    "is_featured": False,
                },
            },
            status_code=400
        )

    results = search_categories(
        name=name,
        slug=slug,
        parent_id=parent_id,
        is_featured=is_featured if is_featured is not None else None,
        limit=5000
    )

    return templates.TemplateResponse(
        "admin_search_results_categories.html",
        {
            "request": request,
            "page_title": "Category Search Results | Admin",
            "heading": "Category Search Results",
            "filters": {
                "name": name,
                "slug": slug,
                "parent_id": parent_id,
                "is_featured": True if is_featured else False,
                "is_featured_provided": is_featured is not None,
            },
            "results": results,
            "count": len(results),
        }
    )


# ============================================================
# ADMIN: Add Category (PAGE)
# /core/ops/admin/dashboard/add-category
# ============================================================

@app.get("/core/ops/admin/dashboard/add-category", response_class=HTMLResponse)
async def admin_add_category_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_add_or_update_category.html",
        {
            "request": request,
            "page_title": "Add Category | Admin",
            "heading": "Add Category",
            "mode": "add",
            "category": None,
            "error": None,
        }
    )


# ============================================================
# ADMIN: Update Category (PAGE)
# /core/ops/admin/dashboard/update-category/{slug}
# ============================================================

@app.get("/core/ops/admin/dashboard/update-category/{slug}", response_class=HTMLResponse)
async def admin_update_category_page(request: Request, slug: str):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    category = get_category_by_slug(slug)
    if not category:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-categories", status_code=302)

    return templates.TemplateResponse(
        "admin_add_or_update_category.html",
        {
            "request": request,
            "page_title": f"Update Category {category['slug']} | Admin",
            "heading": f"Update Category: {category['slug']}",
            "mode": "update",
            "category": category,
            "error": None,
        }
    )


# ============================================================
# ADMIN API: Create Category (POST)
# /core/ops/admin/api/categories/create
# ============================================================

@app.post("/core/ops/admin/api/categories/create")
async def admin_api_create_category(
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    image_url: str = Form(""),
    parent_id: str = Form(""),             # may be "null" or blank
    is_featured: str | None = Form(None),  # checkbox: "on" or None
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    try:
        data = {
            "name": name,
            "slug": slug,
            "image_url": image_url,
            "parent_id": parent_id,                 # parse_parent_id handles "null"
            "is_featured": parse_checkbox_bool(is_featured),
        }

        created = create_category(data)
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/update-category/{created['slug']}",
            status_code=302
        )

    except Exception as e:
        return templates.TemplateResponse(
            "admin_add_or_update_category.html",
            {
                "request": request,
                "page_title": "Add Category | Admin",
                "heading": "Add Category",
                "mode": "add",
                "category": None,
                "error": str(e),
            },
            status_code=400
        )


# ============================================================
# ADMIN API: Update Category (POST)
# /core/ops/admin/api/categories/update/{slug}
# ============================================================

@app.post("/core/ops/admin/api/categories/update/{slug}")
async def admin_api_update_category(
    request: Request,
    slug: str,
    name: str = Form(...),
    image_url: str = Form(""),
    parent_id: str = Form(""),
    is_featured: str | None = Form(None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    try:
        updates = {
            "name": name,
            "image_url": image_url,
            "parent_id": parent_id,
            "is_featured": parse_checkbox_bool(is_featured),
        }

        updated = update_category_by_slug(slug, updates)
        if not updated:
            return RedirectResponse(url="/core/ops/admin/dashboard/all-categories", status_code=302)

        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/update-category/{updated['slug']}",
            status_code=302
        )

    except Exception as e:
        cat = get_category_by_slug(slug)
        return templates.TemplateResponse(
            "admin_add_or_update_category.html",
            {
                "request": request,
                "page_title": f"Update Category {slug} | Admin",
                "heading": f"Update Category: {slug}",
                "mode": "update",
                "category": cat,
                "error": str(e),
            },
            status_code=400
        )


# ============================================================
# ADMIN API: Delete Category (DELETE)
# /core/ops/admin/api/categories/delete/{slug}
# ============================================================

@app.delete("/core/ops/admin/api/categories/delete/{slug}")
async def admin_api_delete_category(request: Request, slug: str):
    current_admin = get_current_admin(request)
    if not current_admin:
        return JSONResponse({"ok": False, "detail": "Unauthorized"}, status_code=401)

    result = delete_category_by_slug(slug)
    if not result.get("ok") or result.get("deleted", 0) == 0:
        return JSONResponse({"ok": False, "detail": "Not found"}, status_code=404)

    return {"ok": True, "deleted": int(result["deleted"]), "slug": normalize_slug(slug)}



# ============================================================
# ADMIN PRODUCTS: Look Up Product (PAGE)
# /core/ops/admin/dashboard/look-up-product
# ============================================================

@app.get("/core/ops/admin/dashboard/look-up-product", response_class=HTMLResponse)
async def admin_lookup_product_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    categories_with_subcategories = get_categories_with_subcategories()

    return templates.TemplateResponse(
        "admin_look_up_product.html",
        {
            "request": request,
            "page_title": "Look Up Product | Admin",
            "heading": "Look Up Product",
            "error": None,
            "filters": {
                "product_id": "",
                "name": "",
                "category_id": "all",
                "sub_category_id": "all",
                # ✅ new filters
                "is_featured": False,
                "is_hot_deal": False,
            },
            "categories_with_subcategories": categories_with_subcategories,
        }
    )


# ============================================================
# ADMIN PRODUCTS: Search Results (PAGE)
# /core/ops/admin/dashboard/search-results-product
# ============================================================

@app.get("/core/ops/admin/dashboard/search-results-product", response_class=HTMLResponse)
async def admin_search_results_product_page(
    request: Request,
    product_id: str | None = Query(default=None),
    name: str | None = Query(default=None),
    category_id: str = Query(default="all"),
    sub_category_id: str = Query(default="all"),

    # ✅ new checkbox filters
    is_featured: bool = Query(default=False),
    is_hot_deal: bool = Query(default=False),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    categories_with_subcategories = get_categories_with_subcategories()

    product_id = (product_id or "").strip()
    name = (name or "").strip()
    category_id = (category_id or "all").strip()
    sub_category_id = (sub_category_id or "all").strip()

    # enforce dependency: if category is all => sub must be all
    if category_id == "all":
        sub_category_id = "all"

    # must provide at least one filter (now includes checkboxes)
    if not any([
        product_id,
        name,
        category_id != "all",
        sub_category_id != "all",
        bool(is_featured),
        bool(is_hot_deal),
    ]):
        return templates.TemplateResponse(
            "admin_look_up_product.html",
            {
                "request": request,
                "page_title": "Look Up Product | Admin",
                "heading": "Look Up Product",
                "error": "Please enter at least one search field.",
                "filters": {
                    "product_id": product_id,
                    "name": name,
                    "category_id": category_id,
                    "sub_category_id": sub_category_id,
                    # ✅ keep checkboxes state
                    "is_featured": bool(is_featured),
                    "is_hot_deal": bool(is_hot_deal),
                },
                "categories_with_subcategories": categories_with_subcategories,
            },
            status_code=400
        )

    # product_id parse
    pid_int = None
    if product_id:
        try:
            pid_int = int(product_id)
        except Exception:
            return templates.TemplateResponse(
                "admin_look_up_product.html",
                {
                    "request": request,
                    "page_title": "Look Up Product | Admin",
                    "heading": "Look Up Product",
                    "error": "Product ID must be a number.",
                    "filters": {
                        "product_id": product_id,
                        "name": name,
                        "category_id": category_id,
                        "sub_category_id": sub_category_id,
                        "is_featured": bool(is_featured),
                        "is_hot_deal": bool(is_hot_deal),
                    },
                    "categories_with_subcategories": categories_with_subcategories,
                },
                status_code=400
            )

    # ✅ call updated DB function
    results = search_products_admin(
        product_id=pid_int,
        name=name if name else None,
        category_id=category_id,
        sub_category_id=sub_category_id,
        is_featured=True if is_featured else None,
        is_hot_deal=True if is_hot_deal else None,
    )

    return templates.TemplateResponse(
        "admin_search_results_products.html",
        {
            "request": request,
            "page_title": "Product Search Results | Admin",
            "heading": "Product Search Results",
            "filters": {
                "product_id": product_id,
                "name": name,
                "category_id": category_id,
                "sub_category_id": sub_category_id,
                # ✅ include for chips + keeping UI consistent
                "is_featured": bool(is_featured),
                "is_hot_deal": bool(is_hot_deal),
            },
            "categories_with_subcategories": categories_with_subcategories,
            "products": results,
            "count": len(results),
        }
    )


# ============================================================
# ADMIN PRODUCTS: Add Product (PAGE)
# /core/ops/admin/dashboard/add-product
# ============================================================

@app.get("/core/ops/admin/dashboard/add-product", response_class=HTMLResponse)
async def admin_add_product_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    categories_with_subcategories = get_categories_with_subcategories()

    return templates.TemplateResponse(
        "admin_add_or_update_products.html",
        {
            "request": request,
            "page_title": "Add Product | Admin",
            "heading": "Add Product",
            "mode": "add",
            "error": None,
            "categories_with_subcategories": categories_with_subcategories,
            "product": {
                "product_id": get_next_product_id(),
                "name": "",
                "description": "",
                "long_description": "",
                "stock_qty": "",
                "category_id": "all",
                "sub_category_id": "all",
                "price": "",
                "discounted_price": "",
                "unit": "",
                "size": "",
                "Brand": "",
                "is_featured": False,
                "is_hot_deal": False,
                "image_file_ids": [],
            },
        }
    )


# ============================================================
# ADMIN PRODUCTS: Update Product (PAGE)
# /core/ops/admin/dashboard/update-product/{product_id}
# ============================================================

@app.get("/core/ops/admin/dashboard/update-product/{product_id}", response_class=HTMLResponse)
async def admin_update_product_page(request: Request, product_id: int):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    categories_with_subcategories = get_categories_with_subcategories()

    p = get_product_by_id_admin(product_id)
    if not p:
        return RedirectResponse(url="/core/ops/admin/dashboard/look-up-product", status_code=302)

    # normalize default dropdowns
    if not p.get("category_id"):
        p["category_id"] = "all"
    if not p.get("sub_category_id"):
        p["sub_category_id"] = "all"
    if p["category_id"] == "all":
        p["sub_category_id"] = "all"

    # ensure images list exists for template (GridFS ids)
    p["image_file_ids"] = p.get("image_file_ids") or []

    return templates.TemplateResponse(
        "admin_add_or_update_products.html",
        {
            "request": request,
            "page_title": f"Update Product {product_id} | Admin",
            "heading": f"Update Product: {product_id}",
            "mode": "update",
            "error": None,
            "categories_with_subcategories": categories_with_subcategories,
            "product": p,
        }
    )


# ============================================================
# ADMIN PRODUCTS API: Create Product (POST)
# /core/ops/admin/api/products/create
# ============================================================

@app.post("/core/ops/admin/api/products/create")
async def admin_api_create_product(
    request: Request,
    product_id: int = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    long_description: str = Form(""),
    stock_qty: int = Form(0),

    category_id: str = Form("all"),
    sub_category_id: str = Form("all"),

    price: float = Form(0),
    discounted_price: float = Form(0),

    unit: str = Form(""),
    size: float = Form(0),

    Brand: str = Form(""),

    is_featured: str | None = Form(None),
    is_hot_deal: str | None = Form(None),

    # ✅ NEW: multiple images
    images: List[UploadFile] = File(default=[]),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    # Normalize dropdown dependency
    category_id = (category_id or "all").strip()
    sub_category_id = (sub_category_id or "all").strip()
    if category_id == "all":
        sub_category_id = "all"

    # ✅ Save images to GridFS
    upload_payload = []
    for img in images or []:
        data = await img.read()
        if data:
            upload_payload.append((data, img.filename or "image", img.content_type or "image/jpeg"))

    image_file_ids = gridfs_save_images(product_id=int(product_id), files=upload_payload)

    product_doc = {
        "product_id": int(product_id),
        "name": (name or "").strip(),
        "description": (description or "").strip(),
        "long_description": (long_description or "").strip(),
        "stock_qty": int(stock_qty),

        "category_id": category_id,
        "sub_category_id": sub_category_id,

        "price": float(price),
        "discounted_price": float(discounted_price),

        "unit": (unit or "").strip(),
        "size": float(size),

        "Brand": (Brand or "").strip(),

        "is_featured": bool(is_featured),
        "is_hot_deal": bool(is_hot_deal),

        # ✅ NEW FIELD
        "image_file_ids": image_file_ids,
    }

    try:
        created = create_product_admin(product_doc)
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/update-product/{created['product_id']}",
            status_code=302
        )
    except Exception as e:
        categories_with_subcategories = get_categories_with_subcategories()
        return templates.TemplateResponse(
            "admin_add_or_update_products.html",
            {
                "request": request,
                "page_title": "Add Product | Admin",
                "heading": "Add Product",
                "mode": "add",
                "error": str(e),
                "categories_with_subcategories": categories_with_subcategories,
                "product": {
                    **product_doc,
                    # for template safety
                    "image_file_ids": [str(x) for x in (image_file_ids or [])],
                },
            },
            status_code=400
        )

# ============================================================
# ADMIN PRODUCTS API: Update Product (POST)
# /core/ops/admin/api/products/update/{product_id}
# ============================================================

@app.post("/core/ops/admin/api/products/update/{product_id}")
async def admin_api_update_product(
    request: Request,
    product_id: int,

    name: str = Form(...),
    description: str = Form(""),
    long_description: str = Form(""),
    stock_qty: int = Form(0),

    category_id: str = Form("all"),
    sub_category_id: str = Form("all"),

    price: float = Form(0),
    discounted_price: float = Form(0),

    unit: str = Form(""),
    size: float = Form(0),

    Brand: str = Form(""),

    is_featured: str | None = Form(None),
    is_hot_deal: str | None = Form(None),

    # ✅ NEW: multiple images
    images: List[UploadFile] = File(default=[]),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    category_id = (category_id or "all").strip()
    sub_category_id = (sub_category_id or "all").strip()
    if category_id == "all":
        sub_category_id = "all"

    updates = {
        "name": (name or "").strip(),
        "description": (description or "").strip(),
        "long_description": (long_description or "").strip(),
        "stock_qty": int(stock_qty),

        "category_id": category_id,
        "sub_category_id": sub_category_id,

        "price": float(price),
        "discounted_price": float(discounted_price),

        "unit": (unit or "").strip(),
        "size": float(size),

        "Brand": (Brand or "").strip(),

        "is_featured": bool(is_featured),
        "is_hot_deal": bool(is_hot_deal),
    }

    # We consumed file streams above if we do it like that, so we must not.
    # Instead do a safe single-pass read:
    upload_payload = []
    if images:
        for img in images:
            data = await img.read()
            if data:
                upload_payload.append((data, img.filename or "image", img.content_type or "image/jpeg"))

    if upload_payload:
        # delete old images
        existing = get_product_by_id_admin(product_id) or {}
        old_ids = existing.get("image_file_ids") or []
        gridfs_delete_files(old_ids)

        # save new images
        new_ids = gridfs_save_images(product_id=int(product_id), files=upload_payload)
        updates["image_file_ids"] = new_ids

    try:
        ok = update_product_admin(product_id, updates)
        if not ok:
            return RedirectResponse(url="/core/ops/admin/dashboard/look-up-product", status_code=302)

        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/update-product/{product_id}",
            status_code=302
        )
    except Exception as e:
        categories_with_subcategories = get_categories_with_subcategories()
        p = get_product_by_id_admin(product_id) or {}
        p.update(updates)
        p["product_id"] = product_id

        return templates.TemplateResponse(
            "admin_add_or_update_products.html",
            {
                "request": request,
                "page_title": f"Update Product {product_id} | Admin",
                "heading": f"Update Product: {product_id}",
                "mode": "update",
                "error": str(e),
                "categories_with_subcategories": categories_with_subcategories,
                "product": p,
            },
            status_code=400
        )

# ============================================================
# ADMIN PRODUCTS API: Delete Product (DELETE)
# /core/ops/admin/api/products/delete/{product_id}
# ============================================================

@app.delete("/core/ops/admin/api/products/delete/{product_id}")
async def admin_api_delete_product(request: Request, product_id: int):
    current_admin = get_current_admin(request)
    if not current_admin:
        return JSONResponse({"ok": False, "detail": "Unauthorized"}, status_code=401)

    ok = delete_product_by_id_admin(product_id)
    if not ok:
        return JSONResponse({"ok": False, "detail": "Not found"}, status_code=404)

    return {"ok": True, "product_id": int(product_id)}


def _amount_to_cents(amount: float) -> int:
    return int(round(float(amount) * 100))


@app.post("/api/checkout/draft")
async def api_checkout_draft(request: Request, payload: CheckoutDraftIn):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(status_code=401, content={"ok": False, "redirect": "/login"})

    customer_id = int(current_customer["customer_id"])
    upsert_checkout_draft(customer_id, payload.model_dump())
    return {"ok": True, "redirect": "/checkout/shipping"}


@app.get("/checkout/shipping", response_class=HTMLResponse)
async def checkout_shipping_page(request: Request):
    shared = build_header_footer_context(request)
    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = int(current_customer["customer_id"])
    draft = get_checkout_draft(customer_id)
    if not draft:
        return RedirectResponse(url="/cart/", status_code=302)

    return templates.TemplateResponse(
        "checkout-shipping.html",
        {
            "request": request,
            "page_title": "Shipping | International Market",
            "draft": draft,
            **shared,
        }
    )



@app.post("/api/checkout/place")
async def api_checkout_place(request: Request, payload: ShippingSubmitIn):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(status_code=401, content={"ok": False, "redirect": "/login"})

    customer_id = int(current_customer["customer_id"])
    draft = get_checkout_draft(customer_id)
    if not draft:
        return JSONResponse(status_code=400, content={"ok": False, "message": "Checkout draft missing."})

    # COD
    if payload.payment_method == "cod":

        # ✅ MARK COUPON AS USED (IMPORTANT)
        coupon_code = (draft.get("coupon_code") or "").strip().upper()
        if coupon_code:
            mark = mark_coupon_used(coupon_code, customer_id)
            if not mark.get("ok"):
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "message": mark.get("message")}
                )

        order_id = create_order_from_draft(
            customer_id=customer_id,
            draft=draft,
            payment_method="cod",
            shipping_address=payload.shipping_address.model_dump(),
            notes=payload.notes,
        )

        tx_id = create_transaction_log(
            order_id=order_id,
            customer_id=customer_id,
            payment_method="cod",
            amount=float(draft.get("total", 0)),
            status="pending",
            provider=None,
            provider_payment_intent_id=None,
        )
        attach_transaction_to_order(order_id, tx_id)

        empty_cart(customer_id)
        delete_checkout_draft(customer_id)

        return {"ok": True, "redirect": f"/order/confirmation/{order_id}"}

    return JSONResponse(status_code=400, content={"ok": False, "message": "Invalid payment flow."})


@app.post("/api/checkout/stripe-session")
async def api_checkout_stripe_session(request: Request, payload: ShippingSubmitIn):
    current_customer = get_current_customer(request)
    if not current_customer:
        return JSONResponse(status_code=401, content={"ok": False, "redirect": "/login"})

    if not stripe.api_key:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Stripe not configured."})

    customer_id = int(current_customer["customer_id"])

    draft = get_checkout_draft(customer_id)
    if not draft:
        return JSONResponse(status_code=400, content={"ok": False, "message": "Checkout draft missing."})

    # ✅ Save shipping info inside the draft (so webhook can use it after payment)
    draft["shipping_address"] = payload.shipping_address.model_dump()
    draft["notes"] = payload.notes
    upsert_checkout_draft(customer_id, draft)

    amount_cents = _amount_to_cents(draft.get("total", 0))

    # ✅ We do NOT have an order_id yet, so success/cancel routes cannot use it.
    success_url = f"{PUBLIC_BASE_URL}/order/stripe-success"
    cancel_url = f"{PUBLIC_BASE_URL}/order/stripe-cancel"

    session = stripe.checkout.Session.create(
        mode="payment",
        success_url=success_url,
        cancel_url=cancel_url,
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": "International Market Order"},
                    "unit_amount": amount_cents,
                },
                "quantity": 1,
            }
        ],
        metadata={
            "customer_id": str(customer_id),
        },
    )

    return {"ok": True, "url": session.url}


@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request, stripe_signature: str = Header(None)):
    try:
        if not STRIPE_WEBHOOK_SECRET:
            return PlainTextResponse("Webhook secret not configured", status_code=500)

        payload = await request.body()

        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=stripe_signature,
            secret=STRIPE_WEBHOOK_SECRET,
        )

        event_type = event["type"]
        obj = event["data"]["object"]

        if event_type == "checkout.session.completed":
            metadata = obj.get("metadata", {}) or {}
            customer_id = int(metadata.get("customer_id", 0) or 0)

            payment_intent_id = obj.get("payment_intent")

            if not customer_id or not payment_intent_id:
                return PlainTextResponse("ok", status_code=200)

            # ✅ PREVENT DUPLICATE ORDERS (STRIPE RETRIES)
            existing = transaction_logs.find_one(
                {"provider": "stripe", "provider_payment_intent_id": payment_intent_id},
                {"_id": 1}
            )
            if existing:
                return PlainTextResponse("ok", status_code=200)

            draft = get_checkout_draft(customer_id)
            if not draft:
                return PlainTextResponse("ok", status_code=200)

            shipping_address = draft.get("shipping_address")
            notes = draft.get("notes")

            if not shipping_address:
                return PlainTextResponse("ok", status_code=200)

            # ✅ MARK COUPON AS USED (IMPORTANT)
            coupon_code = (draft.get("coupon_code") or "").strip().upper()
            if coupon_code:
                mark = mark_coupon_used(coupon_code, customer_id)
                if not mark.get("ok"):
                    print("COUPON MARK FAILED:", mark.get("message"))
                    return PlainTextResponse("ok", status_code=200)

            order_id = create_order_from_draft(
                customer_id=customer_id,
                draft=draft,
                payment_method="online",
                shipping_address=shipping_address,
                notes=notes,
            )

            tx_id = create_transaction_log(
                order_id=order_id,
                customer_id=customer_id,
                payment_method="online",
                amount=float(draft.get("total", 0)),
                status="succeeded",
                provider="stripe",
                provider_payment_intent_id=payment_intent_id,
            )
            attach_transaction_to_order(order_id, tx_id)

            empty_cart(customer_id)
            delete_checkout_draft(customer_id)

        return PlainTextResponse("ok", status_code=200)

    except Exception as e:
        print("STRIPE WEBHOOK ERROR:", repr(e))
        return PlainTextResponse("webhook error", status_code=500)

@app.get("/order/stripe-success", response_class=HTMLResponse)
async def stripe_success(request: Request):
    shared = build_header_footer_context(request)
    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = int(current_customer["customer_id"])

    # give webhook a moment to write (usually instant)
    order_id = get_latest_order_id_for_customer(customer_id)
    if not order_id:
        return RedirectResponse(url="/cart/", status_code=302)

    return RedirectResponse(url=f"/order/confirmation/{order_id}", status_code=302)

@app.get("/order/stripe-cancel", response_class=HTMLResponse)
async def stripe_cancel(request: Request):
    shared = build_header_footer_context(request)
    return templates.TemplateResponse(
        "payment-cancelled.html",
        {
            "request": request,
            "page_title": "Payment Cancelled | International Market",
            **shared,
        }
    )

@app.get("/order/confirmation/{order_id}", response_class=HTMLResponse)
async def order_confirmation(request: Request, order_id: int):
    shared = build_header_footer_context(request)
    current_customer = shared["current_customer"]
    if not current_customer:
        return RedirectResponse(url="/login", status_code=302)

    customer_id = int(current_customer["customer_id"])
    order = get_order_for_customer(order_id=order_id, customer_id=customer_id)
    if not order:
        return RedirectResponse(url="/cart/", status_code=302)

    return templates.TemplateResponse(
        "order-confirmation.html",
        {
            "request": request,
            "page_title": f"Order #{order_id} | International Market",
            "order": order,
            **shared,
        }
    )

@app.get("/api/settings/shipping-fee")
async def api_get_shipping_fee(request: Request):
    # if you want this public, leave as-is
    # if you want only logged-in users, uncomment the check below:

    # current_customer = get_current_customer(request)
    # if not current_customer:
    #     return JSONResponse(status_code=401, content={"ok": False, "redirect": "/login"})

    fee = get_shipping_fee()
    return {"ok": True, "shipping_fee": fee}





# ============================================================
# ADMIN: Recent Transactions (PAGE)
# /core/ops/admin/dashboard/recent-transactions/
# ============================================================

@app.get("/core/ops/admin/dashboard/recent-transactions/", response_class=HTMLResponse)
async def admin_recent_transactions_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    # You said: show 10 latest in table (simple)
    rows = get_recent_transaction_logs(limit=10)

    return templates.TemplateResponse(
        "admin_recent_transactions.html",
        {
            "request": request,
            "page_title": "Recent Transactions | Admin",
            "heading": "Recent Transactions",
            "rows": rows,
            "count": len(rows),
        }
    )


# ============================================================
# ADMIN: Look Up Transactions (PAGE)
# /core/ops/admin/dashboard/look-up-transactions/
# ============================================================

@app.get("/core/ops/admin/dashboard/look-up-transactions/", response_class=HTMLResponse)
async def admin_look_up_transactions_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_look_up_transactions.html",
        {
            "request": request,
            "page_title": "Look Up Transactions | Admin",
            "heading": "Look Up Transactions",
            "error": None,
            "filters": {
                "customer_id": "",
            },
        }
    )


# ============================================================
# ADMIN: Transactions Search Results (PAGE)
# /core/ops/admin/dashboard/transactions-search-results/
# ============================================================

@app.get("/core/ops/admin/dashboard/transactions-search-results/", response_class=HTMLResponse)
async def admin_transactions_search_results_page(
    request: Request,
    customer_id: str | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    customer_id_s = (customer_id or "").strip()

    # require customer_id
    if not customer_id_s:
        return templates.TemplateResponse(
            "admin_look_up_transactions.html",
            {
                "request": request,
                "page_title": "Look Up Transactions | Admin",
                "heading": "Look Up Transactions",
                "error": "Please enter a customer id.",
                "filters": {"customer_id": ""},
            },
            status_code=400
        )

    # must be integer
    try:
        cid = int(customer_id_s)
        if cid <= 0:
            raise ValueError("customer_id must be positive")
    except Exception:
        return templates.TemplateResponse(
            "admin_look_up_transactions.html",
            {
                "request": request,
                "page_title": "Look Up Transactions | Admin",
                "heading": "Look Up Transactions",
                "error": "Customer id must be a valid number.",
                "filters": {"customer_id": customer_id_s},
            },
            status_code=400
        )

    rows = get_transaction_logs_by_customer_id(cid, limit=5000)

    return templates.TemplateResponse(
        "admin_transactions_search_results.html",
        {
            "request": request,
            "page_title": "Transaction Search Results | Admin",
            "heading": "Transaction Search Results",
            "filters": {"customer_id": customer_id_s},
            "rows": rows,
            "count": len(rows),
        }
    )




# expects:
# - get_current_admin(request)
# - templates (Jinja2Templates)
# - send_email(subject, html_message, receiver_email)
# - orders DB functions from database.py:
#     get_open_orders_by_status
#     search_orders_by_customer_id
#     get_order_by_order_id
#     is_open_order
#     is_past_order
#     update_order_status_with_message
#
# and Mongo collections:
# - customers (to fetch email)


# ============================================================
# ADMIN: All Open Orders (PAGE)
# /core/ops/admin/dashboard/all-open-orders/
# ============================================================

@app.get("/core/ops/admin/dashboard/all-open-orders/", response_class=HTMLResponse)
async def admin_all_open_orders_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    grouped = get_open_orders_by_status()

    return templates.TemplateResponse(
        "admin_all_open_orders.html",
        {
            "request": request,
            "page_title": "All Open Orders | Admin",
            "heading": "All Open Orders",
            "grouped": grouped,  # dict: pending/confirmed/packed/out_for_delivery -> list[order]
        }
    )


# ============================================================
# ADMIN: Look Up Past Orders (PAGE)  (but searches ALL orders)
# /core/ops/admin/dashboard/look-up-past-orders/
# ============================================================

@app.get("/core/ops/admin/dashboard/look-up-past-orders/", response_class=HTMLResponse)
async def admin_look_up_past_orders_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    return templates.TemplateResponse(
        "admin_look_up_past_orders.html",
        {
            "request": request,
            "page_title": "Look Up Orders | Admin",
            "heading": "Look Up Orders",
            "error": None,
            "filters": {"customer_id": ""},
        }
    )


# ============================================================
# ADMIN: Search Results Past Orders (PAGE) (shows ALL orders)
# /core/ops/admin/dashboard/search-results-past-orders/
# ============================================================

@app.get("/core/ops/admin/dashboard/search-results-past-orders/", response_class=HTMLResponse)
async def admin_search_results_past_orders_page(
    request: Request,
    customer_id: str | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    customer_id_s = (customer_id or "").strip()

    if not customer_id_s:
        return templates.TemplateResponse(
            "admin_look_up_past_orders.html",
            {
                "request": request,
                "page_title": "Look Up Orders | Admin",
                "heading": "Look Up Orders",
                "error": "Please enter a customer id.",
                "filters": {"customer_id": ""},
            },
            status_code=400
        )

    try:
        cid = int(customer_id_s)
        if cid <= 0:
            raise ValueError("customer_id must be positive")
    except Exception:
        return templates.TemplateResponse(
            "admin_look_up_past_orders.html",
            {
                "request": request,
                "page_title": "Look Up Orders | Admin",
                "heading": "Look Up Orders",
                "error": "Customer id must be a valid number.",
                "filters": {"customer_id": customer_id_s},
            },
            status_code=400
        )

    rows = search_orders_by_customer_id(cid, limit=5000)

    return templates.TemplateResponse(
        "admin_search_results_past_orders.html",
        {
            "request": request,
            "page_title": "Order Search Results | Admin",
            "heading": "Order Search Results",
            "filters": {"customer_id": customer_id_s},
            "rows": rows,
            "count": len(rows),
        }
    )


# ============================================================
# ADMIN: Order Details (PAST order) (PAGE)
# /core/ops/admin/dashboard/order-details-past-order/
# ============================================================

@app.get("/core/ops/admin/dashboard/order-details-past-order/", response_class=HTMLResponse)
async def admin_order_details_past_order_page(
    request: Request,
    order_id: str | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    oid_s = (order_id or "").strip()
    if not oid_s:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    try:
        oid = int(oid_s)
    except Exception:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    order = get_order_by_order_id(oid)
    if not order:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    # If admin accidentally lands here for an open order, bounce to open details page
    if is_open_order(order):
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/order-details-open-order/?order_id={oid}",
            status_code=302
        )

    return templates.TemplateResponse(
        "admin_order_details_past_order.html",
        {
            "request": request,
            "page_title": f"Order {oid} | Admin",
            "heading": f"Order Details: {oid}",
            "order": order,
        }
    )


# ============================================================
# ADMIN: Order Details (OPEN order) (PAGE)
# /core/ops/admin/dashboard/order-details-open-order/
# ============================================================

@app.get("/core/ops/admin/dashboard/order-details-open-order/", response_class=HTMLResponse)
async def admin_order_details_open_order_page(
    request: Request,
    order_id: str | None = Query(default=None),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    oid_s = (order_id or "").strip()
    if not oid_s:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    try:
        oid = int(oid_s)
    except Exception:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    order = get_order_by_order_id(oid)
    if not order:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    # If admin lands here for a past order, bounce
    if is_past_order(order):
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/order-details-past-order/?order_id={oid}",
            status_code=302
        )

    return templates.TemplateResponse(
        "admin_order_details_current_order.html",
        {
            "request": request,
            "page_title": f"Open Order {oid} | Admin",
            "heading": f"Open Order: {oid}",
            "order": order,
            "error": None,
        }
    )


# ============================================================
# ADMIN API: Update Order Status + Email Customer (POST)
# /core/ops/admin/api/orders/update-status/{order_id}
# ============================================================

@app.post("/core/ops/admin/api/orders/update-status/{order_id}")
async def admin_api_update_order_status(
    request: Request,
    order_id: int,
    new_status: str = Form(...),      # "confirmed" | "canceled" | "packed" | "out_for_delivery" | "delivered"
    admin_message: str = Form(""),    # editable text box
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    new_status = (new_status or "").strip()
    admin_message = (admin_message or "").strip()

    # 1) Update order status + timestamps (+ COD delivered => mark tx succeeded) + store admin message into notes
    result = update_order_status_with_message(
        order_id=int(order_id),
        new_status=new_status,
        admin_message=admin_message,
    )

    if not result.get("ok"):
        # Render the open-order details page again with an error
        order = get_order_by_order_id(int(order_id))
        if not order:
            return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

        return templates.TemplateResponse(
            "admin_order_details_current_order.html",
            {
                "request": request,
                "page_title": f"Open Order {order_id} | Admin",
                "heading": f"Open Order: {order_id}",
                "order": order,
                "error": result.get("detail") or "Failed to update order.",
            },
            status_code=400
        )

    updated_order = result.get("order") or get_order_by_order_id(int(order_id))
    if not updated_order:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-open-orders/", status_code=302)

    # 2) Send customer email (best-effort: do not block admin workflow if it fails)
    try:
        cid = int(updated_order.get("customer_id", 0) or 0)
        cust = customers.find_one(
            {"customer_id": cid},
            {"_id": 0, "email": 1, "first_name": 1, "last_name": 1}
        )

        receiver_email = (cust.get("email") if cust else "") or ""
        receiver_email = receiver_email.strip().lower()

        full_name = ""
        if cust:
            first = (cust.get("first_name") or "").strip()
            last = (cust.get("last_name") or "").strip()
            full_name = (first + (" " + last if last else "")).strip()

        if receiver_email:
            msg = admin_message
            if not msg:
                msg = f"Your order #{updated_order.get('order_id')} status is now: {updated_order.get('order_status')}."

            html_message = f"""
            <html>
              <body>
                <h2>International Market — Order Update</h2>
                <p>Hi {full_name or 'Customer'},</p>
                <p>Your order <strong>#{updated_order.get('order_id')}</strong> status is now:</p>
                <p style="font-size:18px;font-weight:bold;">{updated_order.get('order_status')}</p>
                <hr/>
                <p>{msg}</p>
              </body>
            </html>
            """

            send_email(
                subject=f"Order #{updated_order.get('order_id')} status update",
                html_message=html_message,
                receiver_email=receiver_email
            )
    except Exception:
        pass

    # 3) Redirect to correct details page (this reloads the page)
    oid = int(updated_order.get("order_id"))

    if is_past_order(updated_order):
        return RedirectResponse(
            url=f"/core/ops/admin/dashboard/order-details-past-order/?order_id={oid}",
            status_code=302
        )

    return RedirectResponse(
        url=f"/core/ops/admin/dashboard/order-details-open-order/?order_id={oid}",
        status_code=302
    )


@app.get("/core/ops/admin/dashboard/shipping-fee", response_class=HTMLResponse)
async def admin_shipping_fee_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    fee = get_shipping_fee_value()

    return templates.TemplateResponse(
        "admin_shipping_fee.html",
        {
            "request": request,
            "page_title": "Shipping Fee | Admin",
            "heading": "Shipping Fee",
            "fee": fee,
            "error": None,
            "success": None,
        }
    )


# ============================================================
# ADMIN API: Update Shipping Fee (POST)
# /core/ops/admin/api/settings/shipping-fee
# ============================================================

@app.post("/core/ops/admin/api/settings/shipping-fee")
async def admin_update_shipping_fee(
    request: Request,
    shipping_fee: str = Form(...),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    raw = (shipping_fee or "").strip()

    try:
        fee = float(raw)
        if fee < 0:
            raise ValueError("Shipping fee cannot be negative.")
        update_shipping_fee_value(fee)

        # reload page after save
        return RedirectResponse(url="/core/ops/admin/dashboard/shipping-fee", status_code=302)

    except Exception as e:
        # re-render page with error (no redirect)
        current_fee = get_shipping_fee_value()
        return templates.TemplateResponse(
            "admin_shipping_fee.html",
            {
                "request": request,
                "page_title": "Shipping Fee | Admin",
                "heading": "Shipping Fee",
                "fee": current_fee,
                "error": str(e),
                "success": None,
            },
            status_code=400
        )




# ============================================================
# ADMIN: App Settings Page
# /core/ops/admin/dashboard/app-settings
# ============================================================

@app.get("/core/ops/admin/dashboard/app-settings", response_class=HTMLResponse)
async def admin_app_settings_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    settings_doc = get_app_settings()

    return templates.TemplateResponse(
        "admin_app_settings.html",
        {
            "request": request,
            "page_title": "App Settings | Admin",
            "heading": "Application Settings",
            "settings": settings_doc,
            "error": None,
        }
    )


# ============================================================
# ADMIN API: Update App Settings
# /core/ops/admin/api/settings/app
# ============================================================

@app.post("/core/ops/admin/api/settings/app")
async def admin_update_app_settings(
    request: Request,
    number_to_show: str = Form(...),
    number_to_give: str = Form(...),
    address: str = Form(...),
    email: str = Form(...),
    hours: str = Form(...),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    try:
        update_app_settings({
            "number_to_show": number_to_show,
            "number_to_give": number_to_give,
            "address": address,
            "email": email,
            "hours": hours,
        })

        return RedirectResponse(
            url="/core/ops/admin/dashboard/app-settings",
            status_code=302
        )

    except Exception as e:
        settings_doc = get_app_settings()
        return templates.TemplateResponse(
            "admin_app_settings.html",
            {
                "request": request,
                "page_title": "App Settings | Admin",
                "heading": "Application Settings",
                "settings": settings_doc,
                "error": str(e),
            },
            status_code=400
        )




# requires:
# - get_current_admin(request)
# - templates
# - database.py function:
#     get_admin_analytics_snapshot


# ============================================================
# ADMIN: Analytics (PAGE)
# /core/ops/admin/dashboard/analytics
# ============================================================

@app.get("/core/ops/admin/dashboard/analytics", response_class=HTMLResponse)
async def admin_analytics_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    snapshot = get_admin_analytics_snapshot(top_n=10)

    return templates.TemplateResponse(
        "admin_analytics.html",
        {
            "request": request,
            "page_title": "Analytics | Admin",
            "heading": "Analytics",
            "snapshot": snapshot,
        }
    )



# ------------------------------------------------------------
# HELP CENTER PAGE
# ------------------------------------------------------------

@app.get("/help-center/", response_class=HTMLResponse)
async def help_center_page(request: Request):
    # Logged-in customer (if any)
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    # Header / Mega Menu Data
    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    # Header badge counts
    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    # App settings (used in header + footer)
    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "help_center.html",
        {
            "request": request,
            "title": "Help Center | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# PRIVACY POLICY
# ------------------------------------------------------------

@app.get("/privacy-policy/", response_class=HTMLResponse)
async def privacy_policy_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "privacy-policy.html",
        {
            "request": request,
            "title": "Privacy Policy | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# REFUND & RETURN POLICY
# ------------------------------------------------------------

@app.get("/refund-and-return-policy/", response_class=HTMLResponse)
async def refund_and_return_policy_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "refund-and-return-policy.html",
        {
            "request": request,
            "title": "Refund & Return Policy | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# TERMS & CONDITIONS
# ------------------------------------------------------------

@app.get("/terms-and-conditions/", response_class=HTMLResponse)
async def terms_and_conditions_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "terms-and-conditions.html",
        {
            "request": request,
            "title": "Terms & Conditions | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# ABOUT US
# ------------------------------------------------------------

@app.get("/about-us/", response_class=HTMLResponse)
async def about_us_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "about-us.html",
        {
            "request": request,
            "title": "About Us | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# CONTACT US PAGE
# ------------------------------------------------------------

@app.get("/contact-us/", response_class=HTMLResponse)
async def contact_us_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "contact-us.html",
        {
            "request": request,
            "title": "Contact Us | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )


# ------------------------------------------------------------
# SUGGEST A PRODUCT PAGE
# ------------------------------------------------------------

@app.get("/suggest-product/", response_class=HTMLResponse)
async def suggest_product_page(request: Request):
    current_customer = get_current_customer(request)
    customer_id = (
        int(current_customer.get("customer_id"))
        if current_customer and current_customer.get("customer_id") is not None
        else None
    )

    categories_data = get_parent_categories_with_meta()
    parent_categories = categories_data["parents"]

    mega_data = get_mega_menu_categories(limit=10)
    mega_parent_categories = mega_data["parent_categories"]
    mega_featured_subcategories = mega_data["featured_subcategories"]

    cart_qty, wishlist_qty = 0, 0
    if customer_id:
        cart_qty, wishlist_qty = get_cart_and_wishlist_counts(customer_id)

    app_settings = get_app_settings()

    return templates.TemplateResponse(
        "suggest-product.html",
        {
            "request": request,
            "title": "Suggest a Product | International Market",

            "current_customer": current_customer,
            "app_settings": app_settings,

            "parent_categories": parent_categories,
            "mega_parent_categories": mega_parent_categories,
            "mega_featured_subcategories": mega_featured_subcategories,

            "cart_qty": cart_qty,
            "wishlist_qty": wishlist_qty,
        }
    )



# ------------------------------------------------------------
# API: CONTACT US SUBMIT
# ------------------------------------------------------------

@app.post("/api/messages/contact-us")
async def api_contact_us_submit(request: Request):
    data = await request.json()

    # Basic validation (Pydantic validation will happen later if you want body=MessageCreate)
    full_name = (data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip()
    phone_number = (data.get("phone_number") or "").strip()
    message = (data.get("message") or "").strip()

    if not full_name or not email or not phone_number or not message:
        return JSONResponse({"ok": False, "message": "All fields are required."}, status_code=400)

    message_id = create_message({
        "source": "contact",
        "full_name": full_name,
        "email": email,
        "phone_number": phone_number,
        "message": message,
    })

    return JSONResponse(
        {
            "ok": True,
            "message": "Your message has been received. Our team will reply by email soon.",
            "message_id": message_id
        }
    )


# ------------------------------------------------------------
# API: SUGGEST PRODUCT SUBMIT
# ------------------------------------------------------------

@app.post("/api/messages/suggest-product")
async def api_suggest_product_submit(request: Request):
    data = await request.json()

    full_name = (data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip()
    phone_number = (data.get("phone_number") or "").strip()
    message = (data.get("message") or "").strip()

    if not full_name or not email or not phone_number or not message:
        return JSONResponse({"ok": False, "message": "All fields are required."}, status_code=400)

    message_id = create_message({
        "source": "suggest_product",
        "full_name": full_name,
        "email": email,
        "phone_number": phone_number,
        "message": message,
    })

    return JSONResponse(
        {
            "ok": True,
            "message": "Thanks for your suggestion! Our team will review it and reply by email if needed.",
            "message_id": message_id
        }
    )



# requires:
# - get_current_admin(request)
# - templates
# - send_email(subject, html_message, receiver_email)
# - database functions:
#     get_unreplied_messages
#     get_message_by_id
#     mark_message_replied


# ============================================================
# ADMIN: All Messages (PAGE)
# /core/ops/admin/dashboard/all-messages/
# ============================================================

@app.get("/core/ops/admin/dashboard/all-messages/", response_class=HTMLResponse)
async def admin_all_messages_page(request: Request):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    rows = get_unreplied_messages(limit=5000)

    return templates.TemplateResponse(
        "admin_all_messages.html",
        {
            "request": request,
            "page_title": "All Messages | Admin",
            "heading": "Messages (Unreplied)",
            "rows": rows,
            "count": len(rows),
        }
    )


# ============================================================
# ADMIN: Message Details (PAGE)
# /core/ops/admin/dashboard/message-details/
# ============================================================

@app.get("/core/ops/admin/dashboard/message-details/", response_class=HTMLResponse)
async def admin_message_details_page(
    request: Request,
    message_id: int = Query(...),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    msg = get_message_by_id(int(message_id))
    if not msg:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-messages/", status_code=302)

    # If already replied, just send admin back to list (since list shows unreplied only)
    if bool(msg.get("is_replied")):
        return RedirectResponse(url="/core/ops/admin/dashboard/all-messages/", status_code=302)

    return templates.TemplateResponse(
        "admin_message_details.html",
        {
            "request": request,
            "page_title": f"Message {message_id} | Admin",
            "heading": f"Message Details: {message_id}",
            "msg": msg,
            "error": None,
            "defaults": {
                "admin_reply_subject": "",
                "admin_reply_message": "",
            }
        }
    )


# ============================================================
# ADMIN API: Reply to Message (POST)
# /core/ops/admin/api/messages/reply/{message_id}
# ============================================================

@app.post("/core/ops/admin/api/messages/reply/{message_id}")
async def admin_api_reply_message(
    request: Request,
    message_id: int,
    admin_reply_subject: str = Form(...),
    admin_reply_message: str = Form(...),
):
    current_admin = get_current_admin(request)
    if not current_admin:
        return RedirectResponse(url="/core/ops/admin/login", status_code=302)

    msg = get_message_by_id(int(message_id))
    if not msg:
        return RedirectResponse(url="/core/ops/admin/dashboard/all-messages/", status_code=302)

    # If already replied, go back
    if bool(msg.get("is_replied")):
        return RedirectResponse(url="/core/ops/admin/dashboard/all-messages/", status_code=302)

    # 1) Update DB first (so even if email fails, reply is recorded)
    result = mark_message_replied(
        message_id=int(message_id),
        admin_reply_subject=admin_reply_subject,
        admin_reply_message=admin_reply_message
    )

    if not result.get("ok"):
        # re-render details page with error
        return templates.TemplateResponse(
            "admin_message_details.html",
            {
                "request": request,
                "page_title": f"Message {message_id} | Admin",
                "heading": f"Message Details: {message_id}",
                "msg": msg,
                "error": result.get("detail") or "Failed to reply.",
                "defaults": {
                    "admin_reply_subject": admin_reply_subject,
                    "admin_reply_message": admin_reply_message,
                }
            },
            status_code=400
        )

    updated = result.get("message") or get_message_by_id(int(message_id))

    # 2) Send email (best-effort)
    try:
        receiver_email = (msg.get("email") or "").strip().lower()
        user_name = (msg.get("full_name") or "").strip()
        source = (msg.get("source") or "").strip()
        user_message = (msg.get("message") or "").strip()

        subj = (admin_reply_subject or "").strip()
        body = (admin_reply_message or "").strip()

        if receiver_email:
            html_message = f"""
            <html>
              <body>
                <h2>International Market — Reply to Your Message</h2>

                <p>Hi <strong>{user_name or "Customer"}</strong>,</p>

                <p>We received your message from: <strong>{source}</strong></p>

                <hr />

                <h3>Your Message</h3>
                <p style="white-space:pre-wrap;">{user_message}</p>

                <hr />

                <h3>Our Reply</h3>
                <p><strong>Subject:</strong> {subj}</p>
                <p style="white-space:pre-wrap;">{body}</p>

                <hr />
                <p style="color:#7e7e7e;">
                  If you have more questions, reply to this email or contact us again through the website.
                </p>
              </body>
            </html>
            """

            send_email(
                subject=subj,
                html_message=html_message,
                receiver_email=receiver_email
            )
    except Exception:
        # don't block admin flow
        pass

    # 3) Go back to unreplied messages list (page reload)
    return RedirectResponse(url="/core/ops/admin/dashboard/all-messages/", status_code=302)



@app.get("/core/ops/images/{file_id}")
async def serve_image(file_id: str):
    grid_out = gridfs_get_file(file_id)
    if not grid_out:
        return JSONResponse({"ok": False, "detail": "Image not found"}, status_code=404)

    return StreamingResponse(
        grid_out,
        media_type=getattr(grid_out, "content_type", None) or "application/octet-stream"
    )
    
