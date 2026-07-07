#!/usr/bin/env python3
"""
Telegram-WhatsApp Bridge Bot - Production Ready.
WhatsApp Web automation via pyppeteer (Chromium).
All secrets via environment variables.
"""

import asyncio
import base64
import json
import logging
import os
import re
import sys
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("BOT_TOKEN and OWNER_ID must be set in environment.")
OWNER_ID = int(OWNER_ID)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---- Telegram ----
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# ---- Database ----
import aiosqlite
try:
    import pymongo
    from bson import ObjectId
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False
    ObjectId = None

# ---- WhatsApp ----
from pyppeteer import launch
from pyppeteer.errors import TimeoutError as PyTimeoutError

# ---- Image ----
from PIL import Image

# ---- Health server ----
from aiohttp import web

# ---- Config ----
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///whatsapp_bot.db")
MONGO_URI = os.getenv("MONGO_URI", "")
USE_MONGO = MONGO_AVAILABLE and bool(MONGO_URI)
CHROME_PATH = os.getenv("CHROME_PATH", None)
SESSION_DIR = os.path.join(os.getcwd(), "sessions")
os.makedirs(SESSION_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "8080"))

# ============================================================================
# DATABASE LAYER (SQLite + MongoDB)
# ============================================================================
class Database:
    def __init__(self):
        self._mongo_client = None
        self._mongo_db = None
        if USE_MONGO:
            self._mongo_client = pymongo.MongoClient(MONGO_URI)
            self._mongo_db = self._mongo_client.get_database()
            self._init_mongo()
        self._init_sqlite()

    # ---------- SQLite ----------
    def _init_sqlite(self):
        import sqlite3
        db_path = DATABASE_URL.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT,
                session_data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used TEXT
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                from_number TEXT,
                from_name TEXT,
                content TEXT,
                timestamp TEXT,
                is_otp INTEGER DEFAULT 0,
                read INTEGER DEFAULT 0,
                delivered INTEGER DEFAULT 0,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        defaults = {
            "notifications": "true",
            "otp_highlight": "true",
            "auto_reconnect": "true",
            "log_level": "INFO",
            "theme": "dark",
            "session_timeout": "3600",
        }
        for k, v in defaults.items():
            c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
        conn.commit()
        conn.close()

    def _init_mongo(self):
        if self._mongo_db:
            for col in ["accounts", "messages", "settings"]:
                if col not in self._mongo_db.list_collection_names():
                    self._mongo_db.create_collection(col)
            defaults = {
                "notifications": "true",
                "otp_highlight": "true",
                "auto_reconnect": "true",
                "log_level": "INFO",
                "theme": "dark",
                "session_timeout": "3600",
            }
            for k, v in defaults.items():
                self._mongo_db.settings.update_one(
                    {"key": k}, {"$set": {"value": v}}, upsert=True
                )

    async def _sqlite_execute(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(DATABASE_URL.replace("sqlite:///", "")) as db:
            cur = await db.execute(query, params)
            await db.commit()
            return cur

    async def _sqlite_fetchone(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(DATABASE_URL.replace("sqlite:///", "")) as db:
            cur = await db.execute(query, params)
            return await cur.fetchone()

    async def _sqlite_fetchall(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(DATABASE_URL.replace("sqlite:///", "")) as db:
            cur = await db.execute(query, params)
            return await cur.fetchall()

    # ---------- Common CRUD ----------
    async def add_account(self, phone: Optional[str], session_data: str) -> str:
        if USE_MONGO:
            doc = {"phone": phone, "session_data": session_data,
                   "created_at": datetime.utcnow().isoformat(), "last_used": None}
            return str(self._mongo_db.accounts.insert_one(doc).inserted_id)
        else:
            cur = await self._sqlite_execute(
                "INSERT INTO accounts (phone, session_data, created_at) VALUES (?, ?, ?)",
                (phone, session_data, datetime.utcnow().isoformat())
            )
            return str(cur.lastrowid)

    async def get_account(self, account_id: str) -> Optional[Dict]:
        if USE_MONGO:
            doc = self._mongo_db.accounts.find_one({"_id": ObjectId(account_id)})
            if doc:
                doc["id"] = str(doc["_id"])
                del doc["_id"]
                return doc
            return None
        else:
            row = await self._sqlite_fetchone(
                "SELECT id, phone, session_data, created_at, last_used FROM accounts WHERE id = ?",
                (int(account_id),)
            )
            if row:
                return {"id": str(row[0]), "phone": row[1], "session_data": row[2],
                        "created_at": row[3], "last_used": row[4]}
            return None

    async def get_all_accounts(self) -> List[Dict]:
        if USE_MONGO:
            return [{**d, "id": str(d["_id"])} for d in self._mongo_db.accounts.find()]
        else:
            rows = await self._sqlite_fetchall("SELECT id, phone, session_data, created_at, last_used FROM accounts")
            return [{"id": str(r[0]), "phone": r[1], "session_data": r[2],
                     "created_at": r[3], "last_used": r[4]} for r in rows]

    async def update_account_last_used(self, account_id: str):
        if USE_MONGO:
            self._mongo_db.accounts.update_one(
                {"_id": ObjectId(account_id)},
                {"$set": {"last_used": datetime.utcnow().isoformat()}}
            )
        else:
            await self._sqlite_execute(
                "UPDATE accounts SET last_used = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), int(account_id))
            )

    async def delete_account(self, account_id: str):
        if USE_MONGO:
            self._mongo_db.accounts.delete_one({"_id": ObjectId(account_id)})
            self._mongo_db.messages.delete_many({"account_id": account_id})
        else:
            await self._sqlite_execute("DELETE FROM messages WHERE account_id = ?", (int(account_id),))
            await self._sqlite_execute("DELETE FROM accounts WHERE id = ?", (int(account_id),))

    async def save_message(self, account_id: str, from_number: str, from_name: str,
                           content: str, timestamp: str, is_otp: bool = False):
        if USE_MONGO:
            self._mongo_db.messages.insert_one({
                "account_id": account_id,
                "from_number": from_number,
                "from_name": from_name,
                "content": content,
                "timestamp": timestamp,
                "is_otp": 1 if is_otp else 0,
                "read": 0,
                "delivered": 0,
            })
        else:
            await self._sqlite_execute(
                """INSERT INTO messages (account_id, from_number, from_name, content, timestamp, is_otp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (int(account_id), from_number, from_name, content, timestamp, 1 if is_otp else 0)
            )

    async def get_messages(self, account_id: str, limit: int = 50) -> List[Dict]:
        if USE_MONGO:
            docs = self._mongo_db.messages.find({"account_id": account_id}).sort("timestamp", -1).limit(limit)
            return [{**d, "id": str(d["_id"])} for d in docs]
        else:
            rows = await self._sqlite_fetchall(
                """SELECT id, from_number, from_name, content, timestamp, is_otp, read
                   FROM messages WHERE account_id = ? ORDER BY timestamp DESC LIMIT ?""",
                (int(account_id), limit)
            )
            return [{"id": str(r[0]), "from_number": r[1], "from_name": r[2],
                     "content": r[3], "timestamp": r[4], "is_otp": bool(r[5]), "read": bool(r[6])}
                    for r in rows]

    async def mark_messages_read(self, account_id: str, message_ids: List[str]):
        if USE_MONGO:
            self._mongo_db.messages.update_many(
                {"_id": {"$in": [ObjectId(mid) for mid in message_ids]}, "account_id": account_id},
                {"$set": {"read": 1}}
            )
        else:
            ids = [int(mid) for mid in message_ids]
            placeholders = ",".join("?" * len(ids))
            await self._sqlite_execute(
                f"UPDATE messages SET read = 1 WHERE id IN ({placeholders}) AND account_id = ?",
                (*ids, int(account_id))
            )

    async def get_settings(self) -> Dict[str, str]:
        if USE_MONGO:
            return {d["key"]: d["value"] for d in self._mongo_db.settings.find()}
        else:
            rows = await self._sqlite_fetchall("SELECT key, value FROM settings")
            return {r[0]: r[1] for r in rows}

    async def set_setting(self, key: str, value: str):
        if USE_MONGO:
            self._mongo_db.settings.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)
        else:
            await self._sqlite_execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, value)
            )

    async def get_stats(self, account_id: str) -> Dict:
        if USE_MONGO:
            total = self._mongo_db.messages.count_documents({"account_id": account_id})
            otp = self._mongo_db.messages.count_documents({"account_id": account_id, "is_otp": 1})
        else:
            total = (await self._sqlite_fetchone(
                "SELECT COUNT(*) FROM messages WHERE account_id = ?", (int(account_id),)
            ))[0]
            otp = (await self._sqlite_fetchone(
                "SELECT COUNT(*) FROM messages WHERE account_id = ? AND is_otp = 1", (int(account_id),)
            ))[0]
        return {"total": total, "otp": otp}


# ============================================================================
# WHATSAPP CLIENT
# ============================================================================
class WhatsAppClient:
    def __init__(self, account_id: str, db: Database, account_data: Dict):
        self.account_id = account_id
        self.db = db
        self.account_data = account_data
        self.browser = None
        self.page = None
        self.running = False
        self.listening_task = None
        self.health_check_task = None
        self._qr_event = asyncio.Event()
        self._qr_data = None

    async def start(self):
        logger.info(f"Starting WhatsApp client for account {self.account_id}")
        launch_args = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-web-security",
                "--window-size=1280,800",
            ],
            "handleSIGINT": False,
            "handleSIGTERM": False,
            "handleSIGHUP": False,
        }
        if CHROME_PATH:
            launch_args["executablePath"] = CHROME_PATH
        try:
            self.browser = await launch(**launch_args)
            self.page = await self.browser.newPage()
            await self.page.setViewport({"width": 1280, "height": 800})

            session_data = self.account_data.get("session_data")
            if session_data and session_data not in ("{}", ""):
                try:
                    cookies = json.loads(session_data)
                    await self.page.setCookie(*cookies)
                except Exception as e:
                    logger.error(f"Failed to load cookies: {e}")

            await self.page.goto("https://web.whatsapp.com", waitUntil="networkidle0", timeout=30000)
            if not await self._check_logged_in():
                await self._wait_for_login()
            await self._save_cookies()
            self.running = True
            self.listening_task = asyncio.create_task(self._listen_loop())
            self.health_check_task = asyncio.create_task(self._health_check_loop())
            logger.info(f"WhatsApp client {self.account_id} started.")
        except Exception as e:
            logger.error(f"Failed to start client {self.account_id}: {e}")
            await self.logout()
            raise

    async def _check_logged_in(self):
        try:
            await self.page.waitForSelector('div[data-testid="chat-list"]', timeout=5000)
            return True
        except PyTimeoutError:
            return False

    async def _wait_for_login(self):
        qr_selector = 'canvas[aria-label="Scan me!"]'
        try:
            await self.page.waitForSelector(qr_selector, timeout=10000)
            qr_element = await self.page.querySelector(qr_selector)
            if qr_element:
                qr_data = await self.page.evaluate(
                    """(element) => element.toDataURL('image/png').split(',')[1]""",
                    qr_element,
                )
                self._qr_data = qr_data
                self._qr_event.set()
        except PyTimeoutError:
            pass
        await self.page.waitForSelector('div[data-testid="chat-list"]', timeout=120000)

    async def get_qr(self, timeout=30):
        try:
            await asyncio.wait_for(self._qr_event.wait(), timeout)
            return self._qr_data
        except asyncio.TimeoutError:
            return None

    async def _save_cookies(self):
        cookies = await self.page.cookies()
        session_data = json.dumps(cookies)
        if USE_MONGO:
            self.db._mongo_db.accounts.update_one(
                {"_id": ObjectId(self.account_id)},
                {"$set": {"session_data": session_data}}
            )
        else:
            await self.db._sqlite_execute(
                "UPDATE accounts SET session_data = ? WHERE id = ?",
                (session_data, int(self.account_id))
            )
        logger.info(f"Cookies saved for account {self.account_id}")

    async def _listen_loop(self):
        while self.running:
            try:
                await self._check_new_messages()
            except Exception as e:
                logger.error(f"Listen loop error: {e}")
            await asyncio.sleep(8)

    async def _health_check_loop(self):
        while self.running:
            await asyncio.sleep(60)
            try:
                await self.page.evaluate("1 + 1")
            except Exception:
                logger.warning(f"Health check failed for {self.account_id}, restarting...")
                await self.restart()

    async def _check_new_messages(self):
        script = """
        (() => {
            const chats = document.querySelectorAll('div[data-testid="chat-list"] div[role="row"]');
            const results = [];
            for (const chat of chats) {
                const unread = chat.querySelector('span[data-testid="icon-unread-count"]');
                if (unread) {
                    const nameEl = chat.querySelector('span[title]');
                    const name = nameEl ? nameEl.getAttribute('title') : 'Unknown';
                    const phone = chat.getAttribute('data-id') || '';
                    const msgPreview = chat.querySelector('span[data-testid="last-message"]');
                    const content = msgPreview ? msgPreview.textContent : '';
                    results.push({ name, phone, content, timestamp: new Date().toISOString() });
                }
            }
            return results;
        })();
        """
        try:
            new_messages = await self.page.evaluate(script)
            if new_messages:
                for msg in new_messages:
                    if msg["content"]:
                        is_otp = bool(re.search(r'\b\d{4,6}\b', msg["content"]))
                        await self.db.save_message(
                            self.account_id,
                            msg["phone"],
                            msg["name"],
                            msg["content"],
                            msg["timestamp"],
                            is_otp
                        )
                        settings = await self.db.get_settings()
                        if not is_otp or settings.get("otp_highlight") == "true":
                            await self._notify_telegram(msg, is_otp)
                await self._mark_all_read()
        except Exception as e:
            logger.error(f"Error reading messages: {e}")

    async def _mark_all_read(self):
        try:
            await self.page.click('div[data-testid="chat-list"] div[role="row"]:first-child')
            await asyncio.sleep(1)
        except Exception:
            pass

    async def _notify_telegram(self, msg: Dict, is_otp: bool):
        await message_queue.put({
            "account_id": self.account_id,
            "from_name": msg["name"],
            "from_number": msg["phone"],
            "content": msg["content"],
            "timestamp": msg["timestamp"],
            "is_otp": is_otp,
        })

    async def send_text(self, phone: str, text: str) -> bool:
        phone = re.sub(r'\D', '', phone)
        if not phone:
            return False
        try:
            await self.page.goto(f"https://web.whatsapp.com/send?phone={phone}", waitUntil="networkidle0")
            await self.page.waitForSelector('div[data-testid="conversation-compose-box"]', timeout=10000)
            await self.page.type('div[data-testid="conversation-compose-box"]', text)
            await self.page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.error(f"Send text failed: {e}")
            return False

    async def send_media(self, phone: str, file_path: str, caption: str = "", media_type: str = "image"):
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        phone = re.sub(r'\D', '', phone)
        if not phone:
            return False
        try:
            await self.page.goto(f"https://web.whatsapp.com/send?phone={phone}", waitUntil="networkidle0")
            await self.page.waitForSelector('div[data-testid="conversation-compose-box"]', timeout=10000)
            attach_btn = await self.page.querySelector('div[data-testid="compose-attach-button"]')
            if attach_btn:
                await attach_btn.click()
                await asyncio.sleep(1)
                file_input = await self.page.querySelector('input[type="file"]')
                if file_input:
                    await file_input.uploadFile(file_path)
                    await asyncio.sleep(2)
                    send_btn = await self.page.querySelector('span[data-testid="send"]')
                    if send_btn:
                        await send_btn.click()
                        return True
            return False
        except Exception as e:
            logger.error(f"Send media failed: {e}")
            return False

    async def logout(self):
        self.running = False
        for task in [self.listening_task, self.health_check_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if USE_MONGO:
            self.db._mongo_db.accounts.update_one(
                {"_id": ObjectId(self.account_id)}, {"$set": {"session_data": ""}}
            )
        else:
            await self.db._sqlite_execute(
                "UPDATE accounts SET session_data = '' WHERE id = ?", (int(self.account_id),)
            )
        logger.info(f"Logged out account {self.account_id}")

    async def refresh(self):
        if self.page:
            await self.page.reload(waitUntil="networkidle0")
            if not await self._check_logged_in():
                await self._wait_for_login()

    async def restart(self):
        logger.info(f"Restarting client {self.account_id}")
        await self.logout()
        await self.start()


# ============================================================================
# TELEGRAM BOT
# ============================================================================
message_queue = asyncio.Queue()
active_clients: Dict[str, WhatsAppClient] = {}
db = Database()
bot_app = None
background_tasks = set()

SEND_PHONE_STATE, SEND_TEXT_STATE = 1, 2

def is_owner(update: Update) -> bool:
    return update.effective_user.id == OWNER_ID

def format_message_summary(msg: Dict) -> str:
    otp_tag = "🔴 **OTP** " if msg["is_otp"] else ""
    return (
        f"{otp_tag}👤 {msg['from_name']} ({msg['from_number']})\n"
        f"🕒 {msg['timestamp']}\n"
        f"💬 {msg['content']}"
    )

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Home", callback_data="home"),
         InlineKeyboardButton("➕ Add WhatsApp", callback_data="add_whatsapp")],
        [InlineKeyboardButton("📱 Connected Accounts", callback_data="list_accounts"),
         InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
        [InlineKeyboardButton("📤 Send Message", callback_data="send_message"),
         InlineKeyboardButton("💬 Inbox", callback_data="inbox")],
        [InlineKeyboardButton("📊 Statistics", callback_data="stats"),
         InlineKeyboardButton("⚙ Settings", callback_data="settings")],
        [InlineKeyboardButton("🗑 Remove Account", callback_data="remove_account"),
         InlineKeyboardButton("🚪 Logout", callback_data="logout")],
        [InlineKeyboardButton("❓ Help", callback_data="help")],
    ])

# ---------- Handlers ----------
async def start(update: Update, context):
    if not is_owner(update):
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 Welcome to the WhatsApp Bridge Bot!\nUse the menu below.",
        reply_markup=get_main_menu_keyboard(),
    )

async def button_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    if not is_owner(update):
        await query.edit_message_text("⛔ Unauthorized.")
        return
    data = query.data
    try:
        if data == "home":
            await query.edit_message_text("🏠 Main Menu", reply_markup=get_main_menu_keyboard())
        elif data == "add_whatsapp":
            await add_whatsapp(update, context)
        elif data == "list_accounts":
            await list_accounts(update, context)
        elif data == "refresh":
            await refresh_connection(update, context)
        elif data == "send_message":
            await send_message_prompt(update, context)
        elif data == "inbox":
            await inbox(update, context)
        elif data == "stats":
            await stats(update, context)
        elif data == "settings":
            await settings_menu(update, context)
        elif data == "remove_account":
            await remove_account_prompt(update, context)
        elif data == "logout":
            await logout_account(update, context)
        elif data == "help":
            await help_command(update, context)
        elif data.startswith("select_account_"):
            account_id = data.split("_")[2]
            context.user_data["selected_account"] = account_id
            await query.edit_message_text(f"📱 Selected account ID {account_id}.", reply_markup=get_main_menu_keyboard())
        elif data.startswith("remove_account_"):
            account_id = data.split("_")[2]
            await confirm_remove(update, context, account_id)
        elif data == "confirm_remove":
            account_id = context.user_data.get("remove_account_id")
            if account_id:
                await do_remove_account(update, context, account_id)
            else:
                await query.edit_message_text("No account to remove.")
        elif data == "cancel_remove":
            await query.edit_message_text("Removal cancelled.", reply_markup=get_main_menu_keyboard())
        elif data.startswith("settings_"):
            setting = data.split("_")[1]
            current = context.user_data.get("settings", {})
            new_val = "false" if current.get(setting, "true") == "true" else "true"
            await db.set_setting(setting, new_val)
            context.user_data["settings"] = await db.get_settings()
            await query.edit_message_text(
                f"✅ Setting '{setting}' updated to {new_val}.",
                reply_markup=get_main_menu_keyboard(),
            )
        else:
            await query.edit_message_text("Unknown action.")
    except Exception as e:
        logger.error(f"Button callback error: {e}")
        await query.edit_message_text("❌ An error occurred. Check logs.")

async def add_whatsapp(update: Update, context):
    account_id = await db.add_account(None, "{}")
    account_data = await db.get_account(account_id)
    client = WhatsAppClient(account_id, db, account_data)
    active_clients[account_id] = client
    asyncio.create_task(client.start())
    qr_data = await client.get_qr(timeout=30)
    if qr_data:
        try:
            image_data = base64.b64decode(qr_data)
            await update.callback_query.message.reply_photo(
                photo=BytesIO(image_data),
                caption=f"📲 Scan QR for account {account_id}.\nWill notify when connected.",
            )
            await update.callback_query.edit_message_text(
                "✅ QR sent. Waiting for login...", reply_markup=get_main_menu_keyboard()
            )
        except Exception as e:
            logger.error(f"QR send error: {e}")
            await update.callback_query.edit_message_text("❌ Failed to send QR.")
    else:
        await update.callback_query.edit_message_text("❌ QR not available. Try again.")

async def list_accounts(update: Update, context):
    accounts = await db.get_all_accounts()
    if not accounts:
        await update.callback_query.edit_message_text("No accounts.", reply_markup=get_main_menu_keyboard())
        return
    text = "📱 **Connected Accounts**\n\n"
    keyboard = []
    for acc in accounts:
        status = "🟢 Online" if acc["id"] in active_clients else "🔴 Offline"
        text += f"ID: `{acc['id']}`\n📞 {acc['phone'] or 'No phone'}\nStatus: {status}\nLast: {acc['last_used'] or 'Never'}\n\n"
        keyboard.append([InlineKeyboardButton(f"Select {acc['id']}", callback_data=f"select_account_{acc['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="home")])
    await update.callback_query.edit_message_text(
        text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
    )

async def refresh_connection(update: Update, context):
    account_id = context.user_data.get("selected_account")
    if not account_id:
        await update.callback_query.edit_message_text("Select account first.", reply_markup=get_main_menu_keyboard())
        return
    client = active_clients.get(account_id)
    if client:
        await client.refresh()
        await update.callback_query.edit_message_text(f"✅ Account {account_id} refreshed.", reply_markup=get_main_menu_keyboard())
    else:
        await update.callback_query.edit_message_text(f"❌ Account {account_id} not active.", reply_markup=get_main_menu_keyboard())

async def send_message_prompt(update: Update, context):
    account_id = context.user_data.get("selected_account")
    if not account_id:
        await update.callback_query.edit_message_text("Select account first.", reply_markup=get_main_menu_keyboard())
        return ConversationHandler.END
    await update.callback_query.edit_message_text(
        "📤 Enter phone number (country code, e.g., 1234567890):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="home")]]),
    )
    return SEND_PHONE_STATE

async def send_phone(update: Update, context):
    phone = update.message.text.strip()
    context.user_data["send_phone"] = phone
    await update.message.reply_text(
        "Now enter the message text:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="home")]]),
    )
    return SEND_TEXT_STATE

async def send_text_final(update: Update, context):
    text = update.message.text
    phone = context.user_data.get("send_phone")
    account_id = context.user_data.get("selected_account")
    client = active_clients.get(account_id)
    if not client:
        await update.message.reply_text("❌ Account not active.")
        return ConversationHandler.END
    success = await client.send_text(phone, text)
    await update.message.reply_text(
        "✅ Message sent!" if success else "❌ Failed to send.",
        reply_markup=get_main_menu_keyboard()
    )
    return ConversationHandler.END

async def inbox(update: Update, context):
    account_id = context.user_data.get("selected_account")
    if not account_id:
        await update.callback_query.edit_message_text("Select account first.", reply_markup=get_main_menu_keyboard())
        return
    messages = await db.get_messages(account_id, limit=20)
    if not messages:
        await update.callback_query.edit_message_text("📭 No messages.", reply_markup=get_main_menu_keyboard())
        return
    text = "💬 **Recent Messages**\n\n"
    for msg in reversed(messages):
        text += format_message_summary(msg) + "\n\n"
    if len(text) > 4000:
        text = text[:4000] + "\n... (truncated)"
    await update.callback_query.edit_message_text(
        text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard()
    )

async def stats(update: Update, context):
    account_id = context.user_data.get("selected_account")
    if not account_id:
        await update.callback_query.edit_message_text("Select account first.", reply_markup=get_main_menu_keyboard())
        return
    stats = await db.get_stats(account_id)
    account = await db.get_account(account_id)
    phone = account.get("phone", "Unknown") if account else "Unknown"
    text = (
        f"📊 **Stats for Account {account_id}**\n"
        f"📞 Phone: {phone}\n"
        f"📨 Total: {stats['total']}\n"
        f"🔴 OTP: {stats['otp']}\n"
        f"Last activity: {account.get('last_used', 'Never') if account else 'Never'}"
    )
    await update.callback_query.edit_message_text(
        text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard()
    )

async def settings_menu(update: Update, context):
    settings = await db.get_settings()
    context.user_data["settings"] = settings
    keyboard = [
        [InlineKeyboardButton(f"🔔 Notif: {'ON' if settings.get('notifications')=='true' else 'OFF'}", callback_data="settings_notifications")],
        [InlineKeyboardButton(f"🔴 OTP: {'ON' if settings.get('otp_highlight')=='true' else 'OFF'}", callback_data="settings_otp_highlight")],
        [InlineKeyboardButton(f"🔄 AutoReconn: {'ON' if settings.get('auto_reconnect')=='true' else 'OFF'}", callback_data="settings_auto_reconnect")],
        [InlineKeyboardButton("🔙 Back", callback_data="home")],
    ]
    await update.callback_query.edit_message_text("⚙ Settings", reply_markup=InlineKeyboardMarkup(keyboard))

async def remove_account_prompt(update: Update, context):
    accounts = await db.get_all_accounts()
    if not accounts:
        await update.callback_query.edit_message_text("No accounts.", reply_markup=get_main_menu_keyboard())
        return
    keyboard = [[InlineKeyboardButton(f"Remove {acc['id']} ({acc['phone'] or 'No phone'})", callback_data=f"remove_account_{acc['id']}")] for acc in accounts]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="home")])
    await update.callback_query.edit_message_text("Select account to remove:", reply_markup=InlineKeyboardMarkup(keyboard))

async def confirm_remove(update: Update, context, account_id: str):
    context.user_data["remove_account_id"] = account_id
    keyboard = [
        [InlineKeyboardButton("✅ Yes, remove", callback_data="confirm_remove")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_remove")],
    ]
    await update.callback_query.edit_message_text(f"⚠️ Remove account {account_id}?", reply_markup=InlineKeyboardMarkup(keyboard))

async def do_remove_account(update: Update, context, account_id: str):
    if account_id in active_clients:
        await active_clients[account_id].logout()
        del active_clients[account_id]
    await db.delete_account(account_id)
    await update.callback_query.edit_message_text(f"✅ Account {account_id} removed.", reply_markup=get_main_menu_keyboard())
    context.user_data.pop("remove_account_id", None)

async def logout_account(update: Update, context):
    account_id = context.user_data.get("selected_account")
    if not account_id:
        await update.callback_query.edit_message_text("Select account first.", reply_markup=get_main_menu_keyboard())
        return
    client = active_clients.get(account_id)
    if client:
        await client.logout()
        del active_clients[account_id]
        await update.callback_query.edit_message_text(f"✅ Logged out {account_id}.", reply_markup=get_main_menu_keyboard())
    else:
        await db.delete_account(account_id)
        await update.callback_query.edit_message_text(f"Account {account_id} removed.", reply_markup=get_main_menu_keyboard())

async def help_command(update: Update, context):
    text = (
        "❓ **Help**\n\n"
        "Bridge WhatsApp to Telegram.\n"
        "Features:\n"
        "- Add multiple WhatsApp accounts via QR.\n"
        "- Receive and reply to messages.\n"
        "- Send text (media limited).\n"
        "- OTP detection.\n"
        "Use the menu to navigate."
    )
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard())

async def message_notifier(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            msg = await asyncio.wait_for(message_queue.get(), timeout=1.0)
            text = format_message_summary(msg)
            if msg["is_otp"]:
                text = "🔴 **OTP DETECTED**\n" + text
            if bot_app:
                await bot_app.bot.send_message(chat_id=OWNER_ID, text=text, parse_mode=ParseMode.MARKDOWN)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Notifier error: {e}")

# ---------- Health Server ----------
async def health_handler(request):
    return web.Response(text="OK")

async def start_health_server():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Health check server running on port {PORT}")

# ---------- Main ----------
async def main():
    global bot_app
    request = HTTPXRequest(connect_timeout=30.0, read_timeout=30.0, write_timeout=30.0, pool_timeout=30.0)
    application = Application.builder().token(BOT_TOKEN).request(request).build()
    bot_app = application

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(send_message_prompt, pattern="^send_message$")],
        states={
            SEND_PHONE_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, send_phone)],
            SEND_TEXT_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, send_text_final)],
        },
        fallbacks=[CallbackQueryHandler(button_callback, pattern="^home$")],
    )
    application.add_handler(conv_handler)

    # Start health server
    asyncio.create_task(start_health_server())

    # Notifier task
    stop_event = asyncio.Event()
    notifier_task = asyncio.create_task(message_notifier(stop_event))
    background_tasks.add(notifier_task)

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    logger.info("Bot is running. Press Ctrl+C to stop.")

    # Restore WhatsApp sessions
    accounts = await db.get_all_accounts()
    for acc in accounts:
        if acc.get("session_data") and acc["session_data"] not in ("{}", ""):
            client = WhatsAppClient(acc["id"], db, acc)
            active_clients[acc["id"]] = client
            asyncio.create_task(client.start())

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        logger.info("Shutting down...")
        stop_event.set()
        notifier_task.cancel()
        try:
            await notifier_task
        except asyncio.CancelledError:
            pass
        await application.stop()
        for client in active_clients.values():
            await client.logout()
        active_clients.clear()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
