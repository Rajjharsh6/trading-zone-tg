import asyncio
import json
import logging
import aiosqlite
import httpx
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# --- Configuration ---
TELEGRAM_TOKEN = "8935401546:AAF6am97F66ORoCnHnDooaTCqREqrtSv2HM"
OANDA_ACCESS_TOKEN = "309c0809eb47793a55d3cf3063b95921-1690f18abdd1c2647ca7a28dcfe1e1b4"
OANDA_ACCOUNT_ID = "101-001-40731602-001"  # Format: "101-001-XXXXXXX-001"
IS_PRACTICE = True  # True for Demo, False for Live

DB_FILE = "trading_zones.db"
OANDA_STREAM_HOST = (
    "stream-fxpractice.oanda.com" if IS_PRACTICE else "stream-fxtrade.oanda.com"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database Setup ---
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                symbol TEXT,
                low_price REAL,
                high_price REAL,
                zone_name TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        await db.commit()

# --- Telegram Commands ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "📈 *Market Zone Alert Bot Active*\n\n"
        "Commands:\n"
        "• `/addzone <SYMBOL> <LOW> <HIGH> [NAME]`\n"
        "  _Examples:_\n"
        "  `/addzone XAU_USD 2640.50 2645.00 Gold Demand`\n"
        "  `/addzone EUR_USD 1.0820 1.0835 15m Supply`\n\n"
        "• `/zones` — View active key levels\n"
        "• `/clear` — Remove all your tracked zones"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def add_zone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3:
        await update.message.reply_text(
            "⚠️ Usage: `/addzone <SYMBOL> <LOW> <HIGH> [NAME]`\n"
            "_Example: `/addzone XAU_USD 2640.50 2645.00 London Low`_",
            parse_mode="Markdown"
        )
        return

    raw_symbol = context.args[0].upper()
    symbol = raw_symbol if "_" in raw_symbol else f"{raw_symbol[:3]}_{raw_symbol[3:]}"

    try:
        low = float(context.args[1])
        high = float(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Low and High prices must be numbers.")
        return

    if low > high:
        low, high = high, low

    name = " ".join(context.args[3:]) if len(context.args) > 3 else "Key Level"

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO zones (chat_id, symbol, low_price, high_price, zone_name) VALUES (?, ?, ?, ?, ?)",
            (update.effective_chat.id, symbol, low, high, name)
        )
        await db.commit()

    await update.message.reply_text(
        f"✅ *Zone Saved!*\n"
        f"• *Symbol:* `{symbol}`\n"
        f"• *Range:* `{low} - {high}`\n"
        f"• *Label:* {name}\n\n"
        f"I will alert you as soon as price enters this zone.",
        parse_mode="Markdown"
    )

async def list_zones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT id, symbol, low_price, high_price, zone_name FROM zones WHERE chat_id = ? AND is_active = 1",
            (update.effective_chat.id,)
        ) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        await update.message.reply_text("No active zones monitored.")
        return

    text = "📌 *Active Monitored Zones:*\n\n"
    for r in rows:
        text += f"• `#{r[0]}` *{r[1]}* | Range: `{r[2]} - {r[3]}` ({r[4]})\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def clear_zones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE zones SET is_active = 0 WHERE chat_id = ?", (update.effective_chat.id,))
        await db.commit()
    await update.message.reply_text("🧹 All tracked zones deactivated.")

# --- Real-Time OANDA Streaming Engine ---
async def oanda_price_streamer(app):
    headers = {
        "Authorization": f"Bearer {OANDA_ACCESS_TOKEN}",
        "Accept-Datetime-Format": "RFC3339"
    }

    while True:
        try:
            # Check which symbols need monitoring
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT DISTINCT symbol FROM zones WHERE is_active = 1") as cursor:
                    symbols = [row[0] for row in await cursor.fetchall()]

            if not symbols:
                await asyncio.sleep(4)
                continue

            stream_url = f"https://{OANDA_STREAM_HOST}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"
            params = {"instruments": ",".join(symbols)}

            logger.info(f"Streaming live prices for: {params['instruments']}")

            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", stream_url, headers=headers, params=params) as response:
                    if response.status_code != 200:
                        err = await response.aread()
                        logger.error(f"Stream connection failed ({response.status_code}): {err.decode()}")
                        await asyncio.sleep(5)
                        continue

                    async for line in response.aiter_lines():
                        if not line:
                            continue

                        tick = json.loads(line)
                        if tick.get("type") != "PRICE":
                            continue

                        instrument = tick.get("instrument")
                        best_bid = float(tick["bids"][0]["price"])
                        best_ask = float(tick["asks"][0]["price"])
                        mid_price = (best_bid + best_ask) / 2.0

                        # Check if price hits any stored zone
                        async with aiosqlite.connect(DB_FILE) as db:
                            async with db.execute(
                                "SELECT id, chat_id, low_price, high_price, zone_name FROM zones WHERE symbol = ? AND is_active = 1",
                                (instrument,)
                            ) as cur:
                                matching_zones = await cur.fetchall()

                            triggered_ids = []
                            for z_id, chat_id, low, high, name in matching_zones:
                                if low <= mid_price <= high:
                                    alert_msg = (
                                        f"🚨 *ZONE REACHED!*\n\n"
                                        f"• *Symbol:* `{instrument}`\n"
                                        f"• *Current Price:* `{mid_price:.5f}`\n"
                                        f"• *Zone:* `{low} - {high}`\n"
                                        f"• *Label:* {name}\n\n"
                                        f"⚡ Market price has entered your marked zone."
                                    )
                                    try:
                                        await app.bot.send_message(
                                            chat_id=chat_id,
                                            text=alert_msg,
                                            parse_mode="Markdown"
                                        )
                                        triggered_ids.append(z_id)
                                    except Exception as ex:
                                        logger.error(f"Telegram alert delivery error: {ex}")

                            # Deactivate triggered zones so you don't get repeated alerts
                            if triggered_ids:
                                placeholders = ",".join("?" * len(triggered_ids))
                                await db.execute(
                                    f"UPDATE zones SET is_active = 0 WHERE id IN ({placeholders})",
                                    triggered_ids
                                )
                                await db.commit()

        except Exception as err:
            logger.warning(f"Connection dropped: {err}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# --- App Lifecycle ---
async def main():
    await init_db()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addzone", add_zone))
    app.add_handler(CommandHandler("zones", list_zones))
    app.add_handler(CommandHandler("clear", clear_zones))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    asyncio.create_task(oanda_price_streamer(app))

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())