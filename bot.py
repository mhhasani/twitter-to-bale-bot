"""
ربات جستجوگر بله
این ربات متن‌های ارسالی کاربران را در گوگل جستجو می‌کند
"""

import os
import logging
from dotenv import load_dotenv
from bale import Bot, Message, InlineKeyboardMarkup, InlineKeyboardButton
from google_search import quick_search

# بارگذاری متغیرهای محیطی
load_dotenv()

# تنظیمات لاگ
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# دریافت توکن ربات
BOT_TOKEN = os.getenv("BALE_BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("❌ توکن ربات یافت نشد! لطفاً فایل .env را بررسی کنید.")

# ساخت نمونه ربات
bot = Bot(token=BOT_TOKEN)


@bot.event
async def on_message(message: Message):
    """
    مدیریت پیام‌های دریافتی
    """
    try:
        # دریافت متن پیام
        text = message.text

        if not text:
            return

        # پاسخ به دستور /start
        if text.startswith("/start"):
            welcome_message = (
                "👋 سلام! به ربات جستجوگر خوش آمدید!\n\n"
                "🔍 من می‌تونم هر متنی که بهم بدی رو تو گوگل جستجو کنم.\n\n"
                "📝 کافیه متن مورد نظرت رو بفرستی تا برات جستجو کنم!"
            )
            await message.reply(welcome_message)
            logger.info(f"کاربر {message.author.user_id} ربات را استارت کرد")
            return

        # پاسخ به دستور /help
        if text.startswith("/help"):
            help_message = (
                "❓ راهنمای استفاده:\n\n"
                "1️⃣ هر متنی که می‌خواید جستجو بشه رو بفرستید\n"
                "2️⃣ منتظر بمونید تا نتایج رو برای شما بیارم\n"
                "3️⃣ روی لینک‌ها کلیک کنید تا به نتایج برسید\n\n"
                "مثال:\n"
                "شما: هوش مصنوعی چیست؟\n"
                'من: نتایج جستجو برای "هوش مصنوعی چیست؟" 🔍'
            )
            await message.reply(help_message)
            return

        # نمایش پیام در حال جستجو
        searching_msg = await message.reply("🔍 در حال جستجو در گوگل...")
        logger.info(f"جستجوی '{text}' توسط کاربر {message.author.user_id}")

        # جستجو در گوگل
        results = quick_search(text, num_results=5)

        # حذف پیام "در حال جستجو"
        try:
            await searching_msg.delete()
        except:
            pass

        # ارسال نتایج
        await message.reply(results)
        logger.info(f"نتایج جستجو برای '{text}' ارسال شد")

    except Exception as e:
        logger.error(f"خطا در پردازش پیام: {e}")
        try:
            await message.reply("❌ متأسفانه خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        except:
            pass


@bot.event
async def on_callback(callback_query):
    """
    مدیریت callback queryها (در صورت نیاز در آینده)
    """
    await callback_query.answer("✅ دریافت شد")


def main():
    """
    تابع اصلی برای اجرای ربات
    """
    try:
        logger.info("🤖 ربات در حال راه‌اندازی...")
        logger.info(f"🔑 استفاده از توکن: {BOT_TOKEN[:10]}...")

        # اجرای ربات
        bot.run()

    except KeyboardInterrupt:
        logger.info("⛔ ربات توسط کاربر متوقف شد")
    except Exception as e:
        logger.error(f"❌ خطا در اجرای ربات: {e}")
        raise


if __name__ == "__main__":
    main()
