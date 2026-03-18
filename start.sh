#!/bin/bash

# اسکریپت راه‌اندازی ربات بله

echo "🤖 راه‌اندازی ربات جستجوگر بله..."
echo ""

# بررسی نصب Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 نصب نیست. لطفاً ابتدا Python را نصب کنید."
    exit 1
fi

echo "✅ Python نصب شده است"

# بررسی وجود فایل .env
if [ ! -f .env ]; then
    echo "⚠️  فایل .env یافت نشد"
    echo "📝 در حال کپی از .env.example..."
    cp .env.example .env
    echo ""
    echo "⚠️  لطفاً توکن ربات خود را در فایل .env وارد کنید:"
    echo "   nano .env"
    echo ""
    echo "سپس دوباره این اسکریپت را اجرا کنید."
    exit 1
fi

echo "✅ فایل .env موجود است"

# بررسی نصب pip
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 نصب نیست"
    exit 1
fi

echo "✅ pip نصب شده است"

# بررسی و نصب virtualenv
if [ ! -d "venv" ]; then
    echo "📦 در حال ساخت محیط مجازی..."
    python3 -m venv venv
    echo "✅ محیط مجازی ساخته شد"
fi

# فعال‌سازی محیط مجازی
echo "🔄 فعال‌سازی محیط مجازی..."
source venv/bin/activate

# نصب وابستگی‌ها
echo "📥 در حال نصب وابستگی‌ها..."
pip install -r requirements.txt

echo ""
echo "✅ همه چیز آماده است!"
echo "🚀 در حال اجرای ربات..."
echo ""

# اجرای ربات
python bot.py
