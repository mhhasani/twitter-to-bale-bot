#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "🤖 راه‌اندازی پروژه مستقل archive_bot..."

if ! command -v python3 &> /dev/null; then
  echo "❌ Python 3 نصب نیست."
  exit 1
fi

if [ ! -f .env ]; then
  echo "⚠️ فایل .env در archive_bot پیدا نشد."
  echo "لطفاً این فایل را بسازید یا از .env.example کپی کنید."
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "📦 ساخت محیط مجازی..."
  python3 -m venv venv
fi

source venv/bin/activate

echo "📥 نصب وابستگی‌ها..."
pip install -r requirements.txt

echo "🚀 اجرای ربات..."
python bot.py
