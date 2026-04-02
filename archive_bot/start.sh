#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "🤖 Starting archive_bot_v2..."

if ! command -v python3 &> /dev/null; then
  echo "❌ Python 3 is not installed."
  exit 1
fi

if [ ! -f .env ]; then
  echo "⚠️ .env file was not found in archive_bot_v2."
  echo "Please create it or copy from .env.example first."
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv venv
fi

source venv/bin/activate

echo "📥 Installing dependencies..."
pip install -r requirements.txt

echo "🚀 Running bot..."
python bot.py
