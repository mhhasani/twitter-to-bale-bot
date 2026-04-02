# archive_bot_v2

A clean, scalable rewrite of `archive_bot` with preserved runtime behavior.

## What changed

- Cleaner project structure (`app/` package and `ArchiveBotApp` orchestrator)
- English comments and docs in the rewritten project
- Better separation of concerns (config, message utilities, runtime app)
- Fully self-contained runtime, database layer, and analyzer logic inside this folder

## Why this keeps behavior stable

The new project contains local copies of the original database and analyzer logic, while the runtime layer is reorganized into a cleaner structure. This keeps command flow and chat behavior aligned without depending on any sibling project files.

## Run

1. Copy environment file:
   - `cp .env.example .env`
2. Fill required variables.
3. Start:
   - `bash start.sh`

Or manually:

- `python3 -m venv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`
- `python bot.py`
