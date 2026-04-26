# data-builder

Python toolkit for Milestone Innovations Group: data scraping, augmentation, and agent-style orchestration (local CLI and importable library).

## Layout

| Path | Purpose |
|------|---------|
| `src/data_builder/` | Installable package (`config`, `agent`, `scrapers`, `models`, `storage`, `pipelines`) |
| `tests/` | Pytest suite |
| `scripts/` | One-off scripts (not run automatically) |
| `data/raw`, `data/processed`, `data/cache` | Local data directories (contents gitignored) |
| `.env.local` | Local secrets and tuning (gitignored) |

## Setup

Requires **Python 3.11+**.

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements-dev.txt
```

Copy or edit `.env.local` with real keys before running scrapers or uploads.

## CLI

After install:

```bash
data-builder --help
```

## Development

```bash
pytest
ruff check src tests
```
