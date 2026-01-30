# site-scrapers-

Web scraper for SteamRip.com games list.

## Features

- Scrapes game listings from SteamRip.com
- Persists data to SQLite database
- Generates JSON output files (All.Games.json, New.Games.json)
- Automated first-run dependency installation
- Headless mode support for CI environments

## Requirements

- Python 3.x
- Chrome/Chromium browser (for production scraping)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:

```bash
python scrape_steamrip.py
```

The script will:
1. Install dependencies on first run
2. Scrape the SteamRip games list
3. Save results to database and JSON files

## Testing

Run the test suite to verify functionality:

```bash
python test_scraper.py
```

This tests core functionality without requiring internet access or a browser.

## Output Files

- `All.Games.json` - Complete list of all games (sorted by name)
- `New.Games.json` - List of newly discovered games (newest first)
- `steamrip_games.db` - SQLite database with full history