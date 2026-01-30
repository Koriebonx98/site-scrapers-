#!/usr/bin/env python3
"""
steamrip_scrape_db.py

v3-compatible scraper that persists runs to a SQLite DB and also writes FitGirl-style JSON files:

- All.Games.json : canonical list of all games discovered (array of {"Name","Url"}, sorted by Name)
- New.Games.json : historical list of newly-discovered games, newest-first (array of {"Name","Url"})

Behavior:
- On first run (no games in DB) the script will populate the DB and write All.Games.json.
  It will NOT create New.Games.json on the first run (matches FitGirl behavior).
- On subsequent runs new games are discovered, inserted into DB and flagged as new for that run.
  If there are newly discovered games this run:
    - All.Games.json is updated from the DB (sorted by Name)
    - New.Games.json is updated by prepending the truly-new entries for this run (so newest appear first),
      preserving any existing entries in New.Games.json afterwards.
- The script attempts to auto-install needed pip packages and to shim distutils via setuptools where possible
  to avoid "No module named 'distutils'" when importing undetected_chromedriver on modern Pythons.
- The script prints full tracebacks on error and waits for Enter before exiting so console windows remain open.

Usage:
    python steamrip_scrape_db.py
"""
from __future__ import annotations

import sys
import os
import subprocess
import importlib
import traceback
import re
import json
import sqlite3
from datetime import datetime, timezone
from html import unescape
from typing import Dict, List, Tuple, Optional
import tempfile

# Configuration
URL = "https://steamrip.com/games-list-page/"
DB_FILENAME = "steamrip_games.db"
JSON_ALL = "All.Games.json"
JSON_NEW = "New.Games.json"

# mapping import_name -> pip package name
PACKAGE_MAP: Dict[str, str] = {
    "setuptools": "setuptools",
    "selenium": "selenium",
    "webdriver_manager": "webdriver-manager",
    "undetected_chromedriver": "undetected-chromedriver",
}


# -------------------- Utilities: pip/install/distutils shim -------------------- #
def run_pip_install(packages: List[str]) -> None:
    if not packages:
        return
    cmd = [sys.executable, "-m", "pip", "install", "--upgrade"] + packages
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)


def try_install_and_verify(map_import_to_pip: Dict[str, str]) -> None:
    missing_pips: List[str] = []
    missing_imports: List[str] = []
    for import_name, pip_name in map_import_to_pip.items():
        try:
            importlib.import_module(import_name)
        except Exception:
            missing_imports.append(import_name)
            if pip_name not in missing_pips:
                missing_pips.append(pip_name)

    if missing_pips:
        try:
            run_pip_install(missing_pips)
        except subprocess.CalledProcessError as e:
            print("pip install failed:", e)
            print("Please run the following manually and re-run the script:")
            for p in missing_pips:
                print(f"  {sys.executable} -m pip install --upgrade {p}")

    # re-check imports and show any remaining problems
    still_missing = []
    for import_name in missing_imports:
        try:
            importlib.import_module(import_name)
        except Exception as ex:
            still_missing.append((import_name, ex))
    if still_missing:
        print("Warning: Some imports still unavailable after attempted install:")
        for name, ex in still_missing:
            print(f" - {name}: {ex}")


def ensure_imports(import_to_pip: Dict[str, str]) -> None:
    # install setuptools first (can provide distutils shim)
    if "setuptools" in import_to_pip:
        try_install_and_verify({"setuptools": import_to_pip["setuptools"]})
    rest = {k: v for k, v in import_to_pip.items() if k != "setuptools"}
    try_install_and_verify(rest)


def ensure_distutils_shim() -> None:
    """
    Try to expose setuptools._distutils as distutils so packages that import distutils still work.
    """
    try:
        import distutils  # type: ignore
        return
    except Exception:
        pass

    try:
        import setuptools  # type: ignore
    except Exception:
        try:
            print("Installing setuptools to provide distutils support...")
            run_pip_install(["setuptools"])
            import setuptools  # type: ignore
        except Exception as e:
            print("Could not ensure setuptools:", e)
            return

    try:
        sub = importlib.import_module("setuptools._distutils")
        sys.modules["distutils"] = sub
        try:
            ver = importlib.import_module("setuptools._distutils.version")
            sys.modules["distutils.version"] = ver
        except Exception:
            pass
        print("Shimmed 'distutils' using setuptools._distutils")
    except Exception as ex:
        print("Could not shim distutils:", ex)
        print(f"If you still see 'No module named distutils', run:\n  {sys.executable} -m pip install --upgrade setuptools")


# -------------------- Scraping helpers (v3 logic) -------------------- #
def clean_name(raw_text: str) -> str:
    if not raw_text:
        return ""
    text = raw_text.strip()
    text = re.sub(r"\s*free\s+download.*$", "", text, flags=re.I).strip()
    text = re.sub(r"\s*\(.*?\)\s*$", "", text).strip()
    text = re.sub(r"\s+", " ", text).strip(' -–+,:')
    return unescape(text)


def extract_games_from_html(html: str) -> List[Dict[str, str]]:
    """
    Extract anchors with '-free-download' in href; return list of {"Name","Url"}.
    This function mirrors the anchor-finding regex behavior used earlier.
    """
    pattern = r'<a\s+[^>]*href=["\']([^"\']*-free-download[^"\']*)["\'][^>]*>(.*?)</a>'
    results = []
    seen = set()
    for m in re.finditer(pattern, html, flags=re.I | re.S):
        raw_href = m.group(1).strip()
        raw_text = m.group(2).strip()
        if not raw_href:
            continue
        if raw_href.startswith("/"):
            href = "https://steamrip.com" + raw_href.lstrip("/")
        else:
            href = raw_href
        href = href.rstrip("/")
        name = clean_name(raw_text)
        if not name:
            slug = href.rstrip("/").split("/")[-1]
            slug_name = re.sub(r'-free-download$', '', slug, flags=re.I)
            name = slug_name.replace("-", " ").strip()
        if not name or not href:
            continue
        if href in seen:
            continue
        seen.add(href)
        results.append({"Name": name, "Url": href})
    return results


def scrape(driver) -> List[Dict[str, str]]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver.get(URL)
    wait = WebDriverWait(driver, 15)
    try:
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'-free-download') and starts-with(@href,'/')]")
        ))
    except Exception:
        pass

    anchors = driver.find_elements(By.XPATH, "//a[contains(@href,'-free-download') and starts-with(@href,'/')]")
    results = []
    seen = set()
    for a in anchors:
        href = a.get_attribute("href") or a.get_attribute("data-href") or ""
        if href and href.startswith("/"):
            href = "https://steamrip.com" + href.lstrip("/")
        href = href.rstrip("/")
        text = a.text or a.get_attribute("innerText") or ""
        name = clean_name(text)
        if not name:
            slug = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if slug:
                slug = slug.rstrip("/").split("/")[-1]
                name = re.sub(r'-free-download$', '', slug, flags=re.I)
                name = name.replace("-", " ").strip()
        if not name or not href:
            continue
        if href in seen:
            continue
        seen.add(href)
        results.append({"Name": name.strip(), "Url": href})
    return results


# -------------------- Database helpers (v3 schema/behavior) -------------------- #
def get_script_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    if "__file__" in globals():
        return os.path.dirname(os.path.abspath(__file__))
    return os.getcwd()


SCRIPT_DIR = get_script_dir()


# -------------------- First Run Detection -------------------- #
# Note: This tracks whether requirements installation has been attempted.
# This is separate from database first-run detection which tracks game data.
MARKER_FILENAME = 'first_run_success'
REQUIREMENTS_FILENAME = 'requirements.txt'

def is_first_run():
    """Check if this is the first run (requirements not yet installed)."""
    marker_path = os.path.join(SCRIPT_DIR, MARKER_FILENAME)
    return not os.path.exists(marker_path)

def mark_first_run_complete():
    """Mark that first-run requirements installation has been completed."""
    marker_path = os.path.join(SCRIPT_DIR, MARKER_FILENAME)
    with open(marker_path, 'w') as f:
        f.write('This file indicates that the first run tasks have been completed.')

def install_requirements():
    """Install packages from requirements.txt on first run."""
    requirements_path = os.path.join(SCRIPT_DIR, REQUIREMENTS_FILENAME)
    if not os.path.exists(requirements_path):
        print(f"Warning: {requirements_path} not found. Skipping requirements installation.")
        return
    
    try:
        print(f"First run detected. Installing requirements from {requirements_path}...")
        cmd = [sys.executable, "-m", "pip", "install", "-r", requirements_path]
        print("Running:", " ".join(cmd))
        subprocess.check_call(cmd)
        print("Requirements installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install requirements: {e}")
        print("Please run the following manually:")
        print(f"  {sys.executable} -m pip install -r {requirements_path}")
        raise


def default_db_path(filename: str) -> str:
    return os.path.join(SCRIPT_DIR, filename)


def fallback_db_path(filename: str) -> str:
    return os.path.join(os.path.expanduser("~"), filename)


def connect_db(path: Optional[str] = None) -> sqlite3.Connection:
    if path is None:
        path = default_db_path(DB_FILENAME)
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn
    except sqlite3.OperationalError as e:
        print(f"Failed to open DB at {path}: {e}")
        fb = fallback_db_path(DB_FILENAME)
        print(f"Attempting fallback DB at: {fb}")
        conn = sqlite3.connect(fb)
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at TEXT NOT NULL,
        snapshot_count INTEGER NOT NULL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS run_games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        Name TEXT,
        Url TEXT,
        is_new INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(run_id) REFERENCES runs(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT,
        Url TEXT UNIQUE,
        first_seen TEXT,
        last_seen TEXT
    )""")
    conn.commit()


def get_games_count(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM games")
    return cur.fetchone()[0]


def create_run(conn: sqlite3.Connection, run_at: str, snapshot_count: int) -> int:
    cur = conn.cursor()
    cur.execute("INSERT INTO runs(run_at, snapshot_count) VALUES (?, ?)", (run_at, snapshot_count))
    conn.commit()
    return cur.lastrowid


def insert_run_game(conn: sqlite3.Connection, run_id: int, name: str, url: str, is_new: bool) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO run_games(run_id, Name, Url, is_new) VALUES (?, ?, ?, ?)",
        (run_id, name, url, 1 if is_new else 0),
    )


def insert_game(conn: sqlite3.Connection, name: str, url: str, seen_at: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO games(Name, Url, first_seen, last_seen) VALUES (?, ?, ?, ?)",
        (name, url, seen_at, seen_at),
    )
    cur.execute("UPDATE games SET last_seen = ? WHERE Url = ?", (seen_at, url))


def update_game_last_seen(conn: sqlite3.Connection, url: str, seen_at: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE games SET last_seen = ? WHERE Url = ?", (seen_at, url))


def run_persist(conn: sqlite3.Connection, results: List[Dict[str, str]]) -> Tuple[bool, List[Dict[str, str]]]:
    run_at = datetime.now(timezone.utc).isoformat()
    pre_count = get_games_count(conn)
    run_id = create_run(conn, run_at, len(results))

    new_entries: List[Dict[str, str]] = []
    first_run = (pre_count == 0)

    for g in results:
        name = g.get("Name")
        url = (g.get("Url") or "").rstrip("/")
        if not url:
            continue

        cur = conn.cursor()
        cur.execute("SELECT id FROM games WHERE Url = ?", (url,))
        existing = cur.fetchone()
        if existing:
            update_game_last_seen(conn, url, run_at)
            insert_run_game(conn, run_id, name, url, is_new=False)
        else:
            insert_game(conn, name, url, run_at)
            is_new_flag = False if first_run else True
            insert_run_game(conn, run_id, name, url, is_new=is_new_flag)
            if is_new_flag:
                new_entries.append({"Name": name, "Url": url})

    conn.commit()
    return first_run, new_entries


# -------------------- JSON helpers (FitGirl-style output) -------------------- #
def load_json_games(path: str) -> List[Dict[str, str]]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return []
    return []


def save_all_games_from_db(conn: sqlite3.Connection, path: str) -> None:
    cur = conn.cursor()
    cur.execute("SELECT Name, Url FROM games")
    rows = cur.fetchall()
    games = [{"Name": r["Name"], "Url": r["Url"]} for r in rows]
    # sort by Name (case-insensitive) but keep original characters (so names like ".hack..." come first)
    games_sorted = sorted(games, key=lambda g: (g["Name"] or "").lower())
    with open(path, "w", encoding="utf-8") as f:
        json.dump(games_sorted, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(games_sorted)} games to {path}")


def save_new_games_file(new_entries: List[Dict[str, str]], path: str) -> None:
    if not new_entries:
        print("No new entries to write to", path)
        return
    existing_new = load_json_games(path)
    existing_urls = {g["Url"] for g in existing_new}
    truly_new = [g for g in new_entries if g["Url"] not in existing_urls]
    if not truly_new:
        print("No truly new entries to prepend to", path)
        return
    # Prepend newly discovered entries so newest appear first (like FitGirl sample expects newest items first)
    combined = truly_new + existing_new
    with open(path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(truly_new)} new games to {path}")


# -------------------- WebDriver helpers -------------------- #
def get_uc_driver():
    import undetected_chromedriver as uc
    opts = uc.ChromeOptions()
    # Enable headless mode in CI environments
    if os.environ.get("CI"):
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
    else:
        opts.add_argument("--start-maximized")
    return uc.Chrome(options=opts)


def get_selenium_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    chrome_opts = Options()
    # Enable headless mode in CI environments
    if os.environ.get("CI"):
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
    else:
        chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_opts.add_experimental_option("useAutomationExtension", False)
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_opts)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )
    except Exception:
        pass
    return driver


# -------------------- main -------------------- #
def main() -> None:
    driver = None
    conn: Optional[sqlite3.Connection] = None
    db_path_used: Optional[str] = None

    try:
        # Handle first run installation
        if is_first_run():
            install_requirements()
            mark_first_run_complete()
        else:
            print("First run tasks are already completed. Proceeding to scrape site.")
        
        # Ensure dependencies and distutils shim
        ensure_imports(PACKAGE_MAP)
        ensure_distutils_shim()

        # Start driver (try undetected_chromedriver first)
        try:
            print("Trying undetected-chromedriver...")
            driver = get_uc_driver()
        except Exception:
            print("undetected-chromedriver failed; falling back to selenium. Traceback:")
            traceback.print_exc()
            try:
                importlib.import_module("webdriver_manager")
            except Exception:
                try:
                    run_pip_install(["webdriver-manager"])
                except Exception:
                    pass
            driver = get_selenium_driver()

        # Scrape
        print("Scraping", URL)
        results = scrape(driver)

        # Connect DB (script dir preferred; fallback to home)
        try:
            conn = connect_db(default_db_path(DB_FILENAME))
            db_path_used = default_db_path(DB_FILENAME)
        except Exception:
            conn = connect_db(fallback_db_path(DB_FILENAME))
            db_path_used = fallback_db_path(DB_FILENAME)
        print("Using DB at:", db_path_used)

        # Persist results
        if not results:
            print("No matching anchors found. You may need to increase wait time or the page structure changed.")
            create_run(conn, datetime.now(timezone.utc).isoformat(), 0)
            conn.commit()
            print("Created an empty run record in the database.")
            print("\nRun completed (empty snapshot).")
            if not os.environ.get("CI"):
                input("Done. Press Enter to quit and close the browser...")
            return

        first_run, new_entries = run_persist(conn, results)

        # JSON behavior like FitGirl:
        if first_run:
            print("First run detected. Writing All.Games.json from DB and NOT creating New.Games.json.")
            save_all_games_from_db(conn, os.path.join(SCRIPT_DIR, JSON_ALL))
        else:
            if new_entries:
                print(f"Found {len(new_entries)} new entries this run. Updating All.Games.json and New.Games.json.")
                save_all_games_from_db(conn, os.path.join(SCRIPT_DIR, JSON_ALL))
                save_new_games_file(new_entries, os.path.join(SCRIPT_DIR, JSON_NEW))
            else:
                print("No new games found this run.")

        print("\nRun completed normally.")
        if not os.environ.get("CI"):
            input("Done. Press Enter to quit and close the browser...")

    except Exception:
        print("\nAn unhandled exception occurred:")
        traceback.print_exc()
        if not os.environ.get("CI"):
            try:
                input("\nPress Enter to exit and close the browser...")
            except Exception:
                pass
        sys.exit(1)

    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()