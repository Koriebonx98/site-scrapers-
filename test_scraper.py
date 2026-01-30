#!/usr/bin/env python3
"""
test_scraper.py

Simple test to validate the scraper's core functionality without requiring internet access.
This tests the parsing and database logic without actually scraping a live site.
"""
import sys
import os
import sqlite3
import json
import tempfile
import shutil

# Import the scraper functions
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_steamrip as scraper


def test_clean_name():
    """Test the clean_name function"""
    print("Testing clean_name function...")
    
    # Test basic cleaning
    assert scraper.clean_name("Game Name") == "Game Name"
    
    # Test removal of 'free download'
    assert scraper.clean_name("Game Name free download") == "Game Name"
    assert scraper.clean_name("Game Name Free Download") == "Game Name"
    
    # Test removal of parentheses at end
    assert scraper.clean_name("Game Name (2021)") == "Game Name"
    
    # Test whitespace normalization
    assert scraper.clean_name("  Game   Name  ") == "Game Name"
    
    print("✓ clean_name tests passed")


def test_extract_games_from_html():
    """Test the HTML extraction function"""
    print("Testing extract_games_from_html function...")
    
    sample_html = """
    <html>
        <body>
            <a href="/game1-free-download">Game One</a>
            <a href="/game2-free-download">Game Two Free Download</a>
            <a href="https://steamrip.com/game3-free-download/">Game Three (2021)</a>
        </body>
    </html>
    """
    
    games = scraper.extract_games_from_html(sample_html)
    
    assert len(games) >= 3, f"Expected at least 3 games, got {len(games)}"
    
    # Verify structure
    for game in games:
        assert "Name" in game
        assert "Url" in game
        assert game["Name"] != ""
        assert game["Url"] != ""
    
    print(f"✓ Extracted {len(games)} games from HTML")
    print(f"  Sample: {games[0]}")


def test_database_operations():
    """Test database operations"""
    print("Testing database operations...")
    
    # Create a temporary directory for the test
    test_dir = tempfile.mkdtemp()
    
    try:
        db_path = os.path.join(test_dir, "test_games.db")
        
        # Connect and initialize DB
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        scraper.init_db(conn)
        
        # Verify tables were created
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert "runs" in tables
        assert "run_games" in tables
        assert "games" in tables
        
        # Test inserting games
        test_games = [
            {"Name": "Test Game 1", "Url": "https://example.com/game1"},
            {"Name": "Test Game 2", "Url": "https://example.com/game2"},
        ]
        
        first_run, new_entries = scraper.run_persist(conn, test_games)
        
        assert first_run is True, "First run should be detected"
        assert len(new_entries) == 0, "First run should have no new entries"
        
        # Verify games were inserted
        games_count = scraper.get_games_count(conn)
        assert games_count == 2, f"Expected 2 games, got {games_count}"
        
        # Test second run with one new game
        test_games_2 = [
            {"Name": "Test Game 1", "Url": "https://example.com/game1"},
            {"Name": "Test Game 2", "Url": "https://example.com/game2"},
            {"Name": "Test Game 3", "Url": "https://example.com/game3"},
        ]
        
        first_run, new_entries = scraper.run_persist(conn, test_games_2)
        
        assert first_run is False, "Should not be first run"
        assert len(new_entries) == 1, f"Expected 1 new entry, got {len(new_entries)}"
        assert new_entries[0]["Name"] == "Test Game 3"
        
        games_count = scraper.get_games_count(conn)
        assert games_count == 3, f"Expected 3 games, got {games_count}"
        
        conn.close()
        
        print("✓ Database operations tests passed")
        
    finally:
        # Clean up
        shutil.rmtree(test_dir)


def test_json_operations():
    """Test JSON file operations"""
    print("Testing JSON operations...")
    
    test_dir = tempfile.mkdtemp()
    
    try:
        db_path = os.path.join(test_dir, "test_games.db")
        all_games_path = os.path.join(test_dir, "All.Games.json")
        new_games_path = os.path.join(test_dir, "New.Games.json")
        
        # Setup database with test data
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        scraper.init_db(conn)
        
        test_games = [
            {"Name": "Zebra Game", "Url": "https://example.com/zebra"},
            {"Name": "Apple Game", "Url": "https://example.com/apple"},
            {"Name": "Beta Game", "Url": "https://example.com/beta"},
        ]
        
        scraper.run_persist(conn, test_games)
        
        # Test All.Games.json generation
        scraper.save_all_games_from_db(conn, all_games_path)
        
        assert os.path.exists(all_games_path), "All.Games.json should be created"
        
        with open(all_games_path, 'r') as f:
            all_games = json.load(f)
        
        assert len(all_games) == 3
        # Verify sorting (case-insensitive)
        assert all_games[0]["Name"] == "Apple Game"
        assert all_games[1]["Name"] == "Beta Game"
        assert all_games[2]["Name"] == "Zebra Game"
        
        # Test New.Games.json generation
        new_entries = [{"Name": "New Game", "Url": "https://example.com/new"}]
        scraper.save_new_games_file(new_entries, new_games_path)
        
        assert os.path.exists(new_games_path), "New.Games.json should be created"
        
        with open(new_games_path, 'r') as f:
            new_games = json.load(f)
        
        assert len(new_games) == 1
        assert new_games[0]["Name"] == "New Game"
        
        conn.close()
        
        print("✓ JSON operations tests passed")
        
    finally:
        shutil.rmtree(test_dir)


def main():
    """Run all tests"""
    print("=" * 60)
    print("Running scraper tests...")
    print("=" * 60)
    print()
    
    try:
        test_clean_name()
        print()
        
        test_extract_games_from_html()
        print()
        
        test_database_operations()
        print()
        
        test_json_operations()
        print()
        
        print("=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)
        return 0
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
