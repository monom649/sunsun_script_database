#!/usr/bin/env python3
"""
Google Spreadsheet Data Fetcher and Database Updater

This script fetches data from the Google Spreadsheet "作業進捗_new" and updates
the local database with missing entries.

URL: https://docs.google.com/spreadsheets/d/1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8/edit?gid=1504431244#gid=1504431244
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os

class DatabaseUpdater:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.gid = "1504431244"  # Sheet ID from URL
        
    def fetch_spreadsheet_data(self):
        """Fetch data from Google Spreadsheet as CSV"""
        try:
            # Construct CSV export URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.gid}"
            
            print(f"Fetching data from: {csv_url}")
            
            # Fetch the CSV data
            response = requests.get(csv_url)
            response.raise_for_status()
            
            # Parse CSV data
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            print(f"Fetched {len(df)} rows from spreadsheet")
            print("Columns:", df.columns.tolist())
            print("Sample data:")
            print(df.head())
            
            return df
            
        except Exception as e:
            print(f"Error fetching spreadsheet data: {str(e)}")
            return None
    
    def analyze_current_database(self):
        """Analyze current database to understand what's missing"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current date range
            cursor.execute("SELECT MIN(broadcast_date), MAX(broadcast_date), COUNT(*) FROM scripts")
            min_date, max_date, total_count = cursor.fetchone()
            
            print(f"Current database:")
            print(f"  Date range: {min_date} to {max_date}")
            print(f"  Total scripts: {total_count}")
            
            # Get 2025 data
            cursor.execute("SELECT COUNT(*) FROM scripts WHERE broadcast_date LIKE '25/%'")
            count_2025 = cursor.fetchone()[0]
            print(f"  2025 scripts: {count_2025}")
            
            # Get management ID ranges
            cursor.execute("SELECT MIN(management_id), MAX(management_id) FROM scripts WHERE management_id LIKE 'B%'")
            min_id, max_id = cursor.fetchone()
            print(f"  Management ID range: {min_id} to {max_id}")
            
            conn.close()
            
            return {
                'min_date': min_date,
                'max_date': max_date,
                'total_count': total_count,
                'count_2025': count_2025,
                'min_id': min_id,
                'max_id': max_id
            }
            
        except Exception as e:
            print(f"Error analyzing database: {str(e)}")
            return None
    
    def extract_script_data(self, df):
        """Extract script data from spreadsheet DataFrame"""
        scripts = []
        
        # Skip header rows and process data starting from row 4 (index 4)
        for index, row in df.iterrows():
            if index < 4:  # Skip header rows
                continue
                
            # Extract key fields
            broadcast_date = row.get('Unnamed: 2')  # 配信日
            management_id = row.get('Unnamed: 3')   # 管理番号
            title = row.get('台本テンプレ')           # 動画タイトル
            script_url = row.get('https://docs.google.com/spreadsheets/d/1uH7Y0hYMnLoLMhew4jYPnm0vkAVXRkQXoYOzTftg2Q8/edit?gid=1007786454#gid=1007786454')  # 構成台本URL
            
            # Skip rows without essential data
            if pd.isna(management_id) or pd.isna(title) or title == '未定':
                continue
                
            # Clean and validate data
            if isinstance(management_id, str) and management_id.startswith('B'):
                script_data = {
                    'management_id': management_id.strip(),
                    'title': title.strip() if isinstance(title, str) else '',
                    'broadcast_date': broadcast_date.strip() if isinstance(broadcast_date, str) and not pd.isna(broadcast_date) else '',
                    'script_url': script_url.strip() if isinstance(script_url, str) and not pd.isna(script_url) else '',
                    'source_sheet': 'spreadsheet_import'
                }
                scripts.append(script_data)
        
        return scripts
    
    def check_existing_scripts(self, scripts):
        """Check which scripts already exist in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            existing_ids = set()
            new_scripts = []
            
            for script in scripts:
                cursor.execute("SELECT management_id FROM scripts WHERE management_id = ?", (script['management_id'],))
                if cursor.fetchone():
                    existing_ids.add(script['management_id'])
                else:
                    new_scripts.append(script)
            
            conn.close()
            
            print(f"Found {len(existing_ids)} existing scripts, {len(new_scripts)} new scripts to add")
            
            return new_scripts, existing_ids
            
        except Exception as e:
            print(f"Error checking existing scripts: {str(e)}")
            return [], set()
    
    def insert_new_scripts(self, new_scripts):
        """Insert new scripts into database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted_count = 0
            
            for script in new_scripts:
                try:
                    cursor.execute("""
                        INSERT INTO scripts (management_id, title, broadcast_date, script_url, source_sheet)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        script['management_id'],
                        script['title'],
                        script['broadcast_date'],
                        script['script_url'],
                        script['source_sheet']
                    ))
                    inserted_count += 1
                    
                except sqlite3.Error as e:
                    print(f"Error inserting {script['management_id']}: {str(e)}")
            
            conn.commit()
            conn.close()
            
            print(f"Successfully inserted {inserted_count} new scripts")
            return inserted_count
            
        except Exception as e:
            print(f"Error inserting scripts: {str(e)}")
            return 0
    
    def update_database(self, df):
        """Update database with new data from spreadsheet"""
        print("Processing spreadsheet data...")
        
        # Extract script data from spreadsheet
        scripts = self.extract_script_data(df)
        print(f"Extracted {len(scripts)} scripts from spreadsheet")
        
        if len(scripts) > 0:
            # Show sample of extracted data
            print("Sample extracted scripts:")
            for script in scripts[:5]:
                print(f"  {script['management_id']}: {script['title']} ({script['broadcast_date']})")
            
            # Check which scripts are new
            new_scripts, existing_ids = self.check_existing_scripts(scripts)
            
            if new_scripts:
                print(f"\nInserting {len(new_scripts)} new scripts...")
                inserted = self.insert_new_scripts(new_scripts)
                print(f"Database update complete: {inserted} scripts added")
            else:
                print("No new scripts to add - all scripts already exist in database")
        
        return len(scripts)

def main():
    """Main execution function"""
    
    # Path to the local database
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    updater = DatabaseUpdater(db_path)
    
    print("=== Database Update Process ===")
    
    # 1. Analyze current database
    print("\n1. Analyzing current database...")
    current_status = updater.analyze_current_database()
    
    # 2. Fetch spreadsheet data
    print("\n2. Fetching spreadsheet data...")
    df = updater.fetch_spreadsheet_data()
    
    if df is not None:
        # 3. Analyze spreadsheet structure
        print("\n3. Analyzing spreadsheet structure...")
        print(f"Spreadsheet contains {len(df)} rows")
        
        # Show sample to understand structure
        if len(df) > 0:
            print("\nFirst few rows:")
            for i, row in df.head().iterrows():
                print(f"Row {i}: {dict(row)}")
    
        # 4. Update database with new scripts
        print("\n4. Updating database...")
        total_processed = updater.update_database(df)
        
        # 5. Final analysis after update
        print("\n5. Final database analysis...")
        final_status = updater.analyze_current_database()
    
    print("\n=== Database Update Complete ===")
    if current_status and final_status:
        print(f"Scripts before update: {current_status['total_count']}")
        print(f"Scripts after update: {final_status['total_count']}")
        print(f"New scripts added: {final_status['total_count'] - current_status['total_count']}")
        print(f"2025 scripts before: {current_status['count_2025']}")
        print(f"2025 scripts after: {final_status['count_2025']}")
    print("Database successfully updated with missing data from Google Spreadsheet!")

if __name__ == "__main__":
    main()