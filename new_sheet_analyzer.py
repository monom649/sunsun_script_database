#!/usr/bin/env python3
"""
New Sheet Analyzer

This script analyzes the new Google Spreadsheet sheet (gid=1351011705) and 
identifies new data that is not already in the database.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os

class NewSheetAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.new_gid = "1351011705"  # New sheet GID
        
    def fetch_new_sheet_data(self):
        """Fetch data from the new Google Spreadsheet sheet"""
        try:
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.new_gid}"
            
            print(f"Fetching new sheet data from: {csv_url}")
            response = requests.get(csv_url)
            response.raise_for_status()
            
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            print(f"Fetched {len(df)} rows from new sheet")
            print("Columns:", df.columns.tolist())
            print("\nSample data:")
            print(df.head())
            
            return df
            
        except Exception as e:
            print(f"Error fetching new sheet data: {str(e)}")
            return None
    
    def extract_scripts_from_new_sheet(self, df):
        """Extract script data from new sheet DataFrame"""
        scripts = []
        
        if df is None:
            return scripts
        
        print(f"\nAnalyzing {len(df)} rows from new sheet...")
        
        # Analyze the structure of the new sheet
        print("First few rows for structure analysis:")
        for i, row in df.head(10).iterrows():
            print(f"Row {i}: {dict(row)}")
        
        # Try to identify relevant columns
        for index, row in df.iterrows():
            # Skip obvious header rows
            if index < 5:  # Allow more header rows for different format
                continue
            
            # Look for management IDs and titles in any column
            row_dict = dict(row)
            management_id = None
            title = None
            broadcast_date = None
            script_url = None
            
            # Search through all columns for management ID pattern
            for col_name, value in row_dict.items():
                if pd.isna(value):
                    continue
                    
                value_str = str(value).strip()
                
                # Look for management ID pattern (B followed by numbers)
                if re.match(r'^B\d+', value_str):
                    management_id = value_str
                
                # Look for date pattern (YY/MM/DD)
                if re.match(r'^\d{2}/\d{2}/\d{2}$', value_str):
                    broadcast_date = value_str
                
                # Look for Google Sheets URL
                if 'docs.google.com/spreadsheets' in value_str:
                    script_url = value_str
                
                # Look for title (non-empty text that's not ID or date)
                if (not title and 
                    len(value_str) > 3 and 
                    not re.match(r'^B\d+', value_str) and 
                    not re.match(r'^\d{2}/\d{2}/\d{2}$', value_str) and
                    'docs.google.com' not in value_str and
                    value_str not in ['NaN', 'nan', '未定']):
                    title = value_str
            
            # If we found a management ID, add the script
            if management_id:
                script_data = {
                    'management_id': management_id,
                    'title': title or '',
                    'broadcast_date': broadcast_date or '',
                    'script_url': script_url or '',
                    'row_index': index,
                    'source_sheet': 'new_sheet_import'
                }
                scripts.append(script_data)
                
                # Show first few found scripts for verification
                if len(scripts) <= 5:
                    print(f"Found script: {management_id} - {title} ({broadcast_date})")
        
        print(f"\nExtracted {len(scripts)} scripts from new sheet")
        return scripts
    
    def get_existing_management_ids(self):
        """Get all existing management IDs from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT management_id FROM scripts")
            existing_ids = set(row[0] for row in cursor.fetchall())
            
            conn.close()
            return existing_ids
            
        except Exception as e:
            print(f"Error getting existing management IDs: {str(e)}")
            return set()
    
    def filter_new_scripts(self, new_scripts):
        """Filter out scripts that already exist in database"""
        existing_ids = self.get_existing_management_ids()
        
        new_only = []
        duplicates = []
        
        for script in new_scripts:
            if script['management_id'] in existing_ids:
                duplicates.append(script)
            else:
                new_only.append(script)
        
        print(f"\nFiltering results:")
        print(f"Total scripts found in new sheet: {len(new_scripts)}")
        print(f"Already exist in database: {len(duplicates)}")
        print(f"New scripts to add: {len(new_only)}")
        
        return new_only, duplicates
    
    def analyze_new_sheet(self):
        """Main analysis function"""
        print("=== New Sheet Analysis ===")
        
        # Fetch new sheet data
        df = self.fetch_new_sheet_data()
        if df is None:
            return [], []
        
        # Extract scripts from new sheet
        new_scripts = self.extract_scripts_from_new_sheet(df)
        
        # Filter out duplicates
        new_only, duplicates = self.filter_new_scripts(new_scripts)
        
        # Show summary
        if new_only:
            print(f"\n=== New Scripts to Add ({len(new_only)}) ===")
            
            # Group by year
            by_year = {}
            for script in new_only:
                date = script['broadcast_date']
                if date and len(date) >= 8:
                    year = '20' + date[:2]
                else:
                    year = 'Unknown'
                
                if year not in by_year:
                    by_year[year] = []
                by_year[year].append(script)
            
            for year in sorted(by_year.keys()):
                scripts = by_year[year]
                print(f"\n{year} ({len(scripts)} scripts):")
                for script in scripts[:10]:  # Show first 10
                    print(f"  {script['management_id']}: {script['title']} ({script['broadcast_date']})")
                if len(scripts) > 10:
                    print(f"  ... and {len(scripts) - 10} more")
        
        if duplicates:
            print(f"\n=== Duplicate Scripts (already in DB): {len(duplicates)} ===")
            for script in duplicates[:5]:  # Show first 5
                print(f"  {script['management_id']}: {script['title']}")
            if len(duplicates) > 5:
                print(f"  ... and {len(duplicates) - 5} more")
        
        return new_only, duplicates
    
    def insert_new_scripts(self, new_scripts):
        """Insert new scripts into database"""
        if not new_scripts:
            print("No new scripts to insert.")
            return 0
        
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

def main():
    """Main execution function"""
    
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    analyzer = NewSheetAnalyzer(db_path)
    
    # Analyze new sheet
    new_scripts, duplicates = analyzer.analyze_new_sheet()
    
    # Insert new scripts if found
    if new_scripts:
        print(f"\n=== Inserting {len(new_scripts)} New Scripts ===")
        inserted = analyzer.insert_new_scripts(new_scripts)
        print(f"Database update complete: {inserted} scripts added from new sheet")
    else:
        print("\nNo new scripts found in the new sheet.")

if __name__ == "__main__":
    main()