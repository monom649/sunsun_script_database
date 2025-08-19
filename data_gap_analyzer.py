#!/usr/bin/env python3
"""
Data Gap Analyzer

This script compares the current database with the complete Google Spreadsheet data
to identify missing entries and data gaps.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os

class DataGapAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.gid = "1504431244"
        
    def fetch_complete_spreadsheet_data(self):
        """Fetch complete data from Google Spreadsheet"""
        try:
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.gid}"
            
            print(f"Fetching complete spreadsheet data...")
            response = requests.get(csv_url)
            response.raise_for_status()
            
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            return df
            
        except Exception as e:
            print(f"Error fetching spreadsheet data: {str(e)}")
            return None
    
    def extract_all_scripts_from_spreadsheet(self, df):
        """Extract all valid scripts from spreadsheet"""
        all_scripts = []
        
        for index, row in df.iterrows():
            if index < 4:  # Skip header rows
                continue
                
            broadcast_date = row.get('Unnamed: 2')  # 配信日
            management_id = row.get('Unnamed: 3')   # 管理番号
            title = row.get('台本テンプレ')           # 動画タイトル
            script_url = row.get('https://docs.google.com/spreadsheets/d/1uH7Y0hYMnLoLMhew4jYPnm0vkAVXRkQXoYOzTftg2Q8/edit?gid=1007786454#gid=1007786454')
            
            # Skip invalid entries
            if pd.isna(management_id) or pd.isna(title) or title == '未定':
                continue
                
            # Only process B-series management IDs
            if isinstance(management_id, str) and management_id.startswith('B'):
                all_scripts.append({
                    'management_id': management_id.strip(),
                    'title': title.strip() if isinstance(title, str) else '',
                    'broadcast_date': broadcast_date.strip() if isinstance(broadcast_date, str) and not pd.isna(broadcast_date) else '',
                    'script_url': script_url.strip() if isinstance(script_url, str) and not pd.isna(script_url) else '',
                    'row_index': index
                })
        
        return all_scripts
    
    def get_database_scripts(self):
        """Get all scripts currently in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT management_id, title, broadcast_date, script_url
                FROM scripts
                WHERE management_id LIKE 'B%'
                ORDER BY management_id
            """)
            
            db_scripts = []
            for row in cursor.fetchall():
                db_scripts.append({
                    'management_id': row[0],
                    'title': row[1],
                    'broadcast_date': row[2],
                    'script_url': row[3]
                })
            
            conn.close()
            return db_scripts
            
        except Exception as e:
            print(f"Error getting database scripts: {str(e)}")
            return []
    
    def analyze_gaps(self):
        """Analyze gaps between spreadsheet and database"""
        print("=== Data Gap Analysis ===")
        
        # Fetch spreadsheet data
        df = self.fetch_complete_spreadsheet_data()
        if df is None:
            return
        
        print(f"Spreadsheet contains {len(df)} total rows")
        
        # Extract all scripts from spreadsheet
        spreadsheet_scripts = self.extract_all_scripts_from_spreadsheet(df)
        print(f"Found {len(spreadsheet_scripts)} valid scripts in spreadsheet")
        
        # Get database scripts
        db_scripts = self.get_database_scripts()
        print(f"Database contains {len(db_scripts)} B-series scripts")
        
        # Create sets for comparison
        spreadsheet_ids = set(script['management_id'] for script in spreadsheet_scripts)
        db_ids = set(script['management_id'] for script in db_scripts)
        
        # Find missing scripts
        missing_in_db = spreadsheet_ids - db_ids
        missing_in_spreadsheet = db_ids - spreadsheet_ids
        
        print(f"\n=== Gap Analysis Results ===")
        print(f"Scripts in spreadsheet: {len(spreadsheet_ids)}")
        print(f"Scripts in database: {len(db_ids)}")
        print(f"Missing in database: {len(missing_in_db)}")
        print(f"Missing in spreadsheet: {len(missing_in_spreadsheet)}")
        
        # Show missing scripts details
        if missing_in_db:
            print(f"\n=== Missing in Database ({len(missing_in_db)} scripts) ===")
            missing_scripts = [s for s in spreadsheet_scripts if s['management_id'] in missing_in_db]
            
            # Group by year
            missing_by_year = {}
            for script in missing_scripts:
                date = script['broadcast_date']
                if date and len(date) >= 8:
                    year = '20' + date[:2]
                    if year not in missing_by_year:
                        missing_by_year[year] = []
                    missing_by_year[year].append(script)
                else:
                    if 'Unknown' not in missing_by_year:
                        missing_by_year['Unknown'] = []
                    missing_by_year['Unknown'].append(script)
            
            for year in sorted(missing_by_year.keys()):
                scripts = missing_by_year[year]
                print(f"\n{year} ({len(scripts)} scripts):")
                for script in sorted(scripts, key=lambda x: x['management_id'])[:10]:  # Show first 10
                    print(f"  {script['management_id']}: {script['title']} ({script['broadcast_date']})")
                if len(scripts) > 10:
                    print(f"  ... and {len(scripts) - 10} more")
        
        if missing_in_spreadsheet:
            print(f"\n=== In Database but not in Spreadsheet ({len(missing_in_spreadsheet)} scripts) ===")
            extra_scripts = [s for s in db_scripts if s['management_id'] in missing_in_spreadsheet]
            for script in extra_scripts[:10]:  # Show first 10
                print(f"  {script['management_id']}: {script['title']} ({script['broadcast_date']})")
            if len(extra_scripts) > 10:
                print(f"  ... and {len(extra_scripts) - 10} more")
        
        # Analyze 2025 data specifically
        print(f"\n=== 2025 Data Analysis ===")
        spreadsheet_2025 = [s for s in spreadsheet_scripts if s['broadcast_date'] and s['broadcast_date'].startswith('25/')]
        db_2025 = [s for s in db_scripts if s['broadcast_date'] and s['broadcast_date'].startswith('25/')]
        
        print(f"2025 scripts in spreadsheet: {len(spreadsheet_2025)}")
        print(f"2025 scripts in database: {len(db_2025)}")
        
        # Monthly breakdown for 2025
        spreadsheet_2025_by_month = {}
        db_2025_by_month = {}
        
        for script in spreadsheet_2025:
            month = script['broadcast_date'][3:5] if len(script['broadcast_date']) >= 5 else 'Unknown'
            spreadsheet_2025_by_month[month] = spreadsheet_2025_by_month.get(month, 0) + 1
        
        for script in db_2025:
            month = script['broadcast_date'][3:5] if len(script['broadcast_date']) >= 5 else 'Unknown'
            db_2025_by_month[month] = db_2025_by_month.get(month, 0) + 1
        
        print("\n2025 Monthly comparison:")
        all_months = set(list(spreadsheet_2025_by_month.keys()) + list(db_2025_by_month.keys()))
        for month in sorted(all_months):
            ss_count = spreadsheet_2025_by_month.get(month, 0)
            db_count = db_2025_by_month.get(month, 0)
            diff = ss_count - db_count
            status = "✓" if diff == 0 else f"(-{abs(diff)})" if diff < 0 else f"(+{diff})"
            print(f"  {month}月: スプレッドシート {ss_count}件, DB {db_count}件 {status}")
        
        return missing_in_db, missing_in_spreadsheet

def main():
    """Main execution function"""
    
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    analyzer = DataGapAnalyzer(db_path)
    missing_in_db, missing_in_spreadsheet = analyzer.analyze_gaps()
    
    print(f"\n=== Summary ===")
    print(f"Analysis complete. Found {len(missing_in_db)} scripts missing in database.")

if __name__ == "__main__":
    main()