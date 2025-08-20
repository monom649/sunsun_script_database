#!/usr/bin/env python3
"""
Script URL Coverage Analyzer

This script checks if the dialogue data has been extracted from the script URLs
for the 2025 Q1 (Jan-Apr) scripts that were added from the spreadsheet.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os

class ScriptURLCoverageAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.gid = "1504431244"
        
    def get_2025_q1_scripts_from_db(self):
        """Get 2025 Q1 scripts from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get 2025 Jan-Apr scripts
            cursor.execute("""
                SELECT id, management_id, title, broadcast_date, script_url
                FROM scripts 
                WHERE broadcast_date LIKE '25/01/%' 
                   OR broadcast_date LIKE '25/02/%'
                   OR broadcast_date LIKE '25/03/%' 
                   OR broadcast_date LIKE '25/04/%'
                ORDER BY broadcast_date, management_id
            """)
            
            scripts = []
            for row in cursor.fetchall():
                scripts.append({
                    'id': row[0],
                    'management_id': row[1],
                    'title': row[2],
                    'broadcast_date': row[3],
                    'script_url': row[4] or ''
                })
            
            conn.close()
            return scripts
            
        except Exception as e:
            print(f"Error getting 2025 Q1 scripts: {str(e)}")
            return []
    
    def check_dialogue_coverage_for_scripts(self, scripts):
        """Check which scripts have dialogue data extracted"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            scripts_with_dialogue = []
            scripts_without_dialogue = []
            
            for script in scripts:
                # Check if dialogue data exists for this script
                cursor.execute("""
                    SELECT COUNT(*) FROM character_dialogue 
                    WHERE script_id = ?
                """, (script['id'],))
                
                dialogue_count = cursor.fetchone()[0]
                
                script_info = {
                    **script,
                    'dialogue_count': dialogue_count,
                    'has_url': bool(script['script_url'].strip()),
                    'url_accessible': None  # Will be checked later
                }
                
                if dialogue_count > 0:
                    scripts_with_dialogue.append(script_info)
                else:
                    scripts_without_dialogue.append(script_info)
            
            conn.close()
            return scripts_with_dialogue, scripts_without_dialogue
            
        except Exception as e:
            print(f"Error checking dialogue coverage: {str(e)}")
            return [], []
    
    def test_url_accessibility(self, script_url):
        """Test if a script URL is accessible"""
        if not script_url or not script_url.strip():
            return False, "No URL"
        
        try:
            # Extract spreadsheet ID and GID
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return False, "Invalid URL format"
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # Try to access the CSV export
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            response = requests.get(csv_url, timeout=10)
            
            if response.status_code == 200:
                return True, "Accessible"
            elif response.status_code == 401:
                return False, "Unauthorized"
            elif response.status_code == 404:
                return False, "Not Found"
            elif response.status_code == 410:
                return False, "Gone"
            else:
                return False, f"HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            return False, "Timeout"
        except requests.exceptions.RequestException as e:
            return False, f"Request Error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def analyze_coverage(self):
        """Main analysis function"""
        print("=== 2025年Q1台本データ網羅性分析 ===")
        print("作業進捗_newから追加された2025年1-4月のスクリプトの台本データ取得状況を確認")
        print()
        
        # Get 2025 Q1 scripts from database
        q1_scripts = self.get_2025_q1_scripts_from_db()
        print(f"2025年Q1スクリプト総数: {len(q1_scripts)}件")
        
        if not q1_scripts:
            print("2025年Q1のスクリプトが見つかりませんでした")
            return
        
        # Check dialogue coverage
        with_dialogue, without_dialogue = self.check_dialogue_coverage_for_scripts(q1_scripts)
        
        print(f"セリフデータあり: {len(with_dialogue)}件")
        print(f"セリフデータなし: {len(without_dialogue)}件")
        print(f"台本データ取得率: {len(with_dialogue)}/{len(q1_scripts)} = {len(with_dialogue)/len(q1_scripts)*100:.1f}%")
        print()
        
        # Monthly breakdown
        monthly_stats = {}
        for script in q1_scripts:
            month = script['broadcast_date'][3:5]  # Extract month
            if month not in monthly_stats:
                monthly_stats[month] = {'total': 0, 'with_dialogue': 0, 'with_url': 0}
            
            monthly_stats[month]['total'] += 1
            if script['id'] in [s['id'] for s in with_dialogue]:
                monthly_stats[month]['with_dialogue'] += 1
            if script['script_url'].strip():
                monthly_stats[month]['with_url'] += 1
        
        print("月別詳細:")
        for month in sorted(monthly_stats.keys()):
            stats = monthly_stats[month]
            print(f"  {month}月: {stats['total']}件中 {stats['with_dialogue']}件でセリフデータ取得済み ({stats['with_dialogue']/stats['total']*100:.1f}%) [URL有り: {stats['with_url']}件]")
        
        print()
        
        # Analyze scripts without dialogue
        if without_dialogue:
            print(f"=== セリフデータ未取得スクリプト詳細 ({len(without_dialogue)}件) ===")
            
            # Test URL accessibility for scripts without dialogue
            print("URL アクセシビリティテスト:")
            
            url_stats = {'no_url': 0, 'accessible': 0, 'inaccessible': 0}
            
            for i, script in enumerate(without_dialogue[:10]):  # Test first 10
                if not script['script_url'].strip():
                    url_stats['no_url'] += 1
                    status = "No URL"
                else:
                    is_accessible, reason = self.test_url_accessibility(script['script_url'])
                    if is_accessible:
                        url_stats['accessible'] += 1
                        status = "✓ Accessible"
                    else:
                        url_stats['inaccessible'] += 1
                        status = f"✗ {reason}"
                
                print(f"  {script['management_id']} ({script['broadcast_date']}): {script['title'][:30]}... - {status}")
                
                # Add small delay to avoid rate limiting
                if i < 9:
                    import time
                    time.sleep(0.5)
            
            if len(without_dialogue) > 10:
                print(f"  ... and {len(without_dialogue) - 10} more scripts")
            
            print()
            print("URL統計 (サンプル10件):")
            print(f"  URLなし: {url_stats['no_url']}件")
            print(f"  アクセス可能: {url_stats['accessible']}件") 
            print(f"  アクセス不可: {url_stats['inaccessible']}件")
        
        # Show successful extractions
        if with_dialogue:
            print(f"\n=== セリフデータ取得済みスクリプト ({len(with_dialogue)}件) ===")
            for script in with_dialogue[:5]:  # Show first 5
                print(f"  {script['management_id']} ({script['broadcast_date']}): {script['title'][:40]}... - {script['dialogue_count']}行のセリフ")
            
            if len(with_dialogue) > 5:
                print(f"  ... and {len(with_dialogue) - 5} more scripts with dialogue data")

def main():
    """Main execution function"""
    
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    analyzer = ScriptURLCoverageAnalyzer(db_path)
    analyzer.analyze_coverage()

if __name__ == "__main__":
    main()