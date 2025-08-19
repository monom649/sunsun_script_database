#!/usr/bin/env python3
"""
Script URL Coverage Audit

This script audits which scripts from the spreadsheet have their script URLs
properly processed and which are missing actual script content extraction.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os
import time

class ScriptURLCoverageAudit:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.gid = "1504431244"
        
    def fetch_all_spreadsheet_scripts(self):
        """Fetch all scripts from spreadsheet with their URLs"""
        try:
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.gid}"
            
            print("作業進捗_newから全スクリプトを取得中...")
            response = requests.get(csv_url)
            response.raise_for_status()
            
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            scripts = []
            for index, row in df.iterrows():
                if index < 4:  # Skip header rows
                    continue
                    
                broadcast_date = row.get('Unnamed: 2')  # 配信日
                management_id = row.get('Unnamed: 3')   # 管理番号
                title = row.get('台本テンプレ')           # 動画タイトル
                script_url = row.get('https://docs.google.com/spreadsheets/d/1uH7Y0hYMnLoLMhew4jYPnm0vkAVXRkQXoYOzTftg2Q8/edit?gid=1007786454#gid=1007786454')
                
                if pd.isna(management_id) or pd.isna(title) or title == '未定':
                    continue
                    
                if isinstance(management_id, str) and management_id.startswith('B'):
                    scripts.append({
                        'management_id': management_id.strip(),
                        'title': title.strip() if isinstance(title, str) else '',
                        'broadcast_date': broadcast_date.strip() if isinstance(broadcast_date, str) and not pd.isna(broadcast_date) else '',
                        'script_url': script_url.strip() if isinstance(script_url, str) and not pd.isna(script_url) else '',
                        'has_script_url': bool(script_url and isinstance(script_url, str) and script_url.strip() and 'docs.google.com' in script_url)
                    })
            
            print(f"スプレッドシートから{len(scripts)}件のスクリプトを取得")
            return scripts
            
        except Exception as e:
            print(f"❌ スプレッドシート取得エラー: {str(e)}")
            return []
    
    def analyze_database_content(self):
        """Analyze what's actually in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\n=== データベース内容分析 ===")
            
            # Basic stats
            cursor.execute("SELECT COUNT(*) FROM scripts")
            total_scripts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total_dialogue = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT script_id) FROM character_dialogue")
            scripts_with_dialogue = cursor.fetchone()[0]
            
            print(f"データベース総スクリプト数: {total_scripts}")
            print(f"総セリフ行数: {total_dialogue:,}")
            print(f"セリフを持つスクリプト数: {scripts_with_dialogue}")
            
            # Check what management IDs have dialogue
            cursor.execute("""
                SELECT s.management_id, s.title, s.broadcast_date, s.script_url, COUNT(cd.id) as dialogue_count
                FROM scripts s
                LEFT JOIN character_dialogue cd ON s.id = cd.script_id
                GROUP BY s.id, s.management_id, s.title, s.broadcast_date, s.script_url
                HAVING dialogue_count > 0
                ORDER BY s.broadcast_date DESC
                LIMIT 10
            """)
            
            print(f"\nセリフデータがあるスクリプトの例:")
            for mgmt_id, title, date, url, count in cursor.fetchall():
                has_url = "URL有" if url and url.strip() else "URL無"
                print(f"  {mgmt_id} ({date}): {title[:40]}... - {count:,}行セリフ [{has_url}]")
            
            # Check scripts without dialogue but with URLs
            cursor.execute("""
                SELECT s.management_id, s.title, s.broadcast_date, s.script_url
                FROM scripts s
                LEFT JOIN character_dialogue cd ON s.id = cd.script_id
                WHERE s.script_url IS NOT NULL 
                  AND s.script_url != ''
                  AND cd.script_id IS NULL
                ORDER BY s.broadcast_date DESC
                LIMIT 20
            """)
            
            scripts_without_dialogue = cursor.fetchall()
            print(f"\nURLはあるがセリフデータがないスクリプト数: {len(scripts_without_dialogue)}")
            if scripts_without_dialogue:
                print("例:")
                for mgmt_id, title, date, url in scripts_without_dialogue[:5]:
                    print(f"  {mgmt_id} ({date}): {title[:40]}... [URL: {url[:50]}...]")
            
            conn.close()
            return scripts_without_dialogue
            
        except Exception as e:
            print(f"❌ データベース分析エラー: {str(e)}")
            return []
    
    def cross_reference_analysis(self):
        """Cross-reference spreadsheet scripts with database content"""
        print("\n=== スプレッドシートとデータベースの照合 ===")
        
        # Get all scripts from spreadsheet
        spreadsheet_scripts = self.fetch_all_spreadsheet_scripts()
        if not spreadsheet_scripts:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all scripts from database
            cursor.execute("SELECT management_id, script_url FROM scripts")
            db_scripts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get all scripts with dialogue from database
            cursor.execute("""
                SELECT DISTINCT s.management_id
                FROM scripts s
                JOIN character_dialogue cd ON s.id = cd.script_id
            """)
            scripts_with_dialogue = set(row[0] for row in cursor.fetchall())
            
            conn.close()
            
            # Analysis
            categories = {
                'in_spreadsheet_with_url': [],
                'in_spreadsheet_no_url': [],
                'has_dialogue_data': [],
                'missing_dialogue_with_url': [],
                'not_in_database': []
            }
            
            for script in spreadsheet_scripts:
                mgmt_id = script['management_id']
                
                if script['has_script_url']:
                    categories['in_spreadsheet_with_url'].append(script)
                else:
                    categories['in_spreadsheet_no_url'].append(script)
                
                if mgmt_id in db_scripts:
                    if mgmt_id in scripts_with_dialogue:
                        categories['has_dialogue_data'].append(script)
                    elif script['has_script_url']:
                        categories['missing_dialogue_with_url'].append(script)
                else:
                    categories['not_in_database'].append(script)
            
            # Report results
            print(f"スプレッドシート総スクリプト数: {len(spreadsheet_scripts)}")
            print(f"  台本URL有り: {len(categories['in_spreadsheet_with_url'])}")
            print(f"  台本URL無し: {len(categories['in_spreadsheet_no_url'])}")
            print(f"セリフデータ取得済み: {len(categories['has_dialogue_data'])}")
            print(f"URLはあるがセリフ未取得: {len(categories['missing_dialogue_with_url'])}")
            print(f"データベース未登録: {len(categories['not_in_database'])}")
            
            # Show missing dialogue scripts by year
            if categories['missing_dialogue_with_url']:
                print(f"\n台本URLがあるのにセリフ未取得のスクリプト:")
                
                # Group by year
                year_groups = {}
                for script in categories['missing_dialogue_with_url']:
                    date = script['broadcast_date']
                    if date and len(date) >= 8:
                        year = '20' + date[:2]
                    else:
                        year = 'Unknown'
                    
                    if year not in year_groups:
                        year_groups[year] = []
                    year_groups[year].append(script)
                
                for year in sorted(year_groups.keys()):
                    scripts = year_groups[year]
                    print(f"\n{year}年 ({len(scripts)}件):")
                    for script in scripts[:5]:  # Show first 5
                        print(f"  {script['management_id']}: {script['title'][:40]}... ({script['broadcast_date']})")
                    if len(scripts) > 5:
                        print(f"  ... and {len(scripts) - 5} more")
            
            return categories
            
        except Exception as e:
            print(f"❌ 照合分析エラー: {str(e)}")
            return {}
    
    def test_sample_urls(self, scripts_with_urls, sample_size=10):
        """Test a sample of script URLs to see if they're accessible"""
        if not scripts_with_urls:
            return
        
        print(f"\n=== サンプルURL アクセシビリティテスト ({sample_size}件) ===")
        
        accessible = []
        inaccessible = []
        
        for i, script in enumerate(scripts_with_urls[:sample_size]):
            try:
                url = script['script_url']
                
                # Extract spreadsheet ID and GID
                sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
                gid_pattern = r'[#&]gid=([0-9]+)'
                
                sheet_match = re.search(sheet_pattern, url)
                gid_match = re.search(gid_pattern, url)
                
                if not sheet_match:
                    inaccessible.append((script, "Invalid URL format"))
                    print(f"  ❌ {script['management_id']}: Invalid URL format")
                    continue
                
                spreadsheet_id = sheet_match.group(1)
                gid = gid_match.group(1) if gid_match else '0'
                
                csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
                
                response = requests.get(csv_url, timeout=10)
                
                if response.status_code == 200:
                    accessible.append(script)
                    print(f"  ✅ {script['management_id']}: Accessible")
                else:
                    inaccessible.append((script, f"HTTP {response.status_code}"))
                    print(f"  ❌ {script['management_id']}: HTTP {response.status_code}")
                    
            except Exception as e:
                inaccessible.append((script, str(e)))
                print(f"  ❌ {script['management_id']}: {str(e)}")
            
            # Rate limiting
            if i < sample_size - 1:
                time.sleep(0.5)
        
        print(f"\nテスト結果:")
        print(f"  アクセス可能: {len(accessible)}/{sample_size}")
        print(f"  アクセス不可: {len(inaccessible)}/{sample_size}")
        
        return accessible, inaccessible
    
    def run_comprehensive_audit(self):
        """Run comprehensive audit of script URL coverage"""
        print("="*80)
        print("台本URL網羅性監査")
        print("="*80)
        
        # Analyze current database content
        self.analyze_database_content()
        
        # Cross-reference with spreadsheet
        categories = self.cross_reference_analysis()
        
        if categories and categories.get('missing_dialogue_with_url'):
            missing_scripts = categories['missing_dialogue_with_url']
            
            # Test sample URLs
            accessible, inaccessible = self.test_sample_urls(missing_scripts, 15)
            
            if accessible:
                print(f"\n🎯 発見: {len(accessible)}件のアクセス可能な台本URLからデータが未取得")
                print("これらのURLから台本データを取得する必要があります")
                
                return accessible
        
        return []

def main():
    """Main audit function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    auditor = ScriptURLCoverageAudit(db_path)
    recoverable_scripts = auditor.run_comprehensive_audit()
    
    if recoverable_scripts:
        print(f"\n📋 取得可能な台本スクリプト: {len(recoverable_scripts)}件")
        print("これらの台本URLから実際のセリフデータを抽出する処理が必要です。")
    else:
        print(f"\n✅ 全ての台本URLから取得済み、または取得不可能です。")

if __name__ == "__main__":
    main()