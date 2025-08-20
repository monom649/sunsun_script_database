#!/usr/bin/env python3
"""
Comprehensive Script Audit

This script performs a thorough audit of ALL scripts from the main spreadsheet
to identify missing dialogue data and verify complete coverage.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os
import time

class ComprehensiveScriptAudit:
    def __init__(self, db_path):
        self.db_path = db_path
        self.spreadsheet_id = "1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8"
        self.gid = "1504431244"
        self.audit_log = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/audit_log.txt"
        
    def log_audit(self, message):
        """Log audit results"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        with open(self.audit_log, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
        
        print(log_entry)
    
    def fetch_all_spreadsheet_scripts(self):
        """Fetch ALL scripts from the main spreadsheet"""
        try:
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.gid}"
            
            self.log_audit("Fetching ALL scripts from 作業進捗_new...")
            response = requests.get(csv_url)
            response.raise_for_status()
            
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Extract all scripts with proper structure detection
            scripts = []
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
                    scripts.append({
                        'management_id': management_id.strip(),
                        'title': title.strip() if isinstance(title, str) else '',
                        'broadcast_date': broadcast_date.strip() if isinstance(broadcast_date, str) and not pd.isna(broadcast_date) else '',
                        'script_url': script_url.strip() if isinstance(script_url, str) and not pd.isna(script_url) else '',
                        'row_index': index
                    })
            
            self.log_audit(f"Extracted {len(scripts)} scripts from spreadsheet")
            return scripts
            
        except Exception as e:
            self.log_audit(f"❌ Error fetching spreadsheet: {str(e)}")
            return []
    
    def get_database_script_status(self, scripts):
        """Get status of each script in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            script_status = []
            
            for script in scripts:
                mgmt_id = script['management_id']
                
                # Check if script exists in database
                cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (mgmt_id,))
                db_script = cursor.fetchone()
                
                if db_script:
                    script_id = db_script[0]
                    
                    # Check if dialogue data exists
                    cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE script_id = ?", (script_id,))
                    dialogue_count = cursor.fetchone()[0]
                    
                    status = {
                        **script,
                        'in_database': True,
                        'script_id': script_id,
                        'dialogue_count': dialogue_count,
                        'has_dialogue': dialogue_count > 0,
                        'has_url': bool(script['script_url'].strip())
                    }
                else:
                    status = {
                        **script,
                        'in_database': False,
                        'script_id': None,
                        'dialogue_count': 0,
                        'has_dialogue': False,
                        'has_url': bool(script['script_url'].strip())
                    }
                
                script_status.append(status)
            
            conn.close()
            return script_status
            
        except Exception as e:
            self.log_audit(f"❌ Error checking database status: {str(e)}")
            return []
    
    def categorize_scripts(self, script_status):
        """Categorize scripts by status"""
        categories = {
            'complete': [],      # In DB + has dialogue
            'missing_dialogue': [], # In DB + has URL + no dialogue  
            'missing_from_db': [],  # Not in DB
            'no_url': [],          # In DB + no URL
            'url_inaccessible': [] # In DB + has URL + no dialogue (URL may be inaccessible)
        }
        
        for script in script_status:
            if not script['in_database']:
                categories['missing_from_db'].append(script)
            elif script['has_dialogue']:
                categories['complete'].append(script)
            elif not script['has_url']:
                categories['no_url'].append(script)
            else:
                # Has URL but no dialogue - need to verify if URL is accessible
                categories['missing_dialogue'].append(script)
        
        return categories
    
    def test_url_batch(self, scripts_with_missing_dialogue, sample_size=10):
        """Test accessibility of URLs for scripts missing dialogue"""
        self.log_audit(f"Testing URL accessibility for {min(sample_size, len(scripts_with_missing_dialogue))} scripts...")
        
        accessible_urls = []
        inaccessible_urls = []
        
        for i, script in enumerate(scripts_with_missing_dialogue[:sample_size]):
            try:
                # Extract spreadsheet ID and GID
                url = script['script_url']
                sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
                gid_pattern = r'[#&]gid=([0-9]+)'
                
                sheet_match = re.search(sheet_pattern, url)
                gid_match = re.search(gid_pattern, url)
                
                if not sheet_match:
                    inaccessible_urls.append((script, "Invalid URL format"))
                    continue
                
                spreadsheet_id = sheet_match.group(1)
                gid = gid_match.group(1) if gid_match else '0'
                
                csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
                
                response = requests.get(csv_url, timeout=10)
                
                if response.status_code == 200:
                    accessible_urls.append(script)
                    self.log_audit(f"   ✅ {script['management_id']}: Accessible")
                else:
                    inaccessible_urls.append((script, f"HTTP {response.status_code}"))
                    self.log_audit(f"   ❌ {script['management_id']}: HTTP {response.status_code}")
                    
            except Exception as e:
                inaccessible_urls.append((script, str(e)))
                self.log_audit(f"   ❌ {script['management_id']}: {str(e)}")
            
            # Rate limiting
            if i < sample_size - 1:
                time.sleep(0.5)
        
        return accessible_urls, inaccessible_urls
    
    def run_comprehensive_audit(self):
        """Run complete audit of all scripts"""
        self.log_audit("="*80)
        self.log_audit("STARTING COMPREHENSIVE SCRIPT AUDIT")
        self.log_audit("="*80)
        
        # 1. Fetch all scripts from spreadsheet
        all_scripts = self.fetch_all_spreadsheet_scripts()
        if not all_scripts:
            self.log_audit("❌ CRITICAL: Failed to fetch scripts from spreadsheet")
            return
        
        # 2. Check database status for each script
        self.log_audit("Checking database status for all scripts...")
        script_status = self.get_database_script_status(all_scripts)
        
        # 3. Categorize scripts
        categories = self.categorize_scripts(script_status)
        
        # 4. Report findings
        self.log_audit("\n" + "="*80)
        self.log_audit("AUDIT RESULTS SUMMARY")
        self.log_audit("="*80)
        
        total_scripts = len(script_status)
        complete = len(categories['complete'])
        missing_dialogue = len(categories['missing_dialogue'])
        missing_from_db = len(categories['missing_from_db'])
        no_url = len(categories['no_url'])
        
        self.log_audit(f"📊 OVERALL STATISTICS:")
        self.log_audit(f"   Total scripts in spreadsheet: {total_scripts}")
        self.log_audit(f"   Complete (DB + dialogue): {complete} ({complete/total_scripts*100:.1f}%)")
        self.log_audit(f"   Missing dialogue: {missing_dialogue} ({missing_dialogue/total_scripts*100:.1f}%)")
        self.log_audit(f"   Missing from database: {missing_from_db} ({missing_from_db/total_scripts*100:.1f}%)")
        self.log_audit(f"   No URL available: {no_url} ({no_url/total_scripts*100:.1f}%)")
        
        # 5. Detailed analysis of missing scripts
        if missing_from_db:
            self.log_audit(f"\n❌ MISSING FROM DATABASE ({len(missing_from_db)} scripts):")
            for script in missing_from_db[:10]:
                self.log_audit(f"   {script['management_id']}: {script['title'][:40]}... ({script['broadcast_date']})")
            if len(missing_from_db) > 10:
                self.log_audit(f"   ... and {len(missing_from_db) - 10} more")
        
        # 6. Detailed analysis of scripts with missing dialogue
        missing_dialogue_list = categories['missing_dialogue']
        if missing_dialogue_list:
            self.log_audit(f"\n⚠️ SCRIPTS WITH MISSING DIALOGUE ({len(missing_dialogue_list)} scripts):")
            
            # Test URL accessibility for a sample
            accessible, inaccessible = self.test_url_batch(missing_dialogue_list, 20)
            
            self.log_audit(f"\nURL ACCESSIBILITY TEST RESULTS (sample of {len(accessible + inaccessible)}):")
            self.log_audit(f"   Accessible URLs: {len(accessible)}")
            self.log_audit(f"   Inaccessible URLs: {len(inaccessible)}")
            
            if accessible:
                self.log_audit(f"\n🎯 RECOVERABLE SCRIPTS (accessible URLs):")
                for script in accessible[:5]:
                    self.log_audit(f"   {script['management_id']}: {script['title'][:40]}... ({script['broadcast_date']})")
                if len(accessible) > 5:
                    self.log_audit(f"   ... and {len(accessible) - 5} more accessible scripts")
        
        # 7. Year-based analysis
        self.log_audit(f"\n📅 ANALYSIS BY YEAR:")
        year_stats = {}
        for script in script_status:
            date = script['broadcast_date']
            if date and len(date) >= 8:
                year = '20' + date[:2]
            else:
                year = 'Unknown'
            
            if year not in year_stats:
                year_stats[year] = {'total': 0, 'complete': 0, 'missing_dialogue': 0}
            
            year_stats[year]['total'] += 1
            if script in categories['complete']:
                year_stats[year]['complete'] += 1
            elif script in missing_dialogue_list:
                year_stats[year]['missing_dialogue'] += 1
        
        for year in sorted(year_stats.keys()):
            stats = year_stats[year]
            completion_rate = stats['complete'] / stats['total'] * 100 if stats['total'] > 0 else 0
            self.log_audit(f"   {year}: {stats['complete']}/{stats['total']} complete ({completion_rate:.1f}%) - {stats['missing_dialogue']} missing dialogue")
        
        # 8. Final assessment
        self.log_audit("\n" + "="*80)
        overall_completion = (complete / total_scripts * 100) if total_scripts > 0 else 0
        
        if overall_completion >= 90:
            self.log_audit(f"✅ AUDIT RESULT: GOOD - {overall_completion:.1f}% completion rate")
        elif overall_completion >= 70:
            self.log_audit(f"⚠️ AUDIT RESULT: NEEDS IMPROVEMENT - {overall_completion:.1f}% completion rate")
        else:
            self.log_audit(f"❌ AUDIT RESULT: CRITICAL - {overall_completion:.1f}% completion rate")
        
        if accessible:
            self.log_audit(f"🎯 ACTION REQUIRED: {len(accessible)} scripts with accessible URLs need dialogue extraction")
        
        self.log_audit("="*80)
        
        return categories, accessible

def main():
    """Main audit function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    auditor = ComprehensiveScriptAudit(db_path)
    categories, recoverable = auditor.run_comprehensive_audit()
    
    if recoverable:
        print(f"\n🚨 FOUND {len(recoverable)} RECOVERABLE SCRIPTS WITH MISSING DIALOGUE DATA!")
        print("These scripts have accessible URLs but no dialogue data in the database.")

if __name__ == "__main__":
    main()