#!/usr/bin/env python3
"""
Verification Monitor

This script provides strict verification and monitoring of database operations
to ensure accurate reporting and prevent false claims.
"""

import sqlite3
import os
import hashlib
from datetime import datetime

class VerificationMonitor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/verification_log.txt"
        
    def log_verification(self, message):
        """Log verification results with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def get_database_hash(self):
        """Get database file hash for integrity checking"""
        try:
            with open(self.db_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            return f"Error: {str(e)}"
    
    def verify_database_exists(self):
        """Verify database file exists and is accessible"""
        if not os.path.exists(self.db_path):
            self.log_verification(f"❌ CRITICAL: Database file does not exist at {self.db_path}")
            return False
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.close()
            self.log_verification(f"✅ Database file exists and is accessible")
            return True
        except Exception as e:
            self.log_verification(f"❌ CRITICAL: Database file exists but cannot be opened: {str(e)}")
            return False
    
    def verify_table_structure(self):
        """Verify all required tables exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check for required tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['scripts', 'character_dialogue']
            missing_tables = []
            
            for table in required_tables:
                if table not in tables:
                    missing_tables.append(table)
            
            if missing_tables:
                self.log_verification(f"❌ CRITICAL: Missing required tables: {missing_tables}")
                conn.close()
                return False
            
            self.log_verification(f"✅ All required tables exist: {required_tables}")
            conn.close()
            return True
            
        except Exception as e:
            self.log_verification(f"❌ CRITICAL: Error checking table structure: {str(e)}")
            return False
    
    def verify_2025_q1_data(self):
        """Strictly verify 2025 Q1 data exists"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Count 2025 Q1 scripts
            cursor.execute("""
                SELECT COUNT(*) FROM scripts 
                WHERE broadcast_date LIKE '25/01/%' 
                   OR broadcast_date LIKE '25/02/%'
                   OR broadcast_date LIKE '25/03/%' 
                   OR broadcast_date LIKE '25/04/%'
            """)
            q1_scripts_count = cursor.fetchone()[0]
            
            # Count 2025 Q1 scripts with dialogue
            cursor.execute("""
                SELECT COUNT(DISTINCT s.id) FROM scripts s
                JOIN character_dialogue cd ON s.id = cd.script_id
                WHERE s.broadcast_date LIKE '25/01/%' 
                   OR s.broadcast_date LIKE '25/02/%'
                   OR s.broadcast_date LIKE '25/03/%' 
                   OR s.broadcast_date LIKE '25/04/%'
            """)
            q1_with_dialogue = cursor.fetchone()[0]
            
            # Count total dialogue lines from 2025 Q1
            cursor.execute("""
                SELECT COUNT(*) FROM character_dialogue cd
                JOIN scripts s ON cd.script_id = s.id
                WHERE s.broadcast_date LIKE '25/01/%' 
                   OR s.broadcast_date LIKE '25/02/%'
                   OR s.broadcast_date LIKE '25/03/%' 
                   OR s.broadcast_date LIKE '25/04/%'
            """)
            q1_dialogue_lines = cursor.fetchone()[0]
            
            coverage_rate = (q1_with_dialogue / q1_scripts_count * 100) if q1_scripts_count > 0 else 0
            
            self.log_verification(f"📊 2025 Q1 DATA VERIFICATION:")
            self.log_verification(f"   Total Q1 scripts: {q1_scripts_count}")
            self.log_verification(f"   Scripts with dialogue: {q1_with_dialogue}")
            self.log_verification(f"   Total dialogue lines: {q1_dialogue_lines}")
            self.log_verification(f"   Coverage rate: {coverage_rate:.1f}%")
            
            # Verify specific claims
            if q1_scripts_count != 46:
                self.log_verification(f"⚠️  WARNING: Expected 46 Q1 scripts, found {q1_scripts_count}")
            
            if coverage_rate < 90:
                self.log_verification(f"❌ FAIL: Coverage rate {coverage_rate:.1f}% is below expected 90%+")
                return False
            else:
                self.log_verification(f"✅ PASS: Coverage rate {coverage_rate:.1f}% meets expectations")
            
            conn.close()
            return True
            
        except Exception as e:
            self.log_verification(f"❌ CRITICAL: Error verifying 2025 Q1 data: {str(e)}")
            return False
    
    def verify_total_counts(self):
        """Verify total database counts"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total scripts
            cursor.execute("SELECT COUNT(*) FROM scripts")
            total_scripts = cursor.fetchone()[0]
            
            # Total dialogue lines
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total_dialogue = cursor.fetchone()[0]
            
            # Scripts with dialogue
            cursor.execute("SELECT COUNT(DISTINCT script_id) FROM character_dialogue")
            scripts_with_dialogue = cursor.fetchone()[0]
            
            # Main characters
            cursor.execute("""
                SELECT character_name, COUNT(*) FROM character_dialogue
                WHERE character_name IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ')
                GROUP BY character_name
                ORDER BY COUNT(*) DESC
            """)
            main_chars = cursor.fetchall()
            
            self.log_verification(f"📊 TOTAL DATABASE COUNTS:")
            self.log_verification(f"   Total scripts: {total_scripts}")
            self.log_verification(f"   Total dialogue lines: {total_dialogue}")
            self.log_verification(f"   Scripts with dialogue: {scripts_with_dialogue}")
            
            self.log_verification(f"📊 MAIN CHARACTER DISTRIBUTION:")
            total_main_char_lines = 0
            for char, count in main_chars:
                self.log_verification(f"   {char}: {count} lines")
                total_main_char_lines += count
            
            main_char_percentage = (total_main_char_lines / total_dialogue * 100) if total_dialogue > 0 else 0
            self.log_verification(f"   Main characters total: {total_main_char_lines} ({main_char_percentage:.1f}%)")
            
            conn.close()
            return total_scripts, total_dialogue, scripts_with_dialogue
            
        except Exception as e:
            self.log_verification(f"❌ CRITICAL: Error verifying total counts: {str(e)}")
            return 0, 0, 0
    
    def verify_file_integrity(self):
        """Verify both database files are identical"""
        try:
            current_hash = self.get_database_hash()
            new_db_path = "/Users/mitsuruono/sunsun_script_search/new/youtube_search_complete_all.db"
            
            if os.path.exists(new_db_path):
                with open(new_db_path, 'rb') as f:
                    new_hash = hashlib.md5(f.read()).hexdigest()
                
                if current_hash == new_hash:
                    self.log_verification(f"✅ Both database files are identical (hash: {current_hash[:8]}...)")
                    return True
                else:
                    self.log_verification(f"❌ CRITICAL: Database files differ!")
                    self.log_verification(f"   Current: {current_hash[:8]}...")
                    self.log_verification(f"   New dir: {new_hash[:8]}...")
                    return False
            else:
                self.log_verification(f"❌ WARNING: New directory database does not exist")
                return False
                
        except Exception as e:
            self.log_verification(f"❌ CRITICAL: Error verifying file integrity: {str(e)}")
            return False
    
    def run_full_verification(self):
        """Run complete verification suite"""
        self.log_verification("=" * 80)
        self.log_verification("STARTING COMPREHENSIVE DATABASE VERIFICATION")
        self.log_verification("=" * 80)
        
        all_passed = True
        
        # 1. Database exists
        if not self.verify_database_exists():
            all_passed = False
            return False
        
        # 2. Table structure
        if not self.verify_table_structure():
            all_passed = False
            return False
        
        # 3. 2025 Q1 data verification
        if not self.verify_2025_q1_data():
            all_passed = False
        
        # 4. Total counts
        total_scripts, total_dialogue, scripts_with_dialogue = self.verify_total_counts()
        if total_scripts == 0 or total_dialogue == 0:
            all_passed = False
        
        # 5. File integrity
        if not self.verify_file_integrity():
            all_passed = False
        
        # Final assessment
        self.log_verification("=" * 80)
        if all_passed:
            self.log_verification("✅ VERIFICATION PASSED: All checks successful")
        else:
            self.log_verification("❌ VERIFICATION FAILED: One or more checks failed")
        
        self.log_verification(f"Database hash: {self.get_database_hash()}")
        self.log_verification("=" * 80)
        
        return all_passed

def main():
    """Main verification function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    monitor = VerificationMonitor(db_path)
    success = monitor.run_full_verification()
    
    if success:
        print("\n🎉 ALL VERIFICATIONS PASSED - Data integrity confirmed")
    else:
        print("\n🚨 VERIFICATION FAILURES DETECTED - Investigation required")

if __name__ == "__main__":
    main()