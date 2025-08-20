#!/usr/bin/env python3
"""
Backup Data Restore

This script restores the rich dialogue data from the backup database
to fix the massive data loss in the current database.
"""

import sqlite3
import os
from datetime import datetime

def restore_from_backup():
    """Restore data from backup to current database"""
    
    backup_path = "/Users/mitsuruono/sunsun_script_search/new/backups/youtube_search_backup_20250818_1725.db"
    current_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    print("=== BACKUP DATA RESTORATION ===")
    print(f"Backup source: {backup_path}")
    print(f"Current target: {current_path}")
    
    # Verify files exist
    if not os.path.exists(backup_path):
        print(f"❌ Backup file not found: {backup_path}")
        return False
    
    if not os.path.exists(current_path):
        print(f"❌ Current database not found: {current_path}")
        return False
    
    try:
        # Connect to both databases
        conn_backup = sqlite3.connect(backup_path)
        conn_current = sqlite3.connect(current_path)
        
        cursor_backup = conn_backup.cursor()
        cursor_current = conn_current.cursor()
        
        # Clear current character_dialogue table
        print("Clearing current character_dialogue table...")
        cursor_current.execute("DELETE FROM character_dialogue")
        
        # Get all unique management_ids from current scripts table
        cursor_current.execute("SELECT id, management_id FROM scripts")
        script_mapping = {row[1]: row[0] for row in cursor_current.fetchall()}
        
        print(f"Found {len(script_mapping)} scripts in current database")
        
        # Restore data from backup script_lines
        print("Restoring dialogue data from backup...")
        cursor_backup.execute("""
            SELECT management_id, row_number, character_name, dialogue, 
                   voice_instruction, filming_instruction, editing_instruction
            FROM script_lines
            WHERE dialogue IS NOT NULL AND dialogue != ''
        """)
        
        restored_count = 0
        skipped_count = 0
        
        for row in cursor_backup.fetchall():
            mgmt_id, row_num, char_name, dialogue, voice, filming, editing = row
            
            # Find corresponding script_id in current database
            script_id = script_mapping.get(mgmt_id)
            if script_id:
                # Insert into character_dialogue
                cursor_current.execute("""
                    INSERT INTO character_dialogue 
                    (script_id, row_number, character_name, dialogue_text, voice_instruction)
                    VALUES (?, ?, ?, ?, ?)
                """, (script_id, row_num, char_name, dialogue, voice))
                restored_count += 1
            else:
                skipped_count += 1
            
            if restored_count % 10000 == 0:
                print(f"  Restored: {restored_count:,} entries...")
        
        conn_current.commit()
        
        print(f"✅ Restoration complete!")
        print(f"  Restored entries: {restored_count:,}")
        print(f"  Skipped entries: {skipped_count:,}")
        
        # Verify restoration
        cursor_current.execute("SELECT COUNT(*) FROM character_dialogue")
        final_count = cursor_current.fetchone()[0]
        
        print(f"  Final dialogue count: {final_count:,}")
        
        conn_backup.close()
        conn_current.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error during restoration: {str(e)}")
        return False

if __name__ == "__main__":
    success = restore_from_backup()
    if success:
        print("\n🎉 Data restoration completed successfully!")
    else:
        print("\n🚨 Data restoration failed!")