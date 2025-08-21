#!/usr/bin/env python3
"""
全てのスクリプトURLのgidを正しい"台本"タブのgidに統一
"""

import sqlite3
import re

def fix_all_gids():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    target_gid = "1384097767"  # 台本タブのgid
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 全スクリプトのURLを取得
    cursor.execute("SELECT id, management_id, script_url FROM scripts")
    scripts = cursor.fetchall()
    
    print(f"📊 対象スクリプト: {len(scripts)}件")
    
    updated_count = 0
    
    for script_id, management_id, script_url in scripts:
        # gidパターンを検索して置換
        new_url = re.sub(r'#gid=\d+', f'#gid={target_gid}', script_url)
        
        # URLが変更された場合のみ更新
        if new_url != script_url:
            cursor.execute("UPDATE scripts SET script_url = ? WHERE id = ?", (new_url, script_id))
            updated_count += 1
            if updated_count <= 10:  # 最初の10件のみ表示
                print(f"✅ {management_id}: gid更新")
    
    conn.commit()
    conn.close()
    
    print(f"🎯 更新完了: {updated_count}件のURLを修正")
    
    # 検証
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM scripts WHERE script_url LIKE ?", (f'%#gid={target_gid}%',))
    correct_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM scripts")
    total_count = cursor.fetchone()[0]
    
    print(f"📈 検証結果: {correct_count}/{total_count} 件が正しいgidになりました")
    
    conn.close()

if __name__ == "__main__":
    fix_all_gids()