#!/usr/bin/env python3
"""
gid検出結果をカウント
"""

# smart_gid_detectorの出力結果を直接カウント
def count_results():
    # 実際の出力から抽出（最後の出力を直接確認）
    import subprocess
    import sys
    
    print("📊 gid検出結果サマリー:")
    
    # プロセス確認
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        if 'smart_gid_detector' in result.stdout:
            print("⚠️  smart_gid_detectorプロセスがまだ実行中です")
        else:
            print("✅ smart_gid_detectorプロセスは完了しました")
    except Exception:
        pass
    
    # データベース確認
    import sqlite3
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 現在のgid分布を確認
    cursor.execute("""
        SELECT 
            CASE 
                WHEN script_url LIKE '%gid=1384097767%' THEN 'gid=1384097767'
                WHEN script_url LIKE '%gid=1115519680%' THEN 'gid=1115519680'
                ELSE 'その他'
            END as gid_type,
            COUNT(*) as count
        FROM scripts 
        WHERE script_url LIKE '%gid=%'
        GROUP BY gid_type
        ORDER BY count DESC
    """)
    
    results = cursor.fetchall()
    
    print("\n📋 データベース内のgid分布:")
    total_with_gid = 0
    for gid_type, count in results:
        print(f"  {gid_type}: {count}件")
        total_with_gid += count
    
    # 全体統計
    cursor.execute("SELECT COUNT(*) FROM scripts")
    total_scripts = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM scripts WHERE script_url NOT LIKE '%gid=%'")
    no_gid = cursor.fetchone()[0]
    
    print(f"\n📊 全体統計:")
    print(f"  総スクリプト数: {total_scripts}件")
    print(f"  gid付きURL: {total_with_gid}件")
    print(f"  gidなしURL: {no_gid}件")
    
    # 成功率計算（推定）
    success_rate = (total_with_gid / total_scripts * 100) if total_scripts > 0 else 0
    print(f"  成功率: {success_rate:.1f}%")
    
    conn.close()
    
    return total_with_gid, no_gid, total_scripts

if __name__ == "__main__":
    count_results()