#!/usr/bin/env python3
"""
データベース内で直接文字化けキャラクター名を修正
"""

import sqlite3

def direct_character_fix():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 データベース内で直接文字化けキャラクター名修正")
    print("=" * 80)
    
    # 文字化け修正マッピング（実際のデータベースの値）
    char_mapping = {
        'ãµã³ãµã³': 'サンサン',
        'ãããã': 'くもりん',
        'ããªã«': 'プリル',
        'ãã¤ãº': 'ノイズ',
        'ãã¼ã ãããã': 'チームくもりん',
        'ã­ãå©¦äºº': 'ロボ婦人',
        'ã±ãã': 'ゲット',
        'ã´ãã': 'ゴリ',
        'ã·ã¼ã³': 'シーン',
        'ã¿ã¼å­': 'ハンター子',
        'ãã¯ã¢': 'ナクア',
        'ãã³å¤«äºº': 'パン夫人',
        'ãã¦ã¹ãã¼ã·ã§ã³': 'ナビゲーション',
        'ç¬ã¬ã³ãã³': '犬ガンマン',
        'èµ¤ã¡ãã': '赤ちゃん'
    }
    
    total_fixed = 0
    
    # 各文字化けパターンを修正
    for garbled, correct in char_mapping.items():
        print(f"\n🔤 '{garbled}' → '{correct}' の修正")
        print("-" * 50)
        
        # 修正前の件数確認
        cursor.execute("""
            SELECT COUNT(*) FROM character_dialogue_unified 
            WHERE character_name = ?
        """, (garbled,))
        
        before_count = cursor.fetchone()[0]
        print(f"📊 修正前: {before_count}件")
        
        if before_count > 0:
            # 文字化けキャラクター名を正しい名前に更新
            cursor.execute("""
                UPDATE character_dialogue_unified 
                SET character_name = ?
                WHERE character_name = ?
            """, (correct, garbled))
            
            fixed_count = cursor.rowcount
            total_fixed += fixed_count
            print(f"✅ 修正完了: {fixed_count}件")
            
            # サンプル表示
            cursor.execute("""
                SELECT s.management_id, cdu.dialogue_text, cdu.row_number
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE cdu.character_name = ?
                LIMIT 3
            """, (correct,))
            
            samples = cursor.fetchall()
            for management_id, dialogue, row in samples:
                row_str = f"{row:3d}" if row is not None else "---"
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    例: {management_id} 行{row_str} | {correct} | \"{dialogue_short}\"")
        else:
            print(f"ℹ️  該当データなし")
    
    # TRUEキャラクターの残存チェック
    print(f"\n📋 TRUE キャラクターの残存チェック")
    print("-" * 50)
    
    cursor.execute("""
        SELECT COUNT(*) FROM character_dialogue_unified 
        WHERE character_name = 'TRUE' AND is_instruction = 0
    """)
    
    true_remaining = cursor.fetchone()[0]
    print(f"📊 残存TRUE: {true_remaining}件")
    
    if true_remaining > 0:
        # TRUEキャラクターのサンプル表示
        cursor.execute("""
            SELECT s.management_id, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.character_name = 'TRUE' AND cdu.is_instruction = 0
            LIMIT 5
        """)
        
        true_samples = cursor.fetchall()
        print("🔍 TRUEキャラクターのサンプル:")
        for management_id, dialogue, row in true_samples:
            row_str = f"{row:3d}" if row is not None else "---"
            dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
            print(f"    {management_id} 行{row_str} | TRUE | \"{dialogue_short}\"")
    
    # 修正後の統計
    print(f"\n📊 修正後キャラクター統計（上位10位）:")
    print("-" * 50)
    
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 10
    """)
    
    top_chars = cursor.fetchall()
    for i, (char_name, count) in enumerate(top_chars, 1):
        char_short = char_name[:25] + "..." if len(char_name) > 25 else char_name
        print(f"  {i:2d}. {char_short:30s}: {count:6,}件")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 文字化け修正完了: {total_fixed}件")
    return total_fixed

if __name__ == "__main__":
    fixed_count = direct_character_fix()
    print(f"\n🎯 総修正件数: {fixed_count}件")