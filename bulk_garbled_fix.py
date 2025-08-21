#!/usr/bin/env python3
"""
データベース内の文字化けキャラクター名一括修正
"""

import sqlite3

def bulk_garbled_fix():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 文字化けキャラクター名一括修正")
    print("=" * 80)
    
    # 実際のデータベースから取得した文字化けパターン
    char_mapping = {
        'ãããã': 'くもりん',
        'ãµã³ãµã³': 'サンサン', 
        'ããªã«': 'プリル',
        'ã·ã¼ã³': 'シーン',
        'ãã¯ã¢': 'ナクア',
        'ãã¤ãº': 'ノイズ',
        'ã¿ã¼å­': 'ハンター子',
        'èµ¤ã¡ãã': '赤ちゃん',
        'ç¬ã¬ã³ãã³': '犬ガンマン',
        'ã´ãã': 'ゴリ',
        'ãã¼ã ãããã': 'チームくもりん',
        'ã±ãã': 'ゲット',
        'ã­ãå©¦äºº': 'ロボ婦人',
        'ãã³å¤«äºº': 'パン夫人',
        'ãã¦ã¹ãã¼ã·ã§ã³': 'ナビゲーション'
    }
    
    total_fixed = 0
    
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
                ORDER BY s.management_id, cdu.row_number
                LIMIT 2
            """, (correct,))
            
            samples = cursor.fetchall()
            for management_id, dialogue, row in samples:
                row_str = f"{row:3d}" if row is not None else "---"
                dialogue_short = dialogue[:40] + "..." if len(dialogue) > 40 else dialogue
                print(f"    例: {management_id} 行{row_str} | {correct} | \"{dialogue_short}\"")
        else:
            print(f"ℹ️  該当データなし")
    
    # 修正後の統計確認
    print(f"\n📊 修正後キャラクター統計（上位15位）:")
    print("-" * 80)
    
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 15
    """)
    
    top_chars = cursor.fetchall()
    for i, (char_name, count) in enumerate(top_chars, 1):
        char_short = char_name[:25] + "..." if len(char_name) > 25 else char_name
        print(f"  {i:2d}. {char_short:30s}: {count:6,}件")
    
    # 残存文字化けチェック
    print(f"\n⚠️ 残存文字化けチェック:")
    print("-" * 50)
    
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE character_name LIKE '%ã%' OR character_name LIKE '%ï%'
        GROUP BY character_name
        ORDER BY count DESC
    """)
    
    remaining_garbled = cursor.fetchall()
    if remaining_garbled:
        for char_name, count in remaining_garbled:
            print(f"  ⚠️  {char_name}: {count}件")
    else:
        print("  ✅ 文字化けデータなし")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 文字化け一括修正完了: {total_fixed}件")
    return total_fixed

if __name__ == "__main__":
    fixed_count = bulk_garbled_fix()
    print(f"\n🎯 総修正件数: {fixed_count}件")