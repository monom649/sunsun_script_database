#!/usr/bin/env python3
"""
文字化けキャラクター名の強制修正
"""

import sqlite3

def force_fix_garbled_characters():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 文字化けキャラクター名の強制修正")
    print("=" * 80)
    
    # 修正前の文字化けキャラクター統計
    cursor.execute("""
        SELECT character_name, COUNT(*) 
        FROM character_dialogue_unified 
        WHERE character_name LIKE '%ã%' OR character_name LIKE '%ï%'
        GROUP BY character_name 
        ORDER BY COUNT(*) DESC
    """)
    
    garbled_before = cursor.fetchall()
    total_garbled = sum(count for _, count in garbled_before)
    
    print(f"📊 修正前文字化けデータ: {total_garbled}件")
    for char_name, count in garbled_before:
        print(f"  {char_name}: {count}件")
    
    # 個別に修正実行
    fixes = [
        ('ãããã', 'くもりん'),
        ('ãµã³ãµã³', 'サンサン'),
        ('ããªã«', 'プリル'),
        ('ã·ã¼ã³', 'シーン'),
        ('ãã¯ã¢', 'ナクア'),
        ('ãã¤ãº', 'ノイズ'),
        ('ã¿ã¼å­', 'ハンター子'),
        ('èµ¤ã¡ãã', '赤ちゃん'),
        ('ç¬ã¬ã³ãã³', '犬ガンマン'),
        ('ã´ãã', 'ゴリ'),
        ('ãã¼ã ãããã', 'チームくもりん'),
        ('ã±ãã', 'ゲット'),
        ('ã­ãå©¦äºº', 'ロボ婦人'),
        ('ãã³å¤«äºº', 'パン夫人'),
        ('ãã¦ã¹ãã¼ã·ã§ã³', 'ナビゲーション')
    ]
    
    total_fixed = 0
    
    for garbled, correct in fixes:
        print(f"\n🔤 '{garbled}' → '{correct}' の修正")
        
        # 修正前件数確認
        cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE character_name = ?", (garbled,))
        before_count = cursor.fetchone()[0]
        
        if before_count > 0:
            # 修正実行
            cursor.execute("""
                UPDATE character_dialogue_unified 
                SET character_name = ? 
                WHERE character_name = ?
            """, (correct, garbled))
            
            fixed_count = cursor.rowcount
            total_fixed += fixed_count
            
            print(f"  修正前: {before_count}件")
            print(f"  修正実行: {fixed_count}件")
            
            # 修正後確認
            cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE character_name = ?", (correct,))
            after_count = cursor.fetchone()[0]
            print(f"  修正後: {after_count}件")
            
            # サンプル表示
            cursor.execute("""
                SELECT s.management_id, cdu.dialogue_text 
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE cdu.character_name = ?
                LIMIT 2
            """, (correct,))
            
            samples = cursor.fetchall()
            for management_id, dialogue in samples:
                dialogue_short = dialogue[:30] + "..." if len(dialogue) > 30 else dialogue
                print(f"    例: {management_id} | {correct} | \"{dialogue_short}\"")
        else:
            print(f"  該当データなし")
    
    # トランザクションコミット
    conn.commit()
    
    # 修正後の残存文字化け確認
    print(f"\n📊 修正後残存文字化けチェック:")
    cursor.execute("""
        SELECT character_name, COUNT(*) 
        FROM character_dialogue_unified 
        WHERE character_name LIKE '%ã%' OR character_name LIKE '%ï%'
        GROUP BY character_name 
        ORDER BY COUNT(*) DESC
    """)
    
    remaining_garbled = cursor.fetchall()
    
    if remaining_garbled:
        total_remaining = sum(count for _, count in remaining_garbled)
        print(f"  ⚠️ 残存文字化け: {total_remaining}件")
        for char_name, count in remaining_garbled:
            print(f"    {char_name}: {count}件")
    else:
        print(f"  ✅ 文字化けデータなし")
    
    # 修正後の主要キャラクター統計
    print(f"\n📈 修正後主要キャラクター統計:")
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        AND character_name IN ('サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ', 'シーン')
        GROUP BY character_name
        ORDER BY count DESC
    """)
    
    main_chars = cursor.fetchall()
    for char_name, count in main_chars:
        print(f"  {char_name}: {count:,}件")
    
    conn.close()
    
    print(f"\n✅ 文字化け修正完了: {total_fixed}件")
    return total_fixed

if __name__ == "__main__":
    fixed_count = force_fix_garbled_characters()
    print(f"\n🎯 総修正件数: {fixed_count}件")