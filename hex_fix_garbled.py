#!/usr/bin/env python3
"""
16進値による文字化けキャラクター名の直接修正
"""

import sqlite3

def hex_fix_garbled():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 16進値による文字化け修正")
    print("=" * 80)
    
    # 16進値ベースの修正マッピング
    hex_fixes = [
        ('C3A3C281C28FC3A3C282C282C3A3C282C28AC3A3C282C293', 'くもりん'),  # ãããã
        ('C3A3C282C2B5C3A3C282C293C3A3C282C2B5C3A3C282C293', 'サンサン'),  # ãµã³ãµã³  
        ('C3A3C281C299C3A3C282C2AAC3A3C281C2A4C3A3C281C2AB', 'プリル'),    # ããªã«
        ('C3A3C281C299C3A3C282C2A4C3A3C282C29A', 'ノイズ'),              # ãã¤ãº
        ('C3A3C281C28FC3A3C281C2A3C3A3C281C2AF', 'ナクア'),              # ãã¯ã¢
        ('C3A3C282C2B7C3A3C281C28AC3A3C281C28AC3A3C282C293', 'シーン'),  # ã·ã¼ã³
    ]
    
    total_fixed = 0
    
    for hex_value, correct_name in hex_fixes:
        print(f"\n🔤 16進値修正: → '{correct_name}'")
        
        # 16進値で直接更新
        cursor.execute(f"""
            UPDATE character_dialogue_unified 
            SET character_name = ? 
            WHERE hex(character_name) = ?
        """, (correct_name, hex_value))
        
        fixed_count = cursor.rowcount
        total_fixed += fixed_count
        
        if fixed_count > 0:
            print(f"  ✅ 修正完了: {fixed_count}件")
        else:
            print(f"  ℹ️ 該当データなし")
    
    # より簡単な方法：LIKE演算子で直接置換
    print(f"\n🔄 パターンマッチング修正:")
    
    pattern_fixes = [
        ('%ãããã%', 'くもりん'),
        ('%ãµã³ãµã³%', 'サンサン'),
        ('%ããªã«%', 'プリル'),
        ('%ãã¤ãº%', 'ノイズ'),
        ('%ãã¯ã¢%', 'ナクア'),
        ('%ã·ã¼ã³%', 'シーン'),
        ('%ã¿ã¼å­%', 'ハンター子'),
        ('%èµ¤ã¡ãã%', '赤ちゃん'),
        ('%ç¬ã¬ã³ãã³%', '犬ガンマン'),
        ('%ã´ãã%', 'ゴリ'),
        ('%ãã¼ã ãããã%', 'チームくもりん'),
        ('%ã±ãã%', 'ゲット'),
        ('%ã­ãå©¦äºº%', 'ロボ婦人'),
        ('%ãã³å¤«äºº%', 'パン夫人'),
        ('%ãã¦ã¹ãã¼ã·ã§ã³%', 'ナビゲーション')
    ]
    
    for pattern, correct_name in pattern_fixes:
        print(f"🔤 パターン'{pattern}' → '{correct_name}'")
        
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET character_name = ? 
            WHERE character_name LIKE ?
        """, (correct_name, pattern))
        
        fixed_count = cursor.rowcount
        total_fixed += fixed_count
        
        if fixed_count > 0:
            print(f"  ✅ 修正完了: {fixed_count}件")
        else:
            print(f"  ℹ️ 該当データなし")
    
    # コミット
    conn.commit()
    
    # 修正後確認
    print(f"\n📊 修正後文字化け残存確認:")
    cursor.execute("""
        SELECT character_name, COUNT(*) 
        FROM character_dialogue_unified 
        WHERE character_name LIKE '%ã%' OR character_name LIKE '%ï%'
        GROUP BY character_name 
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    
    remaining = cursor.fetchall()
    if remaining:
        total_remaining = sum(count for _, count in remaining)
        print(f"  ⚠️ 残存: {total_remaining}件")
        for char_name, count in remaining[:5]:  # 上位5件のみ表示
            print(f"    {char_name}: {count}件")
    else:
        print(f"  ✅ 文字化けデータなし")
    
    # 修正後の主要キャラクター確認
    cursor.execute("""
        SELECT character_name, COUNT(*) 
        FROM character_dialogue_unified 
        WHERE character_name IN ('サンサン', 'くもりん', 'プリル', 'ノイズ')
        GROUP BY character_name
        ORDER BY COUNT(*) DESC
    """)
    
    main_chars = cursor.fetchall()
    print(f"\n📈 主要キャラクター確認:")
    for char_name, count in main_chars:
        print(f"  {char_name}: {count:,}件")
    
    conn.close()
    
    print(f"\n✅ 文字化け修正完了: {total_fixed}件")
    return total_fixed

if __name__ == "__main__":
    fixed_count = hex_fix_garbled()
    print(f"\n🎯 総修正件数: {fixed_count}件")