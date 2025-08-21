#!/usr/bin/env python3
"""
確実な指示データのみの精密フラグ設定
"""

import sqlite3

def precise_instruction_update():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🎯 確実な指示データの精密フラグ設定開始")
    print("=" * 80)
    
    # 現在の状況
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    print(f"📊 処理前状況:")
    print(f"  指示データ: {current_instruction_count}件")
    print(f"  セリフデータ: {current_dialogue_count}件")
    print()
    
    total_flagged = 0
    
    # 1. FALSE キャラクター（映像説明）- 確実に撮影指示
    print("📹 FALSE キャラクター（映像説明）をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 AND character_name = 'FALSE'
    """)
    false_flagged = cursor.rowcount
    total_flagged += false_flagged
    print(f"  ✅ FALSE: {false_flagged}件をフラグ設定")
    
    # 2. 明確な技術指示を含むテキスト
    print("⚙️ 明確な技術指示テキストをフラグ設定中...")
    tech_patterns = [
        '%BGM切る%', '%BGMとまる%', '%BGMを%変更%', '%BGM%ストップ%',
        '%効果音など%', '%効果音%入れる%', '%SE%つける%', '%SE%追加%',
        '%音声%調整%', '%音量%調整%', '%明度%調整%'
    ]
    
    tech_total = 0
    for pattern in tech_patterns:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE is_instruction = 0 
            AND dialogue_text LIKE ?
        """, (pattern,))
        
        pattern_flagged = cursor.rowcount
        if pattern_flagged > 0:
            tech_total += pattern_flagged
            print(f"  ✅ {pattern}: {pattern_flagged}件")
    
    total_flagged += tech_total
    print(f"  📊 技術指示テキスト合計: {tech_total}件")
    
    # 3. シーン キャラクターの技術指示のみ
    print("🎬 シーン キャラクター（技術指示のみ）をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 
        AND character_name = 'シーン'
        AND (
            dialogue_text LIKE '%テロップ%' OR
            dialogue_text LIKE '%カット%' OR
            dialogue_text LIKE '%アングル%' OR
            dialogue_text LIKE '%ズーム%' OR
            dialogue_text LIKE '%フォーカス%' OR
            dialogue_text LIKE '%エフェクト%' OR
            dialogue_text LIKE '%トランジション%' OR
            dialogue_text LIKE '%明度%' OR
            dialogue_text LIKE '%演出%' OR
            dialogue_text LIKE '%BGM%' OR
            dialogue_text LIKE '%SE%' OR
            dialogue_text LIKE '%音声%' OR
            dialogue_text LIKE '%編集%' OR
            dialogue_text LIKE '%調整%'
        )
    """)
    scene_flagged = cursor.rowcount
    total_flagged += scene_flagged
    print(f"  ✅ シーン（技術指示）: {scene_flagged}件をフラグ設定")
    
    # 4. 処理結果の確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    final_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    final_dialogue_count = cursor.fetchone()[0]
    
    print()
    print("=" * 80)
    print(f"📊 処理結果:")
    print(f"  今回フラグ設定: {total_flagged}件")
    print(f"  最終指示データ: {final_instruction_count}件")
    print(f"  最終セリフデータ: {final_dialogue_count}件")
    print()
    
    # 残存キャラクター統計（問題チェック）
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        AND character_name IN ('SE', 'みんな', 'TRUE', 'シーン')
        GROUP BY character_name
        ORDER BY count DESC
    """)
    
    remaining_problematic = cursor.fetchall()
    if remaining_problematic:
        print("⚠️ 残存する問題キャラクター:")
        for char_name, count in remaining_problematic:
            print(f"  {char_name}: {count}件")
        print()
    
    conn.commit()
    conn.close()
    
    print("✅ 精密指示データフラグ設定完了")
    return total_flagged, final_instruction_count, final_dialogue_count

if __name__ == "__main__":
    flagged, instructions, dialogues = precise_instruction_update()
    
    print(f"\n🎯 結果: {flagged}件の確実な指示データをフラグ設定")
    print(f"📊 最終データ構成: 指示{instructions}件 + セリフ{dialogues}件")