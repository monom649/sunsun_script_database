#!/usr/bin/env python3
"""
セリフ列に混入した指示データの包括的フラグ設定
"""

import sqlite3
from datetime import datetime

def comprehensive_instruction_flag_update():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 セリフ列混入指示データの包括的フラグ設定開始")
    print("=" * 80)
    
    # 現在の状況確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    print(f"📊 処理前状況:")
    print(f"  指示データ: {current_instruction_count}件")
    print(f"  セリフデータ: {current_dialogue_count}件")
    print()
    
    total_flagged = 0
    
    # 1. FALSE キャラクター（映像説明）を全てフラグ設定
    print("📹 FALSE キャラクター（映像説明）をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 AND character_name = 'FALSE'
    """)
    false_flagged = cursor.rowcount
    total_flagged += false_flagged
    print(f"  ✅ FALSE: {false_flagged}件をフラグ設定")
    
    # 2. SE キャラクター（映像描写）を全てフラグ設定
    print("🔊 SE キャラクター（映像描写）をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 AND character_name = 'SE'
    """)
    se_flagged = cursor.rowcount
    total_flagged += se_flagged
    print(f"  ✅ SE: {se_flagged}件をフラグ設定")
    
    # 3. みんな キャラクター（グループ動作説明）をフラグ設定
    print("👥 みんな キャラクター（グループ動作）をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 AND character_name = 'みんな'
    """)
    minna_flagged = cursor.rowcount
    total_flagged += minna_flagged
    print(f"  ✅ みんな: {minna_flagged}件をフラグ設定")
    
    # 4. TRUE キャラクター（存在する場合）
    print("✅ TRUE キャラクター（存在確認）...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 AND character_name = 'TRUE'
    """)
    true_flagged = cursor.rowcount
    total_flagged += true_flagged
    if true_flagged > 0:
        print(f"  ✅ TRUE: {true_flagged}件をフラグ設定")
    else:
        print("  ℹ️ TRUE キャラクターは存在しませんでした")
    
    # 5. シーン キャラクター（技術指示内容のみ）
    print("🎬 シーン キャラクター（技術指示）をフラグ設定中...")
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
            dialogue_text LIKE '%調整%' OR
            dialogue_text LIKE '%追加%' OR
            dialogue_text LIKE '%切り替え%'
        )
    """)
    scene_flagged = cursor.rowcount
    total_flagged += scene_flagged
    print(f"  ✅ シーン（技術指示）: {scene_flagged}件をフラグ設定")
    
    # 6. ナレーション系キャラクター
    print("🎙️ ナレーション系キャラクターをフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 
        AND character_name LIKE '%ナレーション%'
    """)
    narration_flagged = cursor.rowcount
    total_flagged += narration_flagged
    print(f"  ✅ ナレーション系: {narration_flagged}件をフラグ設定")
    
    # 7. その他明確な指示系キャラクター
    instruction_characters = [
        'BGM', '効果音', 'エフェクト', 'スタッフ', 'カメラ', '編集', '音響',
        '撮影指示', '音声指示', '編集指示', '制作指示'
    ]
    
    for char in instruction_characters:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE is_instruction = 0 AND character_name LIKE ?
        """, (f'%{char}%',))
        
        char_flagged = cursor.rowcount
        if char_flagged > 0:
            total_flagged += char_flagged
            print(f"  ✅ {char}系: {char_flagged}件をフラグ設定")
    
    # 8. 技術指示を含むテキスト（キャラクター名に関係なく）
    print("⚙️ 技術指示テキストをフラグ設定中...")
    tech_patterns = [
        '%BGM切る%', '%BGMとまる%', '%BGMを%変更%', '%BGM%ストップ%',
        '%効果音など%', '%効果音%入れる%', '%SE%つける%', '%SE%追加%',
        '%音声%調整%', '%音量%調整%', '%明度%調整%',
        '%カット%分ける%', '%カット%切り替え%', '%アングル%',
        '%エフェクト%&%SE%', '%演出%で%', '%フォーカス%する%'
    ]
    
    tech_total = 0
    for pattern in tech_patterns:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE is_instruction = 0 
            AND dialogue_text LIKE ?
            AND character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
        """, (pattern,))
        
        pattern_flagged = cursor.rowcount
        tech_total += pattern_flagged
    
    total_flagged += tech_total
    print(f"  ✅ 技術指示テキスト: {tech_total}件をフラグ設定")
    
    # 9. 空白・無効なキャラクター名
    print("🔤 空白・無効キャラクター名をフラグ設定中...")
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 1 
        WHERE is_instruction = 0 
        AND (
            character_name = '' OR 
            character_name IS NULL OR
            character_name = '[空白]' OR
            character_name = '不明' OR
            character_name LIKE '%https://%' OR
            character_name LIKE '%:///%'
        )
    """)
    empty_flagged = cursor.rowcount
    total_flagged += empty_flagged
    print(f"  ✅ 空白・無効キャラクター: {empty_flagged}件をフラグ設定")
    
    # 10. 処理結果の確認
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
    
    # データ品質チェック
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 15
    """)
    
    remaining_chars = cursor.fetchall()
    print("📈 残存キャラクター別統計（上位15位）:")
    for char_name, count in remaining_chars:
        char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
        print(f"  {char_short:35s}: {count:6d}件")
    
    conn.commit()
    conn.close()
    
    print()
    print("✅ 包括的指示データフラグ設定完了")
    return total_flagged, final_instruction_count, final_dialogue_count

if __name__ == "__main__":
    flagged, instructions, dialogues = comprehensive_instruction_flag_update()
    
    print(f"\n🎯 結果: {flagged}件の指示データをフラグ設定")
    print(f"📊 最終データ構成: 指示{instructions}件 + セリフ{dialogues}件")
    
    if flagged > 0:
        print(f"\n💡 検索システムの品質が大幅に向上しました")