#!/usr/bin/env python3
"""
真の指示データの精密チェック
"""

import sqlite3
from datetime import datetime

def precise_instruction_check():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    output_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/precise_instruction_list.txt"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 真の指示データ精密チェック開始")
    print("=" * 80)
    
    # 現在の状況
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    print(f"📊 現在の状況:")
    print(f"  指示データ: {current_instruction_count}件")
    print(f"  セリフデータ: {current_dialogue_count}件")
    print()
    
    report_lines = []
    report_lines.append("真の指示データ精密チェック結果")
    report_lines.append("=" * 80)
    report_lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    all_instructions = []
    
    # 1. 明確な技術指示（BGM、SE等を含む実際の指示）
    print("🎵 明確な技術指示:")
    print("-" * 50)
    report_lines.append("🎵 明確な技術指示:")
    report_lines.append("-" * 50)
    
    tech_patterns = [
        '%BGM切る%', '%BGMとまる%', '%BGMを%', '%BGM%変更%',
        '%SE%つける%', '%SE%入れる%', '%効果音%入れる%', '%効果音など%',
        '%音声%調整%', '%音量%調整%', '%ボイス%調整%'
    ]
    
    for pattern in tech_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  {pattern}: {len(matches)}件")
            report_lines.append(f"⚠️  {pattern}: {len(matches)}件")
            for management_id, char_name, dialogue, row in matches:
                row_str = f"{row:3d}" if row is not None else "---"
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"   {management_id} 行{row_str} | {char_short:30s} | \"{dialogue_short}\"")
                report_lines.append(f"   {management_id} 行{row_str} | {char_short:30s} | \"{dialogue_short}\"")
                all_instructions.append((management_id, char_name, dialogue, row, f"技術指示:{pattern}"))
            print()
            report_lines.append("")
    
    # 2. 明確なシーン説明（「シーン」キャラクター）
    print("🎬 シーン説明（指示的内容のみ）:")
    print("-" * 50)
    report_lines.append("🎬 シーン説明（指示的内容のみ）:")
    report_lines.append("-" * 50)
    
    cursor.execute("""
        SELECT s.management_id, cdu.dialogue_text, cdu.row_number
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 0 
        AND cdu.character_name = 'シーン'
        AND (
            cdu.dialogue_text LIKE '%テロップ%' OR
            cdu.dialogue_text LIKE '%カット%' OR
            cdu.dialogue_text LIKE '%アングル%' OR
            cdu.dialogue_text LIKE '%ズーム%' OR
            cdu.dialogue_text LIKE '%フォーカス%' OR
            cdu.dialogue_text LIKE '%エフェクト%' OR
            cdu.dialogue_text LIKE '%トランジション%' OR
            cdu.dialogue_text LIKE '%明度%' OR
            cdu.dialogue_text LIKE '%演出%' OR
            cdu.dialogue_text LIKE '%BGM%' OR
            cdu.dialogue_text LIKE '%SE%'
        )
        LIMIT 20
    """)
    
    scene_instructions = cursor.fetchall()
    print(f"⚠️  シーン指示: {len(scene_instructions)}件（最初の20件表示）")
    report_lines.append(f"⚠️  シーン指示: {len(scene_instructions)}件（最初の20件表示）")
    
    for management_id, dialogue, row in scene_instructions:
        row_str = f"{row:3d}" if row is not None else "---"
        dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
        print(f"   {management_id} 行{row_str} | シーン | \"{dialogue_short}\"")
        report_lines.append(f"   {management_id} 行{row_str} | シーン | \"{dialogue_short}\"")
        all_instructions.append((management_id, 'シーン', dialogue, row, "シーン指示"))
    
    print()
    report_lines.append("")
    
    # 3. 明確なSE指示（「SE」キャラクター）
    print("🔊 SE指示:")
    print("-" * 50)
    report_lines.append("🔊 SE指示:")
    report_lines.append("-" * 50)
    
    cursor.execute("""
        SELECT s.management_id, cdu.dialogue_text, cdu.row_number
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 0 
        AND cdu.character_name = 'SE'
        LIMIT 20
    """)
    
    se_instructions = cursor.fetchall()
    print(f"⚠️  SE指示: {len(se_instructions)}件（最初の20件表示）")
    report_lines.append(f"⚠️  SE指示: {len(se_instructions)}件（最初の20件表示）")
    
    for management_id, dialogue, row in se_instructions:
        row_str = f"{row:3d}" if row is not None else "---"
        dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
        print(f"   {management_id} 行{row_str} | SE | \"{dialogue_short}\"")
        report_lines.append(f"   {management_id} 行{row_str} | SE | \"{dialogue_short}\"")
        all_instructions.append((management_id, 'SE', dialogue, row, "SE指示"))
    
    print()
    report_lines.append("")
    
    # 4. FALSE キャラクター（映像説明）
    print("📹 FALSE キャラクター（映像説明）:")
    print("-" * 50)
    report_lines.append("📹 FALSE キャラクター（映像説明）:")
    report_lines.append("-" * 50)
    
    cursor.execute("""
        SELECT COUNT(*)
        FROM character_dialogue_unified cdu
        WHERE cdu.is_instruction = 0 
        AND cdu.character_name = 'FALSE'
    """)
    
    false_count = cursor.fetchone()[0]
    print(f"⚠️  FALSE（映像説明）: {false_count}件")
    report_lines.append(f"⚠️  FALSE（映像説明）: {false_count}件")
    
    cursor.execute("""
        SELECT s.management_id, cdu.dialogue_text, cdu.row_number
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 0 
        AND cdu.character_name = 'FALSE'
        LIMIT 10
    """)
    
    false_samples = cursor.fetchall()
    print("   例（最初の10件）:")
    report_lines.append("   例（最初の10件）:")
    
    for management_id, dialogue, row in false_samples:
        row_str = f"{row:3d}" if row is not None else "---"
        dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
        print(f"   {management_id} 行{row_str} | FALSE | \"{dialogue_short}\"")
        report_lines.append(f"   {management_id} 行{row_str} | FALSE | \"{dialogue_short}\"")
    
    print()
    report_lines.append("")
    
    # 5. その他の指示系キャラクター
    print("📋 その他指示系キャラクター:")
    print("-" * 50)
    report_lines.append("📋 その他指示系キャラクター:")
    report_lines.append("-" * 50)
    
    other_instruction_chars = ['ナレーション', 'スタッフ', 'BGM', '効果音', 'エフェクト']
    
    for char in other_instruction_chars:
        cursor.execute("""
            SELECT COUNT(*)
            FROM character_dialogue_unified cdu
            WHERE cdu.is_instruction = 0 
            AND cdu.character_name LIKE ?
        """, (f'%{char}%',))
        
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"⚠️  {char}系: {count}件")
            report_lines.append(f"⚠️  {char}系: {count}件")
    
    print()
    report_lines.append("")
    
    # 6. 総計と推奨
    # FALSEキャラクターを含めた真の指示データ総計
    cursor.execute("""
        SELECT COUNT(*)
        FROM character_dialogue_unified cdu
        WHERE cdu.is_instruction = 0 
        AND (
            cdu.character_name = 'FALSE' OR
            cdu.character_name = 'SE' OR
            cdu.character_name = 'シーン' OR
            cdu.character_name LIKE '%ナレーション%' OR
            cdu.character_name LIKE '%スタッフ%' OR
            cdu.character_name LIKE '%BGM%' OR
            cdu.character_name LIKE '%効果音%' OR
            cdu.dialogue_text LIKE '%BGM切る%' OR
            cdu.dialogue_text LIKE '%BGMとまる%' OR
            cdu.dialogue_text LIKE '%効果音など%'
        )
    """)
    
    total_true_instructions = cursor.fetchone()[0]
    
    print("=" * 80)
    print(f"📊 真の指示データ総計: {total_true_instructions}件")
    print()
    print("💡 推奨事項:")
    print("  最も大きな問題は以下のキャラクターです:")
    print(f"  - FALSE: {false_count}件（映像説明）")
    print("  - シーン: 指示的内容多数")
    print("  - SE: 効果音指示")
    print()
    print("  これらを is_instruction=1 にフラグ設定することで")
    print("  検索結果から指示データを完全に除外できます")
    
    report_lines.append("=" * 80)
    report_lines.append(f"📊 真の指示データ総計: {total_true_instructions}件")
    report_lines.append("")
    report_lines.append("💡 推奨事項:")
    report_lines.append("  最も大きな問題は以下のキャラクターです:")
    report_lines.append(f"  - FALSE: {false_count}件（映像説明）")
    report_lines.append("  - シーン: 指示的内容多数")
    report_lines.append("  - SE: 効果音指示")
    report_lines.append("")
    report_lines.append("  これらを is_instruction=1 にフラグ設定することで")
    report_lines.append("  検索結果から指示データを完全に除外できます")
    
    # ファイル出力
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\n📝 精密チェック結果を {output_file} に保存しました")
    
    conn.close()
    return total_true_instructions

if __name__ == "__main__":
    total_instructions = precise_instruction_check()
    
    if total_instructions > 0:
        print(f"\n🔧 {total_instructions}件の真の指示データのフラグ設定を推奨します")