#!/usr/bin/env python3
"""
音声指示など全指示タイプの包括的チェック（結果出力版）
"""

import sqlite3
from datetime import datetime

def comprehensive_instruction_check_with_output():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    output_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/additional_instructions_to_flag.txt"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 追加指示データの包括的チェック開始")
    print("=" * 80)
    
    all_missed_instructions = []
    report_lines = []
    
    # レポートヘッダー
    report_lines.append("追加指示フラグ設定が必要なデータ一覧")
    report_lines.append("=" * 80)
    report_lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    # 1. 現在の状況確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    report_lines.append(f"📊 現在の状況:")
    report_lines.append(f"  指示データ (is_instruction=1): {current_instruction_count}件")
    report_lines.append(f"  セリフデータ (is_instruction=0): {current_dialogue_count}件")
    report_lines.append("")
    
    # 2. 技術系指示の詳細チェック
    technical_patterns = [
        '%BGM%', '%SE%', '%効果音%', '%音声%', '%ボイス%', '%サウンド%', 
        '%編集%', '%カット%', '%エフェクト%', '%テロップ%', '%字幕%',
        '%フェード%', '%ワイプ%', '%トランジション%', '%演出%', '%照明%',
        '%カメラ%', '%アングル%', '%ズーム%', '%フォーカス%', '%明度%',
        '%コントラスト%', '%色調%', '%露出%'
    ]
    
    report_lines.append("🔧 技術系指示データ:")
    report_lines.append("-" * 50)
    
    tech_count = 0
    for pattern in technical_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            report_lines.append(f"⚠️  パターン '{pattern}': {len(matches)}件")
            for management_id, char_name, dialogue, row in matches:
                char_short = char_name[:50] + "..." if len(char_name) > 50 else char_name
                dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
                row_str = f"{row:3d}" if row is not None else "---"
                report_lines.append(f"   {management_id} 行{row_str} | {char_short:35s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"技術系:{pattern}"))
                tech_count += 1
            report_lines.append("")
    
    # 3. ナレーション系の詳細チェック
    report_lines.append("🎙️ ナレーション系指示データ:")
    report_lines.append("-" * 50)
    
    narration_count = 0
    narration_patterns = ['%ナレーション%', '%ナレ%']
    
    for pattern in narration_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND cdu.character_name LIKE ?
        """, (pattern,))
        
        matches = cursor.fetchall()
        if matches:
            report_lines.append(f"⚠️  パターン '{pattern}': {len(matches)}件")
            for management_id, char_name, dialogue, row in matches:
                char_short = char_name[:50] + "..." if len(char_name) > 50 else char_name
                dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
                row_str = f"{row:3d}" if row is not None else "---"
                report_lines.append(f"   {management_id} 行{row_str} | {char_short:35s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"ナレ系:{pattern}"))
                narration_count += 1
            report_lines.append("")
    
    # 4. 指示系キャラクター名の詳細チェック
    report_lines.append("🎬 指示系キャラクター名:")
    report_lines.append("-" * 50)
    
    char_count = 0
    instruction_characters = ['SE', 'BGM', 'シーン', 'スタッフ', '効果音', 'エフェクト', '演出', 'カメラ']
    
    for char_pattern in instruction_characters:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND cdu.character_name LIKE ?
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
        """, (f'%{char_pattern}%',))
        
        matches = cursor.fetchall()
        if matches:
            report_lines.append(f"⚠️  キャラクター名 '{char_pattern}': {len(matches)}件")
            for management_id, char_name, dialogue, row in matches:
                char_short = char_name[:50] + "..." if len(char_name) > 50 else char_name
                dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
                row_str = f"{row:3d}" if row is not None else "---"
                report_lines.append(f"   {management_id} 行{row_str} | {char_short:35s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"指示キャラ:{char_pattern}"))
                char_count += 1
            report_lines.append("")
    
    # 5. 記号系指示の詳細チェック
    report_lines.append("📝 記号・括弧系指示データ:")
    report_lines.append("-" * 50)
    
    symbol_count = 0
    symbol_patterns = ['★%', '●%', '■%', '[%]', '【%】']
    
    for pattern in symbol_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            report_lines.append(f"⚠️  パターン '{pattern}': {len(matches)}件")
            for management_id, char_name, dialogue, row in matches:
                char_short = char_name[:50] + "..." if len(char_name) > 50 else char_name
                dialogue_short = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
                row_str = f"{row:3d}" if row is not None else "---"
                report_lines.append(f"   {management_id} 行{row_str} | {char_short:35s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"記号系:{pattern}"))
                symbol_count += 1
            report_lines.append("")
    
    # 6. 総合統計
    total_missed = len(all_missed_instructions)
    
    report_lines.append("=" * 80)
    report_lines.append(f"📊 追加フラグ設定が必要なデータ総計: {total_missed}件")
    report_lines.append("")
    report_lines.append(f"📈 カテゴリ別内訳:")
    report_lines.append(f"  技術系指示: {tech_count}件")
    report_lines.append(f"  ナレーション系: {narration_count}件") 
    report_lines.append(f"  指示系キャラクター: {char_count}件")
    report_lines.append(f"  記号・括弧系: {symbol_count}件")
    report_lines.append("")
    
    if total_missed > 0:
        report_lines.append("💡 推奨事項:")
        report_lines.append("  これらの指示データを is_instruction=1 にフラグ設定することで")
        report_lines.append("  検索結果の品質を大幅に向上できます")
        report_lines.append("")
    
    # レポートファイルに出力
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    # コンソール出力（要約）
    for line in report_lines:
        print(line)
    
    print(f"\n📝 詳細レポートを {output_file} に保存しました")
    
    conn.close()
    return total_missed, all_missed_instructions

if __name__ == "__main__":
    total_missed, missed_list = comprehensive_instruction_check_with_output()
    
    if total_missed > 0:
        print(f"\n🔧 追加フラグ設定スクリプトの作成を推奨します")
        print(f"   見つかった{total_missed}件の指示データを一括フラグ設定できます")