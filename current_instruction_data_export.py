#!/usr/bin/env python3
"""
現在指示と判断されているデータの書き出し
"""

import sqlite3
from datetime import datetime

def export_instruction_data():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    output_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/current_instruction_data.txt"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("📝 現在の指示データ書き出し開始")
    print("=" * 80)
    
    # 現在の指示データ総数確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    total_instructions = cursor.fetchone()[0]
    
    print(f"📊 指示データ総数: {total_instructions}件")
    print()
    
    # 全指示データを取得
    cursor.execute("""
        SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 1
        ORDER BY s.management_id, cdu.row_number
    """)
    
    instruction_data = cursor.fetchall()
    
    # テキストファイルに書き出し
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("現在指示データと判断されているテキスト一覧\n")
        f.write("=" * 80 + "\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"総件数: {total_instructions}件\n")
        f.write("\n")
        
        # キャラクター別統計
        cursor.execute("""
            SELECT character_name, COUNT(*) as count
            FROM character_dialogue_unified 
            WHERE is_instruction = 1
            GROUP BY character_name
            ORDER BY count DESC
        """)
        
        char_stats = cursor.fetchall()
        f.write("📊 キャラクター別統計:\n")
        f.write("-" * 50 + "\n")
        
        for char_name, count in char_stats:
            char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
            f.write(f"  {char_short:45s}: {count:6d}件\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("詳細データ一覧:\n")
        f.write("-" * 80 + "\n")
        
        # 詳細データ
        current_script = None
        for i, (management_id, char_name, dialogue_text, row_number) in enumerate(instruction_data, 1):
            if current_script != management_id:
                if current_script is not None:
                    f.write("\n")
                f.write(f"\n■ {management_id}\n")
                current_script = management_id
            
            row_str = f"{row_number:3d}" if row_number is not None else "---"
            char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
            
            # テキストが長い場合は改行して見やすく
            if len(dialogue_text) > 80:
                lines = []
                for j in range(0, len(dialogue_text), 80):
                    lines.append(dialogue_text[j:j+80])
                dialogue_display = "\n    ".join(lines)
            else:
                dialogue_display = dialogue_text
            
            f.write(f"  行{row_str} | {char_short:30s} | \"{dialogue_display}\"\n")
            
            # 進捗表示（大量データ対応）
            if i % 1000 == 0:
                print(f"  処理中: {i}/{total_instructions}件 ({i/total_instructions*100:.1f}%)")
    
    print(f"\n📝 指示データを {output_file} に書き出し完了")
    print(f"📊 総件数: {total_instructions}件")
    
    # 統計表示
    print(f"\n📈 キャラクター別統計（上位10位）:")
    for i, (char_name, count) in enumerate(char_stats[:10], 1):
        char_short = char_name[:35] + "..." if len(char_name) > 35 else char_name
        print(f"  {i:2d}. {char_short:40s}: {count:6d}件")
    
    conn.close()
    return total_instructions

if __name__ == "__main__":
    total = export_instruction_data()
    print(f"\n✅ {total}件の指示データ書き出し完了")