#!/usr/bin/env python3
"""
B2300 Specific Fixer

This script specifically fixes the B2300 script using the provided reference URL.
"""

import sqlite3
import requests
import pandas as pd
import io

def fix_b2300_script():
    """Fix B2300 script specifically using reference URL"""
    
    # Reference URL provided by user
    reference_url = "https://docs.google.com/spreadsheets/d/17i46IX9hdVg3xQeoZ2DbbgcAgU1Ll5gPAWKPFSBUgJ8/export?format=csv&gid=2036772822"
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    print("=== B2300 トミカスクリプト個別修正 ===")
    
    try:
        # Fetch CSV data from reference URL
        print("参考URLからデータを取得中...")
        response = requests.get(reference_url, timeout=15)
        response.raise_for_status()
        
        # Parse CSV
        csv_data = response.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_data))
        
        print(f"スプレッドシートデータ取得完了: {len(df)}行")
        
        # Extract dialogue data with correct character-dialogue pairing
        dialogue_entries = []
        
        # Skip header rows and process data
        for index, row in df.iterrows():
            if index < 4:  # Skip header rows
                continue
            
            # Get all columns
            row_data = row.to_dict()
            
            # Look for character names and adjacent dialogue
            character_col = None
            dialogue_col = None
            instruction_col = None
            
            # Find character column (column with character names)
            for col_name, value in row_data.items():
                if pd.notna(value) and isinstance(value, str):
                    value_str = str(value).strip()
                    # Check if this looks like a character name
                    if value_str in ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB']:
                        character_col = col_name
                        character_name = value_str
                        
                        # Get column index to find adjacent dialogue
                        try:
                            # Find the next non-empty column as dialogue
                            cols = list(df.columns)
                            char_col_index = cols.index(col_name)
                            
                            # Look for dialogue in adjacent columns
                            for offset in [1, 2, 3]:  # Check next 3 columns
                                if char_col_index + offset < len(cols):
                                    dialogue_col_name = cols[char_col_index + offset]
                                    dialogue_value = row_data.get(dialogue_col_name)
                                    if pd.notna(dialogue_value) and str(dialogue_value).strip():
                                        dialogue_text = str(dialogue_value).strip()
                                        
                                        # Skip if dialogue looks like instruction
                                        if not any(inst_word in dialogue_text for inst_word in ['カット', '映像', 'テロップ', 'アニメ']):
                                            dialogue_entries.append({
                                                'row_number': index,
                                                'character_name': character_name,
                                                'dialogue_text': dialogue_text,
                                                'filming_audio_instructions': ''
                                            })
                                            print(f"  行{index}: {character_name} -> {dialogue_text[:30]}...")
                                            break
                        except Exception as e:
                            print(f"列解析エラー: {str(e)}")
                        break
        
        print(f"抽出されたセリフ: {len(dialogue_entries)}件")
        
        if len(dialogue_entries) == 0:
            print("❌ セリフデータが抽出できませんでした")
            return False
        
        # Update database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get B2300 script ID
        cursor.execute("SELECT id FROM scripts WHERE management_id = ?", ('B2300',))
        script_row = cursor.fetchone()
        if not script_row:
            print("❌ B2300スクリプトがデータベースに見つかりません")
            conn.close()
            return False
        
        script_id = script_row[0]
        
        # Delete existing empty entries
        cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ? AND (dialogue_text IS NULL OR dialogue_text = '')", (script_id,))
        deleted_count = cursor.rowcount
        print(f"空のエントリを削除: {deleted_count}件")
        
        # Insert new dialogue data
        inserted = 0
        for entry in dialogue_entries:
            try:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    script_id,
                    entry['row_number'],
                    entry['character_name'],
                    entry['dialogue_text'],
                    entry['filming_audio_instructions']
                ))
                inserted += 1
            except Exception as e:
                print(f"挿入エラー: {str(e)}")
        
        conn.commit()
        conn.close()
        
        print(f"✅ B2300スクリプト修正完了: {inserted}件のセリフを追加")
        
        # Verify Tomica count
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE dialogue_text LIKE '%トミカ%'")
        tomica_count = cursor.fetchone()[0]
        conn.close()
        
        print(f"🔍 修正後のトミカセリフ総数: {tomica_count}件")
        
        return True
        
    except Exception as e:
        print(f"❌ B2300修正エラー: {str(e)}")
        return False

if __name__ == "__main__":
    success = fix_b2300_script()
    if success:
        print("\n🎉 B2300スクリプトの修正が完了しました！")
    else:
        print("\n❌ B2300スクリプトの修正に失敗しました")