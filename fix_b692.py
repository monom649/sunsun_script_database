#!/usr/bin/env python3
"""
B692の正しい列構造でのデータ修正
"""

import sqlite3
import requests
import pandas as pd
import io

def fix_b692():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    csv_url = "https://docs.google.com/spreadsheets/d/1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs/export?format=csv&gid=1384097767"
    
    try:
        # CSV取得（UTF-8）
        response = requests.get(csv_url, timeout=30)
        content = response.content.decode('utf-8')
        csv_data = io.StringIO(content)
        df = pd.read_csv(csv_data, header=None)
        
        print(f"B692スプレッドシート: {len(df)}行 x {len(df.columns)}列")
        
        # 正しい列構造の確認
        header_row = 4
        character_col = 2  # キャラクター列
        dialogue_col = 3   # セリフ列
        
        # データ抽出（正しい列で）
        dialogue_data = []
        for idx in range(header_row + 1, len(df)):
            row = df.iloc[idx]
            
            if len(row) > max(character_col, dialogue_col):
                character = row.iloc[character_col]
                dialogue = row.iloc[dialogue_col]
                
                if pd.notna(character) and pd.notna(dialogue):
                    character_str = str(character).strip()
                    dialogue_str = str(dialogue).strip()
                    
                    if character_str and dialogue_str and len(character_str) < 100:
                        dialogue_data.append({
                            'row': idx,
                            'character': character_str,
                            'dialogue': dialogue_str
                        })
        
        print(f"\n抽出されたデータ: {len(dialogue_data)}件")
        
        # 対象のセリフを確認
        target_items = []
        for item in dialogue_data:
            if 'とんかつ' in item['dialogue'] or 'エビフライ' in item['dialogue']:
                target_items.append(item)
        
        print(f"\nとんかつ・エビフライ関連: {len(target_items)}件")
        for item in target_items:
            print(f"  行{item['row']}: {item['character']} - \"{item['dialogue']}\"")
        
        # データベース更新
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 既存データ削除
        cursor.execute("SELECT id FROM scripts WHERE management_id = 'B692'")
        script_id_result = cursor.fetchone()
        if script_id_result:
            script_id = script_id_result[0]
            cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
            
            # 新データ挿入
            for item in dialogue_data:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, row_number, character_name, dialogue_text) 
                    VALUES (?, ?, ?, ?)
                """, (script_id, item['row'], item['character'], item['dialogue']))
            
            conn.commit()
            print(f"\n✅ B692: {len(dialogue_data)}件を再更新完了")
        else:
            print("❌ B692のスクリプトIDが見つかりません")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    fix_b692()