#!/usr/bin/env python3
"""
B692を正しく再処理
"""

import sqlite3
import requests
import pandas as pd
import io

def reprocess_b692():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    csv_url = "https://docs.google.com/spreadsheets/d/1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs/export?format=csv&gid=1384097767"
    
    try:
        # CSV取得
        response = requests.get(csv_url, timeout=30)
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, header=None)
        
        # ヘッダー検出（行4がヘッダー）
        header_row = 4
        character_col = 2  # キャラクター列
        dialogue_col = 3   # セリフ列（行4の列3は空なので、実際のセリフは別の位置）
        
        print(f"ヘッダー行{header_row}の内容:")
        if header_row < len(df):
            row = df.iloc[header_row]
            for i, val in enumerate(row[:8]):
                print(f"  列{i}: \"{val}\"")
        
        # 実際のデータ構造を確認
        print(f"\nデータ行の例（行6-10）:")
        for row_idx in range(6, min(11, len(df))):
            row = df.iloc[row_idx]
            char_val = row.iloc[2] if len(row) > 2 else None
            dialogue_val = row.iloc[3] if len(row) > 3 else None
            print(f"  行{row_idx}: キャラ=\"{char_val}\" セリフ=\"{dialogue_val}\"")
        
        # データ抽出
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
        
        # とんかつ・エビフライを含むセリフをチェック
        target_items = []
        for item in dialogue_data:
            if 'とんかつ' in item['dialogue'] or 'エビフライ' in item['dialogue']:
                target_items.append(item)
        
        print(f"\nとんかつ・エビフライ関連: {len(target_items)}件")
        for item in target_items:
            print(f"  行{item['row']}: {item['character']} - \"{item['dialogue']}\"")
        
        # データベース更新
        if dialogue_data:
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
                print(f"\n✅ B692: {len(dialogue_data)}件を更新完了")
            else:
                print("❌ B692のスクリプトIDが見つかりません")
            
            conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    reprocess_b692()