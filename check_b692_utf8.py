#!/usr/bin/env python3
"""
B692のUTF-8での詳細確認
"""

import requests
import pandas as pd
import io

def check_b692_utf8():
    csv_url = "https://docs.google.com/spreadsheets/d/1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs/export?format=csv&gid=1384097767"
    
    try:
        response = requests.get(csv_url, timeout=30)
        # UTF-8として明示的に解釈
        content = response.content.decode('utf-8')
        csv_data = io.StringIO(content)
        df = pd.read_csv(csv_data, header=None)
        
        print(f"B692スプレッドシート（UTF-8）: {len(df)}行 x {len(df.columns)}列")
        
        # 全データで「とんかつ」「エビフライ」検索
        found_items = []
        
        for row_idx in range(len(df)):
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value):
                    cell_str = str(cell_value)
                    if 'とんかつ' in cell_str or 'エビフライ' in cell_str or 'とんかつとエビフライ' in cell_str:
                        found_items.append({
                            'row': row_idx,
                            'col': col_idx,
                            'content': cell_str
                        })
        
        print(f"\n「とんかつ」「エビフライ」検索結果: {len(found_items)}件")
        for item in found_items:
            print(f"  行{item['row']}, 列{item['col']}: \"{item['content']}\"")
        
        if not found_items:
            print("  該当なし")
            
            # くもりんのセリフを検索
            print("\n「くもりん」のセリフを検索:")
            kumorin_items = []
            for row_idx in range(len(df)):
                row = df.iloc[row_idx]
                if len(row) > 2:
                    character = row.iloc[2]  # キャラクター列
                    dialogue = row.iloc[3] if len(row) > 3 else None   # セリフ列
                    
                    if pd.notna(character) and pd.notna(dialogue):
                        char_str = str(character).strip()
                        dial_str = str(dialogue).strip()
                        
                        if 'くもりん' in char_str:
                            kumorin_items.append({
                                'row': row_idx,
                                'character': char_str,
                                'dialogue': dial_str
                            })
            
            print(f"くもりんのセリフ: {len(kumorin_items)}件")
            for item in kumorin_items[:10]:  # 最初の10件
                print(f"  行{item['row']}: {item['character']} - \"{item['dialogue']}\"")
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_b692_utf8()