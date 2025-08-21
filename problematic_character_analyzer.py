#!/usr/bin/env python3
"""
問題キャラクター（TRUE/SE等）の出現スクリプト分析
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO

def analyze_problematic_characters():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 問題キャラクターの出現スクリプト分析")
    print("=" * 80)
    
    # 問題キャラクターが出現するスクリプトを特定
    problematic_chars = ['TRUE', 'SE', 'みんな']
    
    for char in problematic_chars:
        print(f"\n📌 {char} キャラクターの分析:")
        print("-" * 50)
        
        # 該当キャラクターが出現するスクリプトと行数を取得
        cursor.execute("""
            SELECT s.management_id, s.script_url, COUNT(*) as count,
                   MIN(cdu.row_number) as min_row, MAX(cdu.row_number) as max_row
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 AND cdu.character_name = ?
            GROUP BY s.management_id, s.script_url
            ORDER BY count DESC
            LIMIT 5
        """, (char,))
        
        scripts = cursor.fetchall()
        
        for management_id, url, count, min_row, max_row in scripts:
            print(f"  📄 {management_id}: {count}件 (行{min_row}-{max_row})")
            print(f"     URL: {url}")
            
            # サンプルデータ表示
            cursor.execute("""
                SELECT cdu.dialogue_text, cdu.row_number
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE cdu.is_instruction = 0 AND cdu.character_name = ?
                AND s.management_id = ?
                LIMIT 3
            """, (char, management_id))
            
            samples = cursor.fetchall()
            for dialogue, row in samples:
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                row_str = f"{row:3d}" if row is not None else "---"
                print(f"     行{row_str}: \"{dialogue_short}\"")
            print()
    
    print("\n" + "=" * 80)
    print("💡 分析結果:")
    print("  問題キャラクターは特定のスクリプトに集中している")
    print("  スプレッドシート構造の再確認が必要")
    
    conn.close()

def check_spreadsheet_structure(management_id, url):
    """特定スプレッドシートの構造確認"""
    print(f"\n🔍 {management_id} のスプレッドシート構造確認")
    print("-" * 60)
    
    try:
        # CSV取得
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape}")
        print(f"📋 列名: {list(df.columns)}")
        print()
        
        # 最初の20行を表示
        print("📄 最初の20行:")
        for i in range(min(20, len(df))):
            row_data = []
            for j in range(min(6, len(df.columns))):  # 最初の6列まで
                cell_value = str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""
                cell_short = cell_value[:15] + "..." if len(cell_value) > 15 else cell_value
                row_data.append(f"[{j}]{cell_short}")
            
            print(f"  行{i+1:2d}: {' | '.join(row_data)}")
        
        # キャラクター列とセリフ列の候補特定
        print(f"\n🎯 構造分析:")
        
        # 各列で「サンサン」「くもりん」等のキャラクター名の出現頻度をチェック
        main_characters = ['サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ']
        
        for col_idx in range(min(6, len(df.columns))):
            char_count = 0
            for char in main_characters:
                char_count += df.iloc[:, col_idx].astype(str).str.contains(char, na=False).sum()
            
            if char_count > 0:
                print(f"  列{col_idx}({df.columns[col_idx]}): メインキャラクター {char_count}回出現")
        
        return df
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        return None

if __name__ == "__main__":
    analyze_problematic_characters()
    
    # 代表的な問題スクリプトの詳細確認
    print("\n" + "=" * 80)
    print("📄 代表スクリプトの詳細構造確認")
    
    # B884の構造確認（TRUEキャラクターが多い）
    check_spreadsheet_structure("B884", "https://docs.google.com/spreadsheets/d/1G7jCfP7tH9YKuQGWdLzOYPLQ1DJy0x5r8K-rTyH7pVw/export?format=csv&gid=0")