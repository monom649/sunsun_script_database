#!/usr/bin/env python3
"""
SEキャラクターの誤抽出修正 - 個別スプレッドシート対応
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO

def fix_se_character_misextraction():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 SEキャラクター誤抽出の個別修正")
    print("=" * 80)
    
    # 問題スクリプトの個別対応
    problem_scripts = [
        {
            'id': 'B953',
            'url': 'https://docs.google.com/spreadsheets/d/1rSPNsKEglxmgUOQj3GZuFPINhDWgLvmbsSGSS8TpGWA/export?format=csv&gid=1384097767',
            'char_col': 2,  # キャラクター列
            'dialogue_col': 3  # セリフ列
        },
        {
            'id': 'B1366',
            'url': 'https://docs.google.com/spreadsheets/d/1DZYh4_wqRGwsgObgaSG1ipwim4_UDYQu5OPa-8n3n_Y/export?format=csv&gid=1384097767',
            'char_col': 2,  # キャラクター列  
            'dialogue_col': 3  # セリフ列
        },
        {
            'id': 'B965',
            'url': 'https://docs.google.com/spreadsheets/d/1HNiuovG8-3xbHSnsrO00B7WI4qU4CZ0di8GeFSejuRI/export?format=csv&gid=1384097767',
            'char_col': 2,  # キャラクター列
            'dialogue_col': 3  # セリフ列
        }
    ]
    
    total_fixed = 0
    
    for script_info in problem_scripts:
        print(f"\n🎯 {script_info['id']} の修正処理")
        print("-" * 60)
        
        fixed_count = fix_single_se_script(script_info, cursor)
        total_fixed += fixed_count
        
        print(f"✅ {script_info['id']}: {fixed_count}件修正")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎯 SE誤抽出修正完了: {total_fixed}件")
    return total_fixed

def fix_single_se_script(script_info, cursor):
    """単一スクリプトのSE誤抽出修正"""
    
    try:
        print(f"📡 スプレッドシート取得中...")
        response = requests.get(script_info['url'], timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape}")
        
        # 現在のSEキャラクターデータを削除
        cursor.execute("""
            DELETE FROM character_dialogue_unified 
            WHERE script_id = (SELECT id FROM scripts WHERE management_id = ?)
            AND character_name = 'SE'
        """, (script_info['id'],))
        deleted = cursor.rowcount
        print(f"🗑️ 誤ったSEデータ削除: {deleted}件")
        
        # スクリプトIDを取得
        script_id_result = cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (script_info['id'],)).fetchone()
        if not script_id_result:
            print(f"❌ スクリプトID取得失敗")
            return 0
        
        script_id = script_id_result[0]
        
        # 正しいデータで再挿入（SEキャラクターを除外）
        inserted = 0
        char_col = script_info['char_col']
        dialogue_col = script_info['dialogue_col']
        
        # ヘッダー行をスキップ（行4から開始）
        start_row = 4
        
        for i in range(start_row, len(df)):
            char_name = str(df.iloc[i, char_col]) if pd.notna(df.iloc[i, char_col]) else ""
            dialogue = str(df.iloc[i, dialogue_col]) if pd.notna(df.iloc[i, dialogue_col]) else ""
            
            # 空のデータをスキップ
            if not char_name.strip() or not dialogue.strip() or char_name == "nan" or dialogue == "nan":
                continue
            
            # SEキャラクターをスキップ（実際のスプレッドシートには存在しない）
            if char_name == 'SE':
                continue
            
            # キャラクター名のクリーニング
            char_name = clean_character_name(char_name)
            
            # 明らかな指示データかチェック
            is_instruction = is_filming_instruction(char_name, dialogue)
            
            cursor.execute("""
                INSERT INTO character_dialogue_unified 
                (script_id, character_name, dialogue_text, row_number, is_instruction)
                VALUES (?, ?, ?, ?, ?)
            """, (script_id, char_name, dialogue, i + 1, is_instruction))
            
            inserted += 1
        
        print(f"✅ 正しいデータ挿入: {inserted}件")
        return deleted  # 削除した誤データ件数を返す
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        return 0

def clean_character_name(char_name):
    """キャラクター名のクリーニング"""
    char_name = char_name.strip()
    
    # 背景やセット情報を除外
    if char_name.startswith('背景：') or char_name.startswith('セット'):
        return None
    
    return char_name

def is_filming_instruction(char_name, dialogue):
    """撮影指示かどうかの判定"""
    if not char_name:
        return 1
    
    instruction_chars = ['FALSE', '[撮影指示]', '[話者不明]', 'アイキャッチ', 'CM', '5秒CM']
    
    if char_name in instruction_chars:
        return 1
    
    return 0

if __name__ == "__main__":
    fixed_count = fix_se_character_misextraction()
    print(f"\n✅ SEキャラクター誤抽出修正完了: {fixed_count}件")