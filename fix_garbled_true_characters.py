#!/usr/bin/env python3
"""
文字化けしたTRUEキャラクターの修正
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO

def fix_garbled_true_characters():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔤 文字化けしたTRUEキャラクターの修正開始")
    print("=" * 80)
    
    # TRUEキャラクターが多いスクリプトを特定
    cursor.execute("""
        SELECT s.management_id, s.script_url, COUNT(*) as count
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 0 AND cdu.character_name = 'TRUE'
        GROUP BY s.management_id, s.script_url
        ORDER BY count DESC
        LIMIT 10
    """)
    
    true_scripts = cursor.fetchall()
    
    total_fixed = 0
    
    for management_id, script_url, count in true_scripts:
        print(f"\n🔍 {management_id} の修正処理 ({count}件)")
        print("-" * 60)
        
        # CSV URLに変換
        csv_url = convert_to_csv_url(script_url)
        
        # 修正処理実行
        fixed_count = fix_single_true_script(management_id, csv_url, cursor)
        total_fixed += fixed_count
        
        print(f"✅ {management_id}: {fixed_count}件修正")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎯 総修正件数: {total_fixed}件")
    return total_fixed

def convert_to_csv_url(edit_url):
    """Google Sheets編集URLをCSV URLに変換"""
    if 'edit#gid=' in edit_url:
        base_url = edit_url.split('/edit#gid=')[0]
        gid = edit_url.split('gid=')[1]
        return f"{base_url}/export?format=csv&gid={gid}"
    else:
        base_url = edit_url.split('/edit')[0]
        return f"{base_url}/export?format=csv&gid=0"

def fix_single_true_script(management_id, csv_url, cursor):
    """単一スクリプトのTRUE文字化け修正"""
    
    try:
        print(f"📡 スプレッドシート取得中...")
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape}")
        
        # ヘッダー検出
        char_col, dialogue_col = detect_character_dialogue_columns(df)
        
        if char_col is None or dialogue_col is None:
            print(f"❌ ヘッダー検出失敗")
            return 0
        
        print(f"🎯 キャラクター列: {char_col}, セリフ列: {dialogue_col}")
        
        # 現在のTRUEキャラクターデータを削除
        cursor.execute("""
            DELETE FROM character_dialogue_unified 
            WHERE script_id = (SELECT id FROM scripts WHERE management_id = ?)
            AND character_name = 'TRUE'
        """, (management_id,))
        deleted = cursor.rowcount
        print(f"🗑️ TRUEデータ削除: {deleted}件")
        
        # スクリプトIDを取得
        script_id_result = cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (management_id,)).fetchone()
        if not script_id_result:
            print(f"❌ スクリプトID取得失敗")
            return 0
        
        script_id = script_id_result[0]
        
        # 正しいデータで再挿入
        inserted = 0
        for i in range(len(df)):
            char_name = str(df.iloc[i, char_col]) if pd.notna(df.iloc[i, char_col]) else ""
            dialogue = str(df.iloc[i, dialogue_col]) if pd.notna(df.iloc[i, dialogue_col]) else ""
            
            # 空のデータをスキップ
            if not char_name.strip() or not dialogue.strip() or char_name == "nan" or dialogue == "nan":
                continue
            
            # キャラクター名のクリーニング（文字化け修正）
            original_char = char_name
            char_name = clean_character_name(char_name)
            
            # TRUEが文字化けしていた場合の処理
            if original_char != char_name:
                print(f"  🔤 文字化け修正: '{original_char}' → '{char_name}'")
            
            # 明らかな指示データかチェック
            is_instruction = is_filming_instruction(char_name, dialogue)
            
            # 元々TRUEだったもの、または文字化けから修正されたもののみ挿入
            garbled_patterns = ['ãµã³ãµã³', 'ãããã', 'ããããã', 'ãããã«', 'ããã¤ãº']
            is_originally_true = (original_char == 'TRUE' or 
                                any(pattern in original_char for pattern in garbled_patterns))
            
            if is_originally_true:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, character_name, dialogue_text, row_number, is_instruction)
                    VALUES (?, ?, ?, ?, ?)
                """, (script_id, char_name, dialogue, i + 1, is_instruction))
                
                inserted += 1
        
        print(f"✅ 新規データ挿入: {inserted}件")
        return inserted
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        return 0

def detect_character_dialogue_columns(df):
    """キャラクター列とセリフ列を検出"""
    
    # パターン1: 明確なヘッダー
    for i in range(min(5, len(df))):
        for j in range(len(df.columns)):
            cell_value = str(df.iloc[i, j]).strip().lower()
            if 'キャラクター' in cell_value or 'character' in cell_value:
                for k in range(j+1, min(j+3, len(df.columns))):
                    next_cell = str(df.iloc[i, k]).strip().lower()
                    if 'セリフ' in next_cell or 'dialogue' in next_cell or 'せりふ' in next_cell:
                        return j, k
    
    # パターン2: メインキャラクターの出現頻度で判定
    main_characters = ['サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ']
    garbled_characters = ['ãµã³ãµã³', 'ãããã', 'ããããã', 'ãããã«', 'ããã¤ãº']
    all_chars = main_characters + garbled_characters
    
    char_scores = []
    for col_idx in range(min(6, len(df.columns))):
        score = 0
        for char in all_chars:
            count = df.iloc[:, col_idx].astype(str).str.contains(char, na=False).sum()
            score += count
        char_scores.append((col_idx, score))
    
    char_scores.sort(key=lambda x: x[1], reverse=True)
    
    if char_scores[0][1] > 0:
        char_col = char_scores[0][0]
        dialogue_col = char_col + 1
        
        if dialogue_col < len(df.columns):
            return char_col, dialogue_col
    
    # デフォルト: 2列目がキャラクター、3列目がセリフ
    return 2, 3

def clean_character_name(char_name):
    """キャラクター名のクリーニング（文字化け修正）"""
    char_name = char_name.strip()
    
    # 文字化け修正マッピング
    char_mapping = {
        'ãµã³ãµã³': 'サンサン',
        'ãããã': 'くもりん', 
        'ããããã': 'ツクモ',
        'ãããã«': 'プリル',
        'ããã¤ãº': 'ノイズ',
        'ïïï': 'BB',
        'ãã': 'の'
    }
    
    for garbled, correct in char_mapping.items():
        if garbled in char_name:
            char_name = char_name.replace(garbled, correct)
    
    return char_name

def is_filming_instruction(char_name, dialogue):
    """撮影指示かどうかの判定"""
    instruction_chars = ['FALSE', '[撮影指示]', '[話者不明]', 'アイキャッチ', 'CM', '5秒CM', 'BGM', '効果音']
    
    if char_name in instruction_chars:
        return 1
    
    return 0

if __name__ == "__main__":
    fixed_count = fix_garbled_true_characters()
    print(f"\n✅ 文字化けTRUEキャラクター修正完了: {fixed_count}件")