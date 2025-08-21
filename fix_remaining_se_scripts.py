#!/usr/bin/env python3
"""
残存SEキャラクター問題スクリプトの個別修正
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO

def fix_remaining_se_scripts():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 残存SEキャラクター問題スクリプトの個別修正")
    print("=" * 80)
    
    # 残存SEキャラクターの上位スクリプトを取得
    cursor.execute("""
        SELECT s.management_id, s.script_url, COUNT(*) as count
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.character_name = 'SE' AND cdu.is_instruction = 0
        GROUP BY s.management_id, s.script_url
        ORDER BY count DESC
        LIMIT 5
    """)
    
    remaining_scripts = cursor.fetchall()
    
    total_fixed = 0
    
    for management_id, script_url, count in remaining_scripts:
        print(f"\n🎯 {management_id} の修正処理 ({count}件)")
        print("-" * 60)
        
        # 個別修正処理
        fixed_count = fix_single_remaining_se_script(management_id, script_url, cursor)
        total_fixed += fixed_count
        
        print(f"✅ {management_id}: {fixed_count}件修正")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎯 残存SE修正完了: {total_fixed}件")
    return total_fixed

def fix_single_remaining_se_script(management_id, script_url, cursor):
    """単一スクリプトの残存SE修正"""
    
    # CSV URLに変換
    csv_url = convert_to_csv_url(script_url)
    print(f"📄 URL: {script_url}")
    print(f"📄 CSV: {csv_url}")
    
    try:
        print(f"📡 スプレッドシート取得中...")
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape}")
        
        # 構造分析
        char_col, dialogue_col = analyze_spreadsheet_structure(df, management_id)
        
        if char_col is None or dialogue_col is None:
            print(f"❌ 構造分析失敗")
            return 0
        
        print(f"🎯 推定構造: キャラクター列{char_col}, セリフ列{dialogue_col}")
        
        # 現在のSEキャラクターデータの内容確認
        cursor.execute("""
            SELECT cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE s.management_id = ? AND cdu.character_name = 'SE'
            ORDER BY cdu.row_number
            LIMIT 5
        """, (management_id,))
        
        se_samples = cursor.fetchall()
        print(f"🔍 現在のSEデータサンプル:")
        for dialogue, row in se_samples:
            dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
            row_str = f"{row:3d}" if row is not None else "---"
            print(f"  行{row_str}: \"{dialogue_short}\"")
        
        # SEキャラクターの誤抽出を修正
        fixed_count = correct_se_misextraction(management_id, df, char_col, dialogue_col, cursor)
        
        return fixed_count
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        return 0

def convert_to_csv_url(edit_url):
    """Google Sheets編集URLをCSV URLに変換"""
    if 'edit#gid=' in edit_url:
        base_url = edit_url.split('/edit#gid=')[0]
        gid = edit_url.split('gid=')[1]
        return f"{base_url}/export?format=csv&gid={gid}"
    else:
        base_url = edit_url.split('/edit')[0]
        return f"{base_url}/export?format=csv&gid=0"

def analyze_spreadsheet_structure(df, management_id):
    """スプレッドシート構造の分析"""
    
    # ヘッダー行の検出
    header_row = None
    for i in range(min(10, len(df))):
        for j in range(len(df.columns)):
            cell_value = str(df.iloc[i, j]).lower()
            if 'キャラクター' in cell_value or 'character' in cell_value:
                header_row = i
                break
        if header_row is not None:
            break
    
    if header_row is not None:
        print(f"✅ ヘッダー行発見: 行{header_row + 1}")
        
        # ヘッダー行からキャラクター列とセリフ列を特定
        for j in range(len(df.columns)):
            cell_value = str(df.iloc[header_row, j]).lower()
            if 'キャラクター' in cell_value:
                char_col = j
                # セリフ列は通常キャラクター列の隣
                for k in range(j+1, min(j+3, len(df.columns))):
                    next_cell = str(df.iloc[header_row, k]).lower()
                    if 'セリフ' in next_cell or len(next_cell) == 0:  # セリフ列または無名列
                        dialogue_col = k
                        return char_col, dialogue_col
    
    # フォールバック: メインキャラクターの出現頻度で判定
    main_characters = ['サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ', 'シーン']
    
    char_scores = []
    for col_idx in range(min(8, len(df.columns))):
        score = 0
        for char in main_characters:
            count = df.iloc[:, col_idx].astype(str).str.contains(char, na=False).sum()
            score += count
        char_scores.append((col_idx, score))
    
    char_scores.sort(key=lambda x: x[1], reverse=True)
    
    if char_scores[0][1] > 0:
        char_col = char_scores[0][0]
        dialogue_col = char_col + 1
        
        if dialogue_col < len(df.columns):
            return char_col, dialogue_col
    
    return None, None

def correct_se_misextraction(management_id, df, char_col, dialogue_col, cursor):
    """SEキャラクターの誤抽出修正"""
    
    # 現在のSEキャラクターデータを削除
    cursor.execute("""
        DELETE FROM character_dialogue_unified 
        WHERE script_id = (SELECT id FROM scripts WHERE management_id = ?)
        AND character_name = 'SE'
    """, (management_id,))
    deleted = cursor.rowcount
    print(f"🗑️ 誤ったSEデータ削除: {deleted}件")
    
    # スクリプトIDを取得
    script_id_result = cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (management_id,)).fetchone()
    if not script_id_result:
        print(f"❌ スクリプトID取得失敗")
        return 0
    
    script_id = script_id_result[0]
    
    # 正しいデータで再挿入
    inserted = 0
    
    # ヘッダー行をスキップして開始
    start_row = 4
    
    for i in range(start_row, len(df)):
        char_name = str(df.iloc[i, char_col]) if pd.notna(df.iloc[i, char_col]) else ""
        dialogue = str(df.iloc[i, dialogue_col]) if pd.notna(df.iloc[i, dialogue_col]) else ""
        
        # 空のデータをスキップ
        if not char_name.strip() or not dialogue.strip() or char_name == "nan" or dialogue == "nan":
            continue
        
        # 背景やセット情報をスキップ
        if char_name.startswith('背景：') or char_name.startswith('セット'):
            continue
        
        # キャラクター名のクリーニング
        char_name = clean_character_name(char_name)
        if not char_name:
            continue
        
        # 明らかな指示データかチェック
        is_instruction = is_filming_instruction(char_name, dialogue)
        
        cursor.execute("""
            INSERT INTO character_dialogue_unified 
            (script_id, character_name, dialogue_text, row_number, is_instruction)
            VALUES (?, ?, ?, ?, ?)
        """, (script_id, char_name, dialogue, i + 1, is_instruction))
        
        inserted += 1
    
    print(f"✅ 正しいデータ挿入: {inserted}件")
    return deleted

def clean_character_name(char_name):
    """キャラクター名のクリーニング"""
    char_name = char_name.strip()
    
    # 無効なキャラクター名を除外
    if not char_name or char_name == "nan":
        return None
    
    # 背景やセット情報を除外
    if char_name.startswith('背景：') or char_name.startswith('セット'):
        return None
    
    return char_name

def is_filming_instruction(char_name, dialogue):
    """撮影指示かどうかの判定"""
    instruction_chars = ['FALSE', '[撮影指示]', '[話者不明]', 'アイキャッチ', 'CM', '5秒CM']
    
    if char_name in instruction_chars:
        return 1
    
    return 0

if __name__ == "__main__":
    fixed_count = fix_remaining_se_scripts()
    print(f"\n✅ 残存SEキャラクター修正完了: {fixed_count}件")