#!/usr/bin/env python3
"""
TRUEキャラクター問題スクリプトの個別修正
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO

def fix_true_character_scripts():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔧 TRUEキャラクター問題スクリプトの個別修正")
    print("=" * 80)
    
    # TRUEキャラクターの上位スクリプトを取得（正しいgid付き）
    true_scripts = [
        {
            'id': 'B2142',
            'url': 'https://docs.google.com/spreadsheets/d/1q7gpiEvfAL2fYIGtqddJPtwwdKuTAanurTbQr8wRXqg/export?format=csv&gid=975044043',
            'count': 346
        },
        {
            'id': 'B2066', 
            'url': 'https://docs.google.com/spreadsheets/d/1NYC66KMQFm06Q-WHW25_a-YuIq97PHVp7AQzjILS61A/export?format=csv&gid=1115519680',
            'count': 214
        },
        {
            'id': 'B1582',
            'url': 'https://docs.google.com/spreadsheets/d/1khxQzcM-CxY3_HusiXXB4nOuGaplyw_Hvg036OYVe94/export?format=csv&gid=100444062',
            'count': 199
        }
    ]
    
    total_fixed = 0
    
    for script_info in true_scripts:
        print(f"\n🎯 {script_info['id']} の修正処理 ({script_info['count']}件)")
        print("-" * 60)
        
        # 個別修正処理
        fixed_count = fix_single_true_script(script_info, cursor)
        total_fixed += fixed_count
        
        print(f"✅ {script_info['id']}: {fixed_count}件修正")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎯 TRUE修正完了: {total_fixed}件")
    return total_fixed

def fix_single_true_script(script_info, cursor):
    """単一スクリプトのTRUE修正"""
    
    try:
        print(f"📡 スプレッドシート取得中...")
        response = requests.get(script_info['url'], timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape}")
        print(f"📋 列名: {list(df.columns)}")
        
        # 構造分析
        char_col, dialogue_col = analyze_true_spreadsheet_structure(df, script_info['id'])
        
        if char_col is None or dialogue_col is None:
            print(f"❌ 構造分析失敗")
            return 0
        
        print(f"🎯 推定構造: キャラクター列{char_col}, セリフ列{dialogue_col}")
        
        # 現在のTRUEキャラクターデータの内容確認
        cursor.execute("""
            SELECT cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE s.management_id = ? AND cdu.character_name = 'TRUE'
            ORDER BY cdu.row_number
            LIMIT 5
        """, (script_info['id'],))
        
        true_samples = cursor.fetchall()
        print(f"🔍 現在のTRUEデータサンプル:")
        for dialogue, row in true_samples:
            dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
            row_str = f"{row:3d}" if row is not None else "---"
            print(f"  行{row_str}: \"{dialogue_short}\"")
        
        # TRUEキャラクターがナレーションかセリフかを分析
        narrative_count, dialogue_count = analyze_true_content(df, char_col, dialogue_col)
        print(f"📊 TRUE内容分析: ナレーション{narrative_count}件, セリフ{dialogue_count}件")
        
        # TRUEキャラクターの修正（ナレーションとして適切に分類）
        fixed_count = correct_true_character(script_info['id'], df, char_col, dialogue_col, cursor)
        
        return fixed_count
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return 0

def analyze_true_spreadsheet_structure(df, management_id):
    """TRUEスプレッドシート構造の分析"""
    
    # ヘッダー行の検出
    header_row = None
    char_col = None
    dialogue_col = None
    
    # 最初の10行でヘッダーを探す
    for i in range(min(10, len(df))):
        for j in range(len(df.columns)):
            cell_value = str(df.iloc[i, j]).lower()
            if 'キャラクター' in cell_value or 'character' in cell_value:
                header_row = i
                char_col = j
                # セリフ列を探す
                for k in range(j+1, min(j+4, len(df.columns))):
                    next_cell = str(df.iloc[i, k]).lower()
                    if 'セリフ' in next_cell or next_cell == '' or 'dialogue' in next_cell:
                        dialogue_col = k
                        break
                break
        if header_row is not None:
            break
    
    if header_row is not None and char_col is not None and dialogue_col is not None:
        print(f"✅ ヘッダー行発見: 行{header_row + 1}, キャラクター列{char_col}, セリフ列{dialogue_col}")
        return char_col, dialogue_col
    
    # ヘッダーが見つからない場合、メインキャラクターの出現頻度で判定
    main_characters = ['サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ', 'TRUE']
    
    char_scores = []
    for col_idx in range(min(8, len(df.columns))):
        score = 0
        for char in main_characters:
            count = df.iloc[:, col_idx].astype(str).str.contains(char, na=False, regex=False).sum()
            score += count
        char_scores.append((col_idx, score))
        
        # TRUEキャラクターの出現状況を表示
        true_count = df.iloc[:, col_idx].astype(str).str.contains('TRUE', na=False, regex=False).sum()
        if true_count > 0:
            print(f"  列{col_idx}: TRUE {true_count}回出現")
    
    char_scores.sort(key=lambda x: x[1], reverse=True)
    
    if char_scores[0][1] > 0:
        char_col = char_scores[0][0]
        dialogue_col = char_col + 1
        
        if dialogue_col < len(df.columns):
            return char_col, dialogue_col
    
    print(f"⚠️ 構造推定失敗、デフォルト値使用")
    return 2, 3  # デフォルト値

def analyze_true_content(df, char_col, dialogue_col):
    """TRUEキャラクターの内容分析"""
    
    # TRUEキャラクターの行を抽出
    true_rows = df[df.iloc[:, char_col].astype(str).str.contains('TRUE', na=False, regex=False)]
    
    narrative_patterns = [
        r'むかし', r'昔々', r'ある日', r'そして', r'それから', r'でした', r'ました', r'のです',
        r'物語', r'お話', r'はじまり', r'おしまい', r'めでたし'
    ]
    
    dialogue_patterns = [
        r'[！？!?]$', r'^.*[だよね]！', r'^.*です！', r'「.*」', r'わー', r'やー'
    ]
    
    narrative_count = 0
    dialogue_count = 0
    
    for _, row in true_rows.iterrows():
        content = str(row.iloc[dialogue_col]) if pd.notna(row.iloc[dialogue_col]) else ""
        
        is_narrative = any(re.search(pattern, content) for pattern in narrative_patterns)
        is_dialogue = any(re.search(pattern, content) for pattern in dialogue_patterns)
        
        if is_narrative:
            narrative_count += 1
        elif is_dialogue:
            dialogue_count += 1
    
    return narrative_count, dialogue_count

def correct_true_character(management_id, df, char_col, dialogue_col, cursor):
    """TRUEキャラクターの修正"""
    
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
    
    # 正しいデータで再挿入（TRUEはナレーションとして扱う）
    inserted = 0
    
    # ヘッダー行をスキップして開始
    start_row = 4
    
    for i in range(start_row, len(df)):
        char_name = str(df.iloc[i, char_col]) if pd.notna(df.iloc[i, char_col]) else ""
        dialogue = str(df.iloc[i, dialogue_col]) if pd.notna(df.iloc[i, dialogue_col]) else ""
        
        # 空のデータをスキップ
        if not char_name.strip() or not dialogue.strip() or char_name == "nan" or dialogue == "nan":
            continue
        
        # TRUEキャラクターをナレーションに変更
        if char_name == 'TRUE':
            char_name = 'ナレーション'
        
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
    
    print(f"✅ 正しいデータ挿入: {inserted}件（TRUEをナレーションに変更）")
    return deleted

def clean_character_name(char_name):
    """キャラクター名のクリーニング"""
    char_name = char_name.strip()
    
    # 無効なキャラクター名を除外
    if not char_name or char_name == "nan":
        return None
    
    return char_name

def is_filming_instruction(char_name, dialogue):
    """撮影指示かどうかの判定"""
    instruction_chars = ['FALSE', '[撮影指示]', '[話者不明]', 'アイキャッチ', 'CM', '5秒CM']
    
    if char_name in instruction_chars:
        return 1
    
    return 0

import re

if __name__ == "__main__":
    fixed_count = fix_true_character_scripts()
    print(f"\n✅ TRUEキャラクター修正完了: {fixed_count}件")