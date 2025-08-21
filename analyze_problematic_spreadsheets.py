#!/usr/bin/env python3
"""
問題キャラクター（TRUE/SE/シーン）のスプレッドシート構造詳細分析
"""

import sqlite3
import pandas as pd
import requests
from io import StringIO
import re

def analyze_problematic_spreadsheets():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 問題キャラクターのスプレッドシート構造詳細分析")
    print("=" * 80)
    
    # 各問題キャラクターの上位スクリプトを特定
    problem_chars = ['TRUE', 'SE', 'シーン']
    
    for char in problem_chars:
        print(f"\n📌 {char} キャラクターの詳細分析")
        print("=" * 60)
        
        # 該当キャラクターが多いスクリプトを特定
        cursor.execute("""
            SELECT s.management_id, s.script_url, COUNT(*) as count
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 AND cdu.character_name = ?
            GROUP BY s.management_id, s.script_url
            ORDER BY count DESC
            LIMIT 3
        """, (char,))
        
        scripts = cursor.fetchall()
        
        for i, (management_id, script_url, count) in enumerate(scripts, 1):
            print(f"\n🎯 {char}キャラクター上位{i}位: {management_id} ({count}件)")
            print("-" * 50)
            
            # スプレッドシートの詳細構造分析
            analyze_single_spreadsheet(management_id, script_url, char, cursor)
    
    conn.close()

def analyze_single_spreadsheet(management_id, script_url, focus_char, cursor):
    """単一スプレッドシートの詳細構造分析"""
    
    # CSV URLに変換
    csv_url = convert_to_csv_url(script_url)
    print(f"📄 URL: {script_url}")
    print(f"📄 CSV: {csv_url}")
    
    try:
        # スプレッドシート取得
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        
        # UTF-8でデコード
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"📊 データ形状: {df.shape[0]}行 × {df.shape[1]}列")
        print(f"📋 列名: {list(df.columns)}")
        
        # ヘッダー行の検出
        print(f"\n🔍 ヘッダー構造分析:")
        for i in range(min(10, len(df))):
            row_preview = []
            for j in range(min(8, len(df.columns))):
                cell_value = str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""
                cell_short = cell_value[:12] + "..." if len(cell_value) > 12 else cell_value
                row_preview.append(f"[{j}]{cell_short}")
            print(f"  行{i+1:2d}: {' | '.join(row_preview)}")
        
        # 問題キャラクターの出現パターン分析
        print(f"\n🎯 {focus_char}キャラクターの出現分析:")
        
        # 各列での出現確認
        for col_idx in range(len(df.columns)):
            char_count = df.iloc[:, col_idx].astype(str).str.contains(focus_char, na=False, regex=False).sum()
            if char_count > 0:
                print(f"  列{col_idx}({df.columns[col_idx]}): {char_count}回出現")
                
                # サンプル表示
                char_rows = df[df.iloc[:, col_idx].astype(str).str.contains(focus_char, na=False, regex=False)]
                for idx, row in char_rows.head(3).iterrows():
                    adjacent_cols = []
                    for adj_col in range(max(0, col_idx-1), min(len(df.columns), col_idx+3)):
                        adj_value = str(row.iloc[adj_col]) if pd.notna(row.iloc[adj_col]) else ""
                        adj_short = adj_value[:20] + "..." if len(adj_value) > 20 else adj_value
                        adjacent_cols.append(f"[{adj_col}]{adj_short}")
                    print(f"    行{idx+1}: {' | '.join(adjacent_cols)}")
        
        # データベースでの登録状況と比較
        print(f"\n📊 データベース登録状況:")
        cursor.execute("""
            SELECT cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE s.management_id = ? AND cdu.character_name = ?
            ORDER BY cdu.row_number
            LIMIT 5
        """, (management_id, focus_char))
        
        db_samples = cursor.fetchall()
        for char_name, dialogue, row_num in db_samples:
            row_str = f"{row_num:3d}" if row_num is not None else "---"
            dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
            print(f"  DB行{row_str}: {char_name} | \"{dialogue_short}\"")
        
        # 構造の問題点分析
        print(f"\n⚠️ 構造問題の分析:")
        analyze_structure_issues(df, focus_char, management_id)
        
    except Exception as e:
        print(f"❌ エラー: {e}")

def convert_to_csv_url(edit_url):
    """Google Sheets編集URLをCSV URLに変換"""
    if 'edit#gid=' in edit_url:
        base_url = edit_url.split('/edit#gid=')[0]
        gid = edit_url.split('gid=')[1]
        return f"{base_url}/export?format=csv&gid={gid}"
    else:
        base_url = edit_url.split('/edit')[0]
        return f"{base_url}/export?format=csv&gid=0"

def analyze_structure_issues(df, focus_char, management_id):
    """構造問題の分析"""
    
    issues = []
    
    # 1. ヘッダー位置の問題
    header_found = False
    for i in range(min(5, len(df))):
        for j in range(len(df.columns)):
            cell_value = str(df.iloc[i, j]).lower()
            if 'キャラクター' in cell_value or 'character' in cell_value:
                header_found = True
                print(f"  ✅ ヘッダー発見: 行{i+1}, 列{j+1}")
                break
        if header_found:
            break
    
    if not header_found:
        issues.append("ヘッダー行が不明確")
    
    # 2. キャラクター列の特定
    main_characters = ['サンサン', 'くもりん', 'ツクモ', 'プリル', 'ノイズ']
    char_col_candidates = []
    
    for col_idx in range(len(df.columns)):
        char_score = 0
        for char in main_characters:
            char_score += df.iloc[:, col_idx].astype(str).str.contains(char, na=False).sum()
        if char_score > 0:
            char_col_candidates.append((col_idx, char_score))
    
    char_col_candidates.sort(key=lambda x: x[1], reverse=True)
    
    if char_col_candidates:
        best_char_col = char_col_candidates[0][0]
        print(f"  🎯 推定キャラクター列: {best_char_col} (スコア: {char_col_candidates[0][1]})")
        
        # 3. 問題キャラクターがキャラクター列にあるかチェック
        focus_in_char_col = df.iloc[:, best_char_col].astype(str).str.contains(focus_char, na=False, regex=False).sum()
        print(f"  📊 {focus_char}のキャラクター列出現: {focus_in_char_col}回")
        
        # 4. セリフ列の推定
        dialogue_col = best_char_col + 1
        if dialogue_col < len(df.columns):
            focus_in_dialogue_col = df.iloc[:, dialogue_col].astype(str).str.contains(focus_char, na=False, regex=False).sum()
            print(f"  📊 {focus_char}のセリフ列出現: {focus_in_dialogue_col}回")
            
            if focus_in_dialogue_col > focus_in_char_col:
                issues.append(f"{focus_char}がセリフ列に多く出現 - 列構造の問題可能性")
        
        # 5. 実際のセリフかどうかの判定
        if focus_char == 'TRUE':
            analyze_true_character_content(df, best_char_col, dialogue_col)
        elif focus_char == 'SE':
            analyze_se_character_content(df, best_char_col, dialogue_col)
        elif focus_char == 'シーン':
            analyze_scene_character_content(df, best_char_col, dialogue_col)
    
    else:
        issues.append("キャラクター列が特定できない")
    
    # 問題点の総括
    if issues:
        print(f"  ⚠️ 発見された問題:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"  ✅ 構造上の明確な問題は発見されませんでした")

def analyze_true_character_content(df, char_col, dialogue_col):
    """TRUEキャラクターの内容分析"""
    true_rows = df[df.iloc[:, char_col].astype(str).str.contains('TRUE', na=False, regex=False)]
    
    if len(true_rows) > 0:
        print(f"  🔍 TRUE内容分析 ({len(true_rows)}件):")
        
        # ナレーション的内容かセリフかを判定
        narrative_patterns = [
            r'むかし', r'昔々', r'ある日', r'そして', r'それから',
            r'でした', r'ました', r'のです', r'^.*たち.*',
            r'物語', r'お話', r'はじまり'
        ]
        
        dialogue_patterns = [
            r'[！？!?]$', r'^.*[だよね]！', r'^.*[です]！',
            r'「.*」', r'わー', r'やー', r'そう'
        ]
        
        narrative_count = 0
        dialogue_count = 0
        
        for _, row in true_rows.head(5).iterrows():
            content = str(row.iloc[dialogue_col]) if pd.notna(row.iloc[dialogue_col]) else ""
            
            is_narrative = any(re.search(pattern, content) for pattern in narrative_patterns)
            is_dialogue = any(re.search(pattern, content) for pattern in dialogue_patterns)
            
            if is_narrative:
                narrative_count += 1
                content_type = "ナレーション"
            elif is_dialogue:
                dialogue_count += 1
                content_type = "セリフ"
            else:
                content_type = "不明"
            
            content_short = content[:40] + "..." if len(content) > 40 else content
            print(f"    {content_type}: \"{content_short}\"")
        
        print(f"  📊 TRUE内容判定: ナレーション{narrative_count}件, セリフ{dialogue_count}件")

def analyze_se_character_content(df, char_col, dialogue_col):
    """SEキャラクターの内容分析"""
    se_rows = df[df.iloc[:, char_col].astype(str).str.contains('SE', na=False, regex=False)]
    
    if len(se_rows) > 0:
        print(f"  🔍 SE内容分析 ({len(se_rows)}件):")
        
        # 効果音指示かセリフかを判定
        sound_patterns = [
            r'^\d+$', r'効果音', r'SE', r'BGM', r'音',
            r'ドン', r'バン', r'ガシャ', r'ピロリン'
        ]
        
        dialogue_patterns = [
            r'[！？!?]$', r'「.*」', r'^.*と.*',
            r'する', r'です', r'だ', r'ます'
        ]
        
        sound_count = 0
        dialogue_count = 0
        
        for _, row in se_rows.head(5).iterrows():
            content = str(row.iloc[dialogue_col]) if pd.notna(row.iloc[dialogue_col]) else ""
            
            is_sound = any(re.search(pattern, content) for pattern in sound_patterns)
            is_dialogue = any(re.search(pattern, content) for pattern in dialogue_patterns)
            
            if is_sound:
                sound_count += 1
                content_type = "効果音指示"
            elif is_dialogue:
                dialogue_count += 1
                content_type = "セリフ/説明"
            else:
                content_type = "不明"
            
            content_short = content[:40] + "..." if len(content) > 40 else content
            print(f"    {content_type}: \"{content_short}\"")
        
        print(f"  📊 SE内容判定: 効果音指示{sound_count}件, セリフ/説明{dialogue_count}件")

def analyze_scene_character_content(df, char_col, dialogue_col):
    """シーンキャラクターの内容分析"""
    scene_rows = df[df.iloc[:, char_col].astype(str).str.contains('シーン', na=False, regex=False)]
    
    if len(scene_rows) > 0:
        print(f"  🔍 シーン内容分析 ({len(scene_rows)}件):")
        
        # 技術指示か場面説明かを判定
        tech_patterns = [
            r'テロップ', r'カット', r'アングル', r'ズーム',
            r'エフェクト', r'BGM', r'SE', r'編集', r'調整'
        ]
        
        scene_patterns = [
            r'する', r'である', r'います', r'ました',
            r'様子', r'状態', r'姿', r'表情'
        ]
        
        tech_count = 0
        scene_count = 0
        
        for _, row in scene_rows.head(5).iterrows():
            content = str(row.iloc[dialogue_col]) if pd.notna(row.iloc[dialogue_col]) else ""
            
            is_tech = any(re.search(pattern, content) for pattern in tech_patterns)
            is_scene = any(re.search(pattern, content) for pattern in scene_patterns)
            
            if is_tech:
                tech_count += 1
                content_type = "技術指示"
            elif is_scene:
                scene_count += 1
                content_type = "場面説明"
            else:
                content_type = "不明"
            
            content_short = content[:40] + "..." if len(content) > 40 else content
            print(f"    {content_type}: \"{content_short}\"")
        
        print(f"  📊 シーン内容判定: 技術指示{tech_count}件, 場面説明{scene_count}件")

if __name__ == "__main__":
    analyze_problematic_spreadsheets()