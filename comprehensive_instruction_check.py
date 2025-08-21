#!/usr/bin/env python3
"""
音声指示など全指示タイプの包括的チェック
"""

import sqlite3
import re

def comprehensive_instruction_check():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 音声指示など全指示タイプの包括的チェック開始")
    print("=" * 80)
    
    # 1. 現在のフラグ設定状況確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    print(f"📊 現在の状況:")
    print(f"  指示データ (is_instruction=1): {current_instruction_count}件")
    print(f"  セリフデータ (is_instruction=0): {current_dialogue_count}件")
    print()
    
    # 2. 音声指示系のパターンをチェック
    audio_patterns = [
        '%音声%', '%ボイス%', '%声%', '%SE%', '%BGM%', '%効果音%', 
        '%サウンド%', '%音%', '%ナレーション%', '%ナレ%',
        '%マイク%', '%録音%', '%音量%', '%音質%'
    ]
    
    print("🎵 音声指示系パターンのチェック:")
    print("-" * 50)
    
    missed_audio_instructions = []
    for pattern in audio_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            LIMIT 10
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    {management_id} 行{row} | {char_short} | \"{dialogue_short}\"")
                missed_audio_instructions.append((management_id, char_name, dialogue, row, pattern))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 3. 編集指示系のパターンをチェック
    editing_patterns = [
        '%編集%', '%カット%', '%エフェクト%', '%テロップ%', '%字幕%',
        '%フェード%', '%ワイプ%', '%トランジション%', '%合成%', '%CG%',
        '%エンコード%', '%レンダリング%', '%出力%'
    ]
    
    print("✂️  編集指示系パターンのチェック:")
    print("-" * 50)
    
    missed_editing_instructions = []
    for pattern in editing_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            LIMIT 10
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    {management_id} 行{row} | {char_short} | \"{dialogue_short}\"")
                missed_editing_instructions.append((management_id, char_name, dialogue, row, pattern))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 4. 演出指示系のパターンをチェック
    direction_patterns = [
        '%演出%', '%ライティング%', '%照明%', '%カメラ%', '%アングル%',
        '%ズーム%', '%パン%', '%チルト%', '%フォーカス%', '%露出%',
        '%ホワイトバランス%', '%色調%', '%明度%', '%コントラスト%'
    ]
    
    print("🎬 演出指示系パターンのチェック:")
    print("-" * 50)
    
    missed_direction_instructions = []
    for pattern in direction_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            LIMIT 10
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    {management_id} 行{row} | {char_short} | \"{dialogue_short}\"")
                missed_direction_instructions.append((management_id, char_name, dialogue, row, pattern))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 5. 制作指示系のパターンをチェック
    production_patterns = [
        '%制作%', '%プロデューサー%', '%ディレクター%', '%スタッフ%',
        '%スケジュール%', '%予算%', '%リテイク%', '%確認%', '%承認%',
        '%チェック%', '%修正%', '%変更%', '%追加%', '%削除%'
    ]
    
    print("📋 制作指示系パターンのチェック:")
    print("-" * 50)
    
    missed_production_instructions = []
    for pattern in production_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB')
            LIMIT 10
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    {management_id} 行{row} | {char_short} | \"{dialogue_short}\"")
                missed_production_instructions.append((management_id, char_name, dialogue, row, pattern))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 6. 角括弧や記号で囲まれた指示系
    print("📝 角括弧・記号で囲まれた指示系のチェック:")
    print("-" * 50)
    
    bracket_patterns = [
        r'\[.*\]',      # [指示内容]
        r'\（.*指示.*\）',  # （指示内容）
        r'\※.*',        # ※印で始まる
        r'^★.*',        # ★で始まる
        r'^●.*',        # ●で始まる
        r'^■.*',        # ■で始まる
        r'^【.*】$',     # 【】で囲まれた
    ]
    
    missed_bracket_instructions = []
    for pattern in bracket_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name REGEXP ? OR cdu.dialogue_text REGEXP ?)
            LIMIT 10
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  正規表現 '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:3]:
                char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
                dialogue_short = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                print(f"    {management_id} 行{row} | {char_short} | \"{dialogue_short}\"")
                missed_bracket_instructions.append((management_id, char_name, dialogue, row, pattern))
            if len(matches) > 3:
                print(f"    ... 他{len(matches) - 3}件")
            print()
    
    # 7. 総計と提案
    total_missed = (len(missed_audio_instructions) + len(missed_editing_instructions) + 
                   len(missed_direction_instructions) + len(missed_production_instructions) + 
                   len(missed_bracket_instructions))
    
    print("=" * 80)
    print(f"📊 包括的チェック結果:")
    print(f"  音声指示系未フラグ: {len(missed_audio_instructions)}件")
    print(f"  編集指示系未フラグ: {len(missed_editing_instructions)}件") 
    print(f"  演出指示系未フラグ: {len(missed_direction_instructions)}件")
    print(f"  制作指示系未フラグ: {len(missed_production_instructions)}件")
    print(f"  記号囲み指示未フラグ: {len(missed_bracket_instructions)}件")
    print(f"  総未フラグ件数: {total_missed}件")
    
    if total_missed > 0:
        print(f"\n💡 追加のフラグ設定を実行して指示データを完全に除外することを推奨します")
    else:
        print(f"\n✅ 全指示タイプが適切にフラグ設定されています")
    
    conn.close()
    return total_missed

if __name__ == "__main__":
    comprehensive_instruction_check()