#!/usr/bin/env python3
"""
音声指示など全指示タイプの包括的チェック（修正版）
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
    
    all_missed_instructions = []
    
    # 2. 角括弧系の指示チェック（最も確実な指示）
    print("📝 角括弧・記号系指示のチェック:")
    print("-" * 50)
    
    bracket_keywords = ['[%]', '（%指示%）', '【%】', '★%', '●%', '■%']
    
    for pattern in bracket_keywords:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            LIMIT 15
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    {management_id} 行{row:3d} | {char_short:30s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"角括弧系:{pattern}"))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 3. 技術系指示のチェック
    print("🔧 技術系指示のチェック:")
    print("-" * 50)
    
    technical_patterns = [
        '%BGM%', '%SE%', '%効果音%', '%音声%', '%ボイス%', '%サウンド%', 
        '%編集%', '%カット%', '%エフェクト%', '%テロップ%', '%字幕%',
        '%フェード%', '%ワイプ%', '%トランジション%', '%演出%', '%照明%',
        '%カメラ%', '%アングル%', '%ズーム%', '%フォーカス%', '%明度%'
    ]
    
    for pattern in technical_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
            LIMIT 8
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:3]:
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    {management_id} 行{row:3d} | {char_short:30s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"技術系:{pattern}"))
            if len(matches) > 3:
                print(f"    ... 他{len(matches) - 3}件")
            print()
    
    # 4. ナレーション系のチェック
    print("🎙️ ナレーション系のチェック:")
    print("-" * 50)
    
    narration_patterns = ['%ナレーション%', '%ナレ%']
    
    for pattern in narration_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND cdu.character_name LIKE ?
            LIMIT 8
        """, (pattern,))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    {management_id} 行{row:3d} | {char_short:30s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"ナレ系:{pattern}"))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 5. 制作系指示のチェック（キャラクター以外）
    print("📋 制作系指示のチェック:")
    print("-" * 50)
    
    production_patterns = [
        '%制作%', '%スタッフ%', '%確認%', '%チェック%', '%修正%', '%追加%',
        '%スケジュール%', '%リテイク%', '%承認%', '%変更%', '%削除%'
    ]
    
    for pattern in production_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
            LIMIT 5
        """, (pattern, pattern))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  パターン '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:3]:
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    {management_id} 行{row:3d} | {char_short:30s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"制作系:{pattern}"))
            if len(matches) > 3:
                print(f"    ... 他{len(matches) - 3}件")
            print()
    
    # 6. 明らかに指示と思われるキャラクター名のチェック
    print("🎬 指示系キャラクター名のチェック:")
    print("-" * 50)
    
    instruction_character_patterns = [
        '%指示%', '%撮影%', '%編集%', '%音響%', '%技術%', '%スタッフ%',
        'SE', 'BGM', '効果音', 'エフェクト', 'シーン', '演出', 'カメラ'
    ]
    
    for pattern in instruction_character_patterns:
        cursor.execute("""
            SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
            FROM character_dialogue_unified cdu
            JOIN scripts s ON cdu.script_id = s.id
            WHERE cdu.is_instruction = 0 
            AND cdu.character_name LIKE ?
            AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
            LIMIT 8
        """, (pattern,))
        
        matches = cursor.fetchall()
        if matches:
            print(f"⚠️  キャラクター名 '{pattern}': {len(matches)}件が未フラグ設定")
            for management_id, char_name, dialogue, row in matches[:5]:
                char_short = char_name[:40] + "..." if len(char_name) > 40 else char_name
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    {management_id} 行{row:3d} | {char_short:30s} | \"{dialogue_short}\"")
                all_missed_instructions.append((management_id, char_name, dialogue, row, f"指示キャラ:{pattern}"))
            if len(matches) > 5:
                print(f"    ... 他{len(matches) - 5}件")
            print()
    
    # 7. 総計と推奨事項
    print("=" * 80)
    print(f"📊 包括的チェック結果:")
    print(f"  未フラグ設定の指示系データ: {len(all_missed_instructions)}件")
    
    if len(all_missed_instructions) > 0:
        print(f"\n💡 追加のフラグ設定を実行することを強く推奨します")
        print(f"   これらの指示データが検索結果に混入する可能性があります")
        
        # カテゴリ別の集計
        categories = {}
        for item in all_missed_instructions:
            category = item[4].split(':')[0] if ':' in item[4] else 'その他'
            categories[category] = categories.get(category, 0) + 1
        
        print(f"\n📈 カテゴリ別未フラグ件数:")
        for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {category}: {count}件")
            
    else:
        print(f"\n✅ 全指示タイプが適切にフラグ設定されています")
    
    conn.close()
    return len(all_missed_instructions), all_missed_instructions

if __name__ == "__main__":
    total_missed, missed_list = comprehensive_instruction_check()
    
    if total_missed > 0:
        print(f"\n🔧 修正スクリプトの作成を推奨します")
        print(f"   見つかった{total_missed}件の指示データをフラグ設定できます")