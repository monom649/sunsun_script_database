#!/usr/bin/env python3
"""
is_instruction=1に設定されたデータを詳細確認
セリフが誤ってフラグ設定されていないかチェック
"""

import sqlite3
import re

def verify_instruction_flags():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. フラグ設定されたデータを全て取得
    cursor.execute("""
        SELECT s.management_id, cdu.character_name, cdu.dialogue_text, cdu.row_number
        FROM character_dialogue_unified cdu
        JOIN scripts s ON cdu.script_id = s.id
        WHERE cdu.is_instruction = 1
        ORDER BY s.management_id, cdu.row_number
    """)
    
    flagged_data = cursor.fetchall()
    print(f"📊 フラグ設定されたデータ: {len(flagged_data)}件")
    
    # 2. 疑わしいデータの分析
    suspicious_items = []
    
    # 通常のキャラクター名パターン
    normal_characters = [
        'サンサン', 'くもりん', 'プリル', 'ノイズ', 'ツクモ', 'BB', 'シーン', 'ナレーション', 'ナレ'
    ]
    
    # 明らかにセリフらしいパターン
    dialogue_patterns = [
        r'[！？!?]$',  # 感嘆符・疑問符で終わる
        r'^[あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん]+[！？!?]*$',  # ひらがなのみ
        r'だよ[！？!?]*$',  # 「だよ」で終わる
        r'です[！？!?]*$',  # 「です」で終わる
        r'でしょ[！？!?]*$',  # 「でしょ」で終わる
        r'ます[！？!?]*$',  # 「ます」で終わる
        r'^[ぁ-んァ-ヶー]{1,10}[！？!?]+$',  # 短い感嘆詞
    ]
    
    for management_id, character_name, dialogue_text, row_number in flagged_data:
        # 通常のキャラクターによるセリフらしきもの
        if character_name in normal_characters:
            # セリフらしいパターンをチェック
            for pattern in dialogue_patterns:
                if re.search(pattern, dialogue_text):
                    suspicious_items.append({
                        'management_id': management_id,
                        'character': character_name,
                        'dialogue': dialogue_text,
                        'row': row_number,
                        'reason': f'パターン: {pattern}'
                    })
                    break
        
        # 短すぎるテキスト（5文字以下で指示っぽくない）
        if len(dialogue_text.strip()) <= 5 and not any(keyword in dialogue_text for keyword in 
            ['CM', '※', '指示', '撮影', '音声', '編集', 'SE', 'BGM']):
            if character_name in normal_characters:
                suspicious_items.append({
                    'management_id': management_id,
                    'character': character_name,
                    'dialogue': dialogue_text,
                    'row': row_number,
                    'reason': '短すぎるテキスト'
                })
    
    # 3. 疑わしいアイテムを表示
    print(f"\n⚠️ 疑わしいデータ: {len(suspicious_items)}件")
    print("=" * 80)
    
    if suspicious_items:
        for i, item in enumerate(suspicious_items[:50], 1):  # 最初の50件
            print(f"{i:2d}. {item['management_id']} 行{item['row']:3d} | {item['character']:10s} | \"{item['dialogue']}\"")
            print(f"    理由: {item['reason']}")
            print()
    
    # 4. キャラクター名別統計
    print("\n📋 フラグ設定されたキャラクター名別統計:")
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 1
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 20
    """)
    
    char_stats = cursor.fetchall()
    for char_name, count in char_stats:
        char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
        print(f"  {char_short:35s}: {count:4d}件")
    
    # 5. 指示文らしいサンプル
    print(f"\n✅ 正しく指示文として分類されたサンプル:")
    cursor.execute("""
        SELECT character_name, dialogue_text
        FROM character_dialogue_unified 
        WHERE is_instruction = 1 
        AND (character_name LIKE '%指示%' OR character_name LIKE '%撮影%' OR character_name LIKE 'SE' OR character_name LIKE 'CM')
        LIMIT 10
    """)
    
    correct_samples = cursor.fetchall()
    for i, (char, dialogue) in enumerate(correct_samples, 1):
        char_short = char[:20] + "..." if len(char) > 20 else char
        dialogue_short = dialogue[:40] + "..." if len(dialogue) > 40 else dialogue
        print(f"  {i:2d}. {char_short:25s} | \"{dialogue_short}\"")
    
    conn.close()
    
    return len(suspicious_items), len(flagged_data)

if __name__ == "__main__":
    verify_instruction_flags()