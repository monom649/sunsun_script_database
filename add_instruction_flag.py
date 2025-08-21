#!/usr/bin/env python3
"""
撮影指示データにフラグを追加して検索除外対応
"""

import sqlite3

def add_instruction_flag():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. フラグ列を追加
    try:
        cursor.execute("ALTER TABLE character_dialogue_unified ADD COLUMN is_instruction BOOLEAN DEFAULT 0")
        print("✅ is_instruction列を追加しました")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("ℹ️ is_instruction列は既に存在します")
        else:
            raise e
    
    # 2. 撮影指示パターンの定義
    instruction_patterns = [
        '%指示%',
        '%撮影%', 
        '%音声%',
        '%編集%',
        '%アイキャッチ%',
        '%CM%',
        '%※%',
        '%定点で撮影%',
        '%ビームが出る%',
        '%編集で%',
        '%編集をお%',
        '[%]',  # [撮影指示]のようなパターン
        '※%',
        '%秒CM%',
        '%社内編集%',
        '%絵を部分的に%'
    ]
    
    # 3. 撮影指示データにフラグを設定
    total_flagged = 0
    
    for pattern in instruction_patterns:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE is_instruction = 0 AND (
                character_name LIKE ? OR 
                dialogue_text LIKE ?
            )
        """, (pattern, pattern))
        
        flagged_count = cursor.rowcount
        if flagged_count > 0:
            print(f"📌 パターン '{pattern}': {flagged_count}件をフラグ設定")
            total_flagged += flagged_count
    
    # 4. 明らかに撮影指示と思われるキャラクター名を個別処理
    specific_instructions = [
        'ゲーム音声',
        'せつこちゃんの目からビームが出る',
        '基本的に定点で撮影しているので、セリフに応じて絵を部分的にアップする等、編集でよろしくお願いいたします',
        'アイキャッチ　↓ここから社内編集',
        '以降５秒CM ※MANAMITOさんは本編までの編集をおねがいいたします！'
    ]
    
    for instruction in specific_instructions:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE character_name = ?
        """, (instruction,))
        
        flagged_count = cursor.rowcount
        if flagged_count > 0:
            print(f"📌 個別指定 '{instruction}': {flagged_count}件をフラグ設定")
            total_flagged += flagged_count
    
    # 5. 統計確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    dialogue_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified")
    total_count = cursor.fetchone()[0]
    
    print(f"\n📊 処理結果:")
    print(f"  撮影指示データ: {instruction_count}件")
    print(f"  通常のセリフ: {dialogue_count}件") 
    print(f"  総データ数: {total_count}件")
    print(f"  今回フラグ設定: {total_flagged}件")
    
    # 6. サンプル確認
    print(f"\n📋 撮影指示データのサンプル（最初の10件）:")
    cursor.execute("""
        SELECT character_name, dialogue_text 
        FROM character_dialogue_unified 
        WHERE is_instruction = 1 
        LIMIT 10
    """)
    
    samples = cursor.fetchall()
    for i, (char, dialogue) in enumerate(samples, 1):
        char_short = char[:30] + "..." if len(char) > 30 else char
        dialogue_short = dialogue[:30] + "..." if len(dialogue) > 30 else dialogue
        print(f"  {i}. {char_short} - {dialogue_short}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 撮影指示フラグ設定完了")
    return instruction_count, dialogue_count

if __name__ == "__main__":
    add_instruction_flag()