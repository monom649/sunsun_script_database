#!/usr/bin/env python3
"""
B1780の正しいデータでデータベース更新
"""

import sqlite3
from dynamic_header_extractor import DynamicHeaderExtractor

def fix_b1780():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = DynamicHeaderExtractor(db_path)
    
    # B1780の正しいデータを抽出
    extracted_data = extractor.test_single_script("B1780")
    
    if not extracted_data:
        print("❌ B1780のデータ抽出に失敗")
        return
    
    print(f"✅ {len(extracted_data)}件のデータを抽出")
    
    # データベースに挿入
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    script_id = 1808  # B1780のscript_id
    
    inserted_count = 0
    for data in extracted_data:
        # 有効なデータのみ挿入
        if data['character_name'] or data['dialogue_text']:
            cursor.execute("""
                INSERT INTO character_dialogue_unified 
                (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)
                VALUES (?, ?, ?, ?, ?)
            """, (
                script_id,
                data['row_number'],
                data['character_name'] or '[話者不明]',
                data['dialogue_text'],
                data['filming_instruction']
            ))
            inserted_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ B1780: {inserted_count}件のデータを正しく挿入完了")
    
    # 検証
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT row_number, character_name, dialogue_text 
        FROM character_dialogue_unified 
        WHERE script_id = ? 
        ORDER BY row_number 
        LIMIT 5
    """, (script_id,))
    
    results = cursor.fetchall()
    print("\n🔍 修正後の確認（最初の5件）:")
    for row_num, char_name, dialogue in results:
        print(f"  行{row_num}: '{char_name}' → '{dialogue[:50]}...'")
    
    conn.close()

if __name__ == "__main__":
    fix_b1780()