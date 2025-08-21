#!/usr/bin/env python3
"""
誤ってフラグ設定されたセリフデータを修正
"""

import sqlite3
import re

def fix_instruction_flags():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. 通常のキャラクターによるセリフで誤フラグ設定されたものを修正
    normal_characters = ['サンサン', 'くもりん', 'プリル', 'ノイズ', 'ツクモ', 'BB']
    
    # セリフらしいパターン（これらは指示ではない）
    dialogue_patterns = [
        r'.*[だよだねです]！*$',  # 「〜だよ！」「〜だね！」「〜です！」で終わる
        r'^[^※].*(CMです|CMだ|CMの|写真撮影|撮影し|撮影で).*[！？!?]+$',  # ※なしでCM・撮影を含む感嘆文
        r'^[^※].*(指示を|指示に).*[！？!?]+$',  # ※なしで指示を含む感嘆文
        r'.*[たちった]ら.*[！？!?]+$',  # 「〜したら〜！」のような過去形
        r'.*会えた.*記念.*[！？!?]+$',  # 「会えた記念に〜」
        r'.*お腹.*すい.*[！？!?]+$',  # 「お腹すいちゃった」
        r'.*カッチョイー.*[！？!?]+$',  # 「カッチョイー！」
        r'.*犯罪です.*[！？!?]+$',  # 「犯罪です！」
    ]
    
    corrected_count = 0
    
    print("🔧 誤フラグ設定データの修正開始...")
    
    for character in normal_characters:
        cursor.execute("""
            SELECT rowid, character_name, dialogue_text
            FROM character_dialogue_unified
            WHERE is_instruction = 1 AND character_name = ?
        """, (character,))
        
        character_flagged = cursor.fetchall()
        
        for rowid, char_name, dialogue_text in character_flagged:
            should_unflag = False
            matched_pattern = None
            
            # ※印がない場合はさらに詳しくチェック
            if '※' not in dialogue_text:
                for pattern in dialogue_patterns:
                    if re.search(pattern, dialogue_text):
                        should_unflag = True
                        matched_pattern = pattern
                        break
                
                # 短い普通のセリフ（5文字以下で指示系キーワードなし）
                if len(dialogue_text.strip()) <= 10 and not any(keyword in dialogue_text for keyword in 
                    ['※', '指示', '撮影指示', '編集指示', '音声指示', 'SE', 'BGM']):
                    if not any(keyword in dialogue_text for keyword in ['CM', '撮影', '編集']):
                        should_unflag = True
                        matched_pattern = "短いセリフ"
            
            if should_unflag:
                cursor.execute("UPDATE character_dialogue_unified SET is_instruction = 0 WHERE rowid = ?", (rowid,))
                corrected_count += 1
                print(f"  ✅ 修正: {char_name} | \"{dialogue_text[:50]}{'...' if len(dialogue_text) > 50 else ''}\"")
                print(f"      理由: {matched_pattern}")
    
    # 2. 具体的な誤分類データを個別修正
    specific_corrections = [
        "ここでCMです！",
        "の前に一旦CM！",
        "妖怪出没看板を撮影するんだね！",
        "最初はみんなと会えた記念に写真撮影だー！！",
        "みんなと楽しく撮影してたらお腹すいちゃった！",
        "目からレーザービームが出るんだよね！カッチョイー！",
        "上映中の撮影や録画は犯罪です！",
        "一緒に撮影できなかったお友達にも会いに来たよ～！！",
        "出てきたマスの指示に従ってちょうだい！",
        "ゴールにたどり着くには、クリスにどういう指示をしたらいいんだろう・・？？"
    ]
    
    for dialogue in specific_corrections:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 0 
            WHERE is_instruction = 1 AND dialogue_text = ?
        """, (dialogue,))
        
        if cursor.rowcount > 0:
            corrected_count += cursor.rowcount
            print(f"  ✅ 個別修正: \"{dialogue[:50]}{'...' if len(dialogue) > 50 else ''}\"")
    
    # 3. CMコンテンツ紹介系セリフの修正（ツクモによるもの）
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 0 
        WHERE is_instruction = 1 
        AND character_name = 'ツクモ'
        AND (dialogue_text LIKE '%CMのあとは%' OR dialogue_text LIKE '%CMの後は%')
        AND dialogue_text LIKE '%！'
    """)
    
    if cursor.rowcount > 0:
        corrected_count += cursor.rowcount
        print(f"  ✅ ツクモCM紹介セリフ修正: {cursor.rowcount}件")
    
    # 4. 統計確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    dialogue_count = cursor.fetchone()[0]
    
    print(f"\n📊 修正結果:")
    print(f"  修正件数: {corrected_count}件")
    print(f"  撮影指示データ: {instruction_count}件")
    print(f"  通常のセリフ: {dialogue_count}件")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 誤フラグ修正完了")
    return corrected_count

if __name__ == "__main__":
    fix_instruction_flags()