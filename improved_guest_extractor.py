#!/usr/bin/env python3
"""
改良版ゲストキャラクター抽出ツール

ゲストキャラクターの抽出漏れを防ぐための改善されたロジック
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class ImprovedGuestExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/improved_guest_extraction.txt"
        
        # 既知のゲストキャラクター許可リスト
        self.known_guest_characters = {
            'プリル', 'カエルン', 'ウサッチ', 'モーレツ先生', 'そーだおじさん',
            'アイス姉さん', 'ゾンビ', 'サンタ', 'レッツプレイ！', 'どきどき！',
            'ター子', 'おりがみ夫人', '赤鬼', '犬ガンマン', 'いぬガンマン'
        }
        
        # 緩和された除外パターン（より厳密に）
        self.strict_exclusion_patterns = [
            r'^TRUE$', r'^FALSE$', r'^セリフ$', r'^不明$',
            r'^\[=.*\]$',  # Excel数式
            r'^https?://',  # URL
            r'^・.*',  # 箇条書き開始
            r'.*→.*→.*',  # 複数の矢印（明らかな指示文）
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_valid_character_name(self, character_name):
        """改良されたキャラクター名検証"""
        if not character_name or len(character_name.strip()) == 0:
            return False
        
        char_name = character_name.strip()
        
        # 1. 長さ制限を緩和（20文字 → 30文字）
        if len(char_name) > 30:
            return False
        
        # 2. 既知のゲストキャラクターは常に許可
        if char_name in self.known_guest_characters:
            return True
        
        # 3. 厳密な除外パターンチェック
        for pattern in self.strict_exclusion_patterns:
            if re.match(pattern, char_name):
                return False
        
        # 4. 明らかな指示文パターンの除外（より厳密に）
        instruction_indicators = [
            'カット:', 'SE:', '映像:', 'テロップ:', 'アニメ:', '編集:',
            '・・・', '→→', '演出指示', '撮影指示', '音声指示'
        ]
        
        if any(indicator in char_name for indicator in instruction_indicators):
            return False
        
        # 5. 括弧付きキャラクター名の特別処理
        if '（' in char_name and '）' in char_name:
            # 「（ゲスト）カエルン」のような形式を許可
            bracket_content = re.search(r'（(.+?)）', char_name)
            if bracket_content:
                bracket_text = bracket_content.group(1)
                if bracket_text in ['ゲスト', 'ナレ', 'ナレーション']:
                    return True
        
        return True
    
    def find_all_header_columns_flexible(self, df):
        """より柔軟なヘッダー列検索"""
        character_col = None
        dialogue_col = None
        filming_col = None
        audio_col = None
        header_row = None
        
        # より多くの行を検索（10行 → 15行）
        search_rows = min(15, len(df))
        
        for row_idx in range(search_rows):
            row = df.iloc[row_idx]
            
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    
                    # より柔軟なヘッダー検出
                    if any(keyword in value_str for keyword in ['キャラクター', 'キャラ', 'character']):
                        character_col = col_idx
                        header_row = row_idx
                    
                    if any(keyword in value_str for keyword in ['セリフ', 'せりふ', 'dialogue', 'ダイアログ']):
                        dialogue_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    if any(keyword in value_str for keyword in ['撮影指示', '撮影', 'filming']):
                        filming_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    if any(keyword in value_str for keyword in ['音声指示', '音声', 'audio']):
                        audio_col = col_idx
                        if header_row is None:
                            header_row = row_idx
        
        return character_col, dialogue_col, filming_col, audio_col, header_row
    
    def extract_with_improved_logic(self, script_url, management_id):
        """改良された抽出ロジック"""
        if not script_url or 'docs.google.com' not in script_url:
            return None
        
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return None
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                self.log_message(f"⚠️  {management_id}: HTTP {response.status_code}")
                return None
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📊 {management_id}: {len(df)}行 x {len(df.columns)}列")
            
            # 改良されたヘッダー検索
            character_col, dialogue_col, filming_col, audio_col, header_row = self.find_all_header_columns_flexible(df)
            
            if character_col is None:
                # ヘッダーが見つからない場合の代替処理
                self.log_message(f"⚠️  {management_id}: ヘッダー検出失敗、代替抽出を試行")
                return self.fallback_extraction(df, management_id)
            
            entries = []
            start_row = header_row + 1 if header_row is not None else 3
            
            for index in range(start_row, len(df)):
                row = df.iloc[index]
                
                # キャラクター名取得
                character_value = row.iloc[character_col] if character_col < len(row) else None
                if not pd.notna(character_value):
                    continue
                
                character_name = str(character_value).strip()
                
                # 改良された検証ロジック
                if not self.is_valid_character_name(character_name):
                    continue
                
                # セリフテキスト取得
                dialogue_text = ""
                if dialogue_col is not None and dialogue_col < len(row):
                    dialogue_value = row.iloc[dialogue_col]
                    if pd.notna(dialogue_value):
                        dialogue_text = str(dialogue_value).strip()
                
                # 指示文取得
                filming_instructions = ""
                if filming_col is not None and filming_col < len(row):
                    filming_value = row.iloc[filming_col]
                    if pd.notna(filming_value):
                        filming_instructions = str(filming_value).strip()
                
                audio_instructions = ""
                if audio_col is not None and audio_col < len(row):
                    audio_value = row.iloc[audio_col]
                    if pd.notna(audio_value):
                        audio_instructions = str(audio_value).strip()
                
                combined_instructions = ""
                if filming_instructions and audio_instructions:
                    combined_instructions = f"{filming_instructions} / {audio_instructions}"
                elif filming_instructions:
                    combined_instructions = filming_instructions
                elif audio_instructions:
                    combined_instructions = audio_instructions
                
                entries.append({
                    'row_number': index,
                    'character_name': character_name,
                    'dialogue_text': dialogue_text,
                    'filming_audio_instructions': combined_instructions
                })
            
            guest_chars = [e for e in entries if e['character_name'] in self.known_guest_characters]
            self.log_message(f"✅ {management_id}: {len(entries)}件抽出 (ゲストキャラ: {len(guest_chars)}件)")
            
            return entries if len(entries) > 0 else None
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: 抽出エラー - {str(e)}")
            return None
    
    def fallback_extraction(self, df, management_id):
        """ヘッダー検出失敗時の代替抽出"""
        self.log_message(f"🔄 {management_id}: 代替抽出モード")
        
        entries = []
        
        # 全セルをスキャンして既知のゲストキャラクターを探す
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip()
                    
                    # 既知のゲストキャラクターかチェック
                    if value_str in self.known_guest_characters:
                        # 隣接するセルからセリフを探す
                        dialogue_text = ""
                        for offset in [1, 2, -1]:
                            adj_col = col_idx + offset
                            if 0 <= adj_col < len(row):
                                adj_value = row.iloc[adj_col]
                                if pd.notna(adj_value):
                                    adj_text = str(adj_value).strip()
                                    if len(adj_text) > 5 and not any(marker in adj_text for marker in ['TRUE', 'FALSE', 'http']):
                                        dialogue_text = adj_text
                                        break
                        
                        entries.append({
                            'row_number': row_idx,
                            'character_name': value_str,
                            'dialogue_text': dialogue_text,
                            'filming_audio_instructions': ""
                        })
        
        if entries:
            self.log_message(f"🎯 {management_id}: 代替抽出で{len(entries)}件のゲストキャラクターを発見")
        
        return entries if entries else None
    
    def search_missing_characters(self, target_chars=['カエルン', 'ウサッチ', 'モーレツ先生']):
        """未発見キャラクターの検索"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 処理対象のスクリプト一覧取得
            cursor.execute("""
                SELECT DISTINCT management_id, title, script_url
                FROM scripts
                WHERE script_url IS NOT NULL AND script_url != ''
                ORDER BY management_id
                LIMIT 100
            """)
            
            scripts = cursor.fetchall()
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("未発見ゲストキャラクター検索開始")
            self.log_message("=" * 80)
            
            found_characters = {char: [] for char in target_chars}
            
            for i, (management_id, title, script_url) in enumerate(scripts):
                if i % 10 == 0:
                    self.log_message(f"🔍 検索進行: {i+1}/{len(scripts)}")
                
                entries = self.extract_with_improved_logic(script_url, management_id)
                
                if entries:
                    for entry in entries:
                        char_name = entry['character_name']
                        if char_name in target_chars:
                            found_characters[char_name].append({
                                'script': management_id,
                                'title': title,
                                'dialogue': entry['dialogue_text'][:100]
                            })
            
            # 結果報告
            self.log_message("=" * 80)
            self.log_message("検索結果")
            self.log_message("=" * 80)
            
            for char, occurrences in found_characters.items():
                if occurrences:
                    self.log_message(f"✅ {char}: {len(occurrences)}件発見")
                    for occ in occurrences[:3]:  # 最初の3件表示
                        self.log_message(f"  {occ['script']}: {occ['dialogue']}")
                else:
                    self.log_message(f"❌ {char}: 見つかりませんでした")
            
            return found_characters
            
        except Exception as e:
            self.log_message(f"❌ 検索エラー: {str(e)}")
            return {}

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = ImprovedGuestExtractor(db_path)
    
    print("=== 改良版ゲストキャラクター抽出ツール ===")
    
    # 未発見キャラクター検索
    results = extractor.search_missing_characters()
    
    total_found = sum(len(occurrences) for occurrences in results.values())
    print(f"\n✅ 検索完了！発見件数: {total_found}")
    print(f"詳細は improved_guest_extraction.txt を確認してください。")

if __name__ == "__main__":
    main()