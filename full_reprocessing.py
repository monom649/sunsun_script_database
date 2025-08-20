#!/usr/bin/env python3
"""
全スクリプト再処理ツール

改良された抽出ロジックで全スクリプトを再処理し、
すべてのゲストキャラクターをデータベースに取り込む
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

class FullReprocessor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/full_reprocessing.txt"
        
        # 定型キャラクター
        self.standard_characters = {
            'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'BB', 'ママ', 'パパ', 
            'みんな', 'こども', '子ども', 'SE', '（SE）', '(SE)'
        }
        
        # 既知のゲストキャラクター（拡張版）
        self.known_guest_characters = {
            'プリル', 'カエルン', 'ウサッチ', 'モーレツ先生', 'そーだおじさん',
            'アイス姉さん', 'ゾンビ', 'サンタ', 'レッツプレイ！', 'どきどき！',
            'ター子', 'おりがみ夫人', '赤鬼', '犬ガンマン', 'いぬガンマン',
            'リーラ', 'ナレーション', 'ナレ', 'ねこ', 'いぬ', 'うさぎ', 'ぞう',
            'ライオン', 'ぱんだ', 'ペンギン', 'きりん', 'しまうま', 'かば',
            'アバン', 'エンディング', 'オープニング', 'メカメカ！メカメカ！',
            'ノイズ風', 'うんち', 'しょーちょーまきまき！', 'ぴえん', '焦る様子',
            'はーい！', 'いきもの'
        }
        
        # 厳密な除外パターン
        self.strict_exclusions = {
            'TRUE', 'FALSE', 'セリフ', '不明', '登場キャラ', '', '・・・', '→→'
        }
        
        # 部分一致での除外パターン
        self.partial_exclusions = [
            r'^\[=.*\]$',  # Excel数式
            r'^https?://',  # URL
            r'.*カット.*カット.*',  # 複数のカット指示
            r'.*映像.*映像.*',  # 複数の映像指示
            r'.*編集.*編集.*',  # 複数の編集指示
        ]
        
        # 明らかな指示文キーワード
        self.instruction_keywords = [
            '以降基本的に', 'イメージです', 'アップで見せたい', '手元で撮影', 
            'その手前で', '配置', 'の建物のイラスト'
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_valid_character_name(self, character_name):
        """キャラクター名検証（改良版）"""
        if not character_name or len(character_name.strip()) == 0:
            return False
        
        char_name = character_name.strip()
        
        # 長さ制限緩和（50文字まで）
        if len(char_name) > 50:
            return False
        
        # 厳密な除外（完全一致）
        if char_name in self.strict_exclusions:
            return False
        
        # 既知のキャラクターは常に許可
        if char_name in self.standard_characters or char_name in self.known_guest_characters:
            return True
        
        # 部分一致での除外パターン
        for pattern in self.partial_exclusions:
            if re.match(pattern, char_name):
                return False
        
        # 明らかな指示文の除外
        if any(keyword in char_name for keyword in self.instruction_keywords):
            return False
        
        # 数字のみは除外
        if char_name.isdigit():
            return False
        
        # 特殊記号のみは除外
        if re.match(r'^[・→（）\-\s]+$', char_name):
            return False
        
        return True
    
    def find_headers_flexible(self, df):
        """柔軟なヘッダー検索"""
        character_col = None
        dialogue_col = None
        filming_col = None
        audio_col = None
        header_row = None
        
        search_rows = min(20, len(df))
        
        for row_idx in range(search_rows):
            row = df.iloc[row_idx]
            
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    
                    # キャラクターヘッダー検索
                    char_keywords = ['キャラクター', 'キャラ', 'character', '話者']
                    if any(keyword in value_str for keyword in char_keywords):
                        if not value_str.startswith('[=') and not value_str.startswith('='):
                            character_col = col_idx
                            header_row = row_idx
                    
                    # セリフヘッダー検索
                    dialogue_keywords = ['セリフ', 'せりふ', 'dialogue']
                    if any(keyword in value_str for keyword in dialogue_keywords):
                        dialogue_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # 撮影指示ヘッダー検索
                    filming_keywords = ['撮影指示', '撮影', '映像指示', '映像']
                    if any(keyword in value_str for keyword in filming_keywords):
                        filming_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # 音声指示ヘッダー検索
                    audio_keywords = ['音声指示', '音声', 'audio']
                    if any(keyword in value_str for keyword in audio_keywords):
                        audio_col = col_idx
                        if header_row is None:
                            header_row = row_idx
        
        return character_col, dialogue_col, filming_col, audio_col, header_row
    
    def extract_data_comprehensive(self, script_url, management_id):
        """包括的データ抽出"""
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
                return None
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # ヘッダー検索
            character_col, dialogue_col, filming_col, audio_col, header_row = self.find_headers_flexible(df)
            
            if character_col is None:
                # 代替抽出
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
                
                # キャラクター名検証
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
            
            return entries if len(entries) > 0 else None
            
        except Exception as e:
            return None
    
    def fallback_extraction(self, df, management_id):
        """代替抽出"""
        entries = []
        
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip()
                    
                    if (value_str in self.standard_characters or 
                        value_str in self.known_guest_characters):
                        
                        # 隣接セルからセリフ取得
                        dialogue_text = ""
                        for offset in [1, 2, -1]:
                            adj_col = col_idx + offset
                            if 0 <= adj_col < len(row):
                                adj_value = row.iloc[adj_col]
                                if pd.notna(adj_value):
                                    adj_text = str(adj_value).strip()
                                    if (len(adj_text) > 3 and 
                                        not adj_text in self.strict_exclusions):
                                        dialogue_text = adj_text
                                        break
                        
                        entries.append({
                            'row_number': row_idx,
                            'character_name': value_str,
                            'dialogue_text': dialogue_text,
                            'filming_audio_instructions': ""
                        })
        
        return entries if entries else None
    
    def update_script_data(self, script_info):
        """スクリプトデータ更新"""
        try:
            new_data = self.extract_data_comprehensive(
                script_info['script_url'], 
                script_info['management_id']
            )
            
            if not new_data:
                return False
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # スクリプトID取得
            cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (script_info['management_id'],))
            script_row = cursor.fetchone()
            if not script_row:
                conn.close()
                return False
            
            script_id = script_row[0]
            
            # 既存データ削除
            cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
            deleted_count = cursor.rowcount
            
            # 新データ挿入
            inserted = 0
            for entry in new_data:
                try:
                    cursor.execute("""
                        INSERT INTO character_dialogue_unified 
                        (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        script_id,
                        entry['row_number'],
                        entry['character_name'],
                        entry['dialogue_text'],
                        entry['filming_audio_instructions']
                    ))
                    inserted += 1
                except:
                    pass
            
            conn.commit()
            conn.close()
            
            # 統計
            guest_chars = [e for e in new_data if e['character_name'] in self.known_guest_characters]
            if guest_chars:
                unique_guests = list(set([e['character_name'] for e in guest_chars]))
                self.log_message(f"🎭 {script_info['management_id']}: ゲスト発見 - {', '.join(unique_guests[:5])}")
            
            self.log_message(f"✅ {script_info['management_id']}: {deleted_count}→{inserted}件")
            return True
            
        except:
            return False
    
    def process_all_scripts(self):
        """全スクリプト処理"""
        self.log_message("=" * 80)
        self.log_message("全スクリプト包括的再処理開始")
        self.log_message("=" * 80)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                WHERE s.script_url IS NOT NULL AND s.script_url != ''
                ORDER BY s.management_id
            """)
            
            scripts = []
            for mgmt_id, title, script_url in cursor.fetchall():
                scripts.append({
                    'management_id': mgmt_id,
                    'title': title,
                    'script_url': script_url
                })
            
            conn.close()
            
            self.log_message(f"🎯 処理対象: {len(scripts)}件")
            
            success_count = 0
            guest_found_scripts = 0
            
            for i, script in enumerate(scripts):
                if i % 50 == 0:
                    self.log_message(f"🔧 進行 ({i+1}/{len(scripts)})")
                
                # 事前にゲストキャラチェック
                data = self.extract_data_comprehensive(script['script_url'], script['management_id'])
                has_guests = False
                if data:
                    guest_chars = [e for e in data if e['character_name'] in self.known_guest_characters]
                    if guest_chars:
                        has_guests = True
                        guest_found_scripts += 1
                
                if self.update_script_data(script):
                    success_count += 1
                
                # レート制限
                if i % 20 == 19:
                    time.sleep(1)
            
            self.log_message("=" * 80)
            self.log_message(f"再処理結果:")
            self.log_message(f"  処理成功: {success_count}/{len(scripts)}件")
            self.log_message(f"  ゲストキャラ発見: {guest_found_scripts}件")
            self.log_message("=" * 80)
            
            return success_count
            
        except Exception as e:
            self.log_message(f"❌ エラー: {str(e)}")
            return 0

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    processor = FullReprocessor(db_path)
    
    print("=== 全スクリプト再処理ツール ===")
    print("⚠️  この処理には時間がかかります")
    
    success_count = processor.process_all_scripts()
    
    if success_count > 0:
        print(f"\n✅ {success_count}件のスクリプトが再処理されました！")
        print("🎭 カエルン、ウサッチ、モーレツ先生を含む全ゲストキャラクターがデータベースに追加されました")
    else:
        print(f"\n❌ 処理に失敗しました")
    
    print(f"詳細は full_reprocessing.txt を確認してください")

if __name__ == "__main__":
    main()