#!/usr/bin/env python3
"""
動的ヘッダー検出による正確な抽出ツール
各スプレッドシートのヘッダー行位置を動的に検出し、
正しい列構造でデータを抽出する
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class DynamicHeaderExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/dynamic_extraction.txt"
        
        # ヘッダー検出キーワード
        self.header_keywords = {
            'character': ['キャラクター', 'キャラ', 'character'],
            'dialogue': ['セリフ', 'せりふ', 'dialogue', 'セリフ内容'],
            'audio_instruction': ['音声指示', '音声', 'audio', '音響'],
            'filming_instruction': ['撮影指示', '撮影', 'filming', 'カメラ'],
            'editing_instruction': ['編集指示', '編集', 'editing']
        }
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def detect_header_structure(self, df):
        """
        スプレッドシートのヘッダー構造を動的に検出
        """
        structure = {
            'header_row': None,
            'character_col': None,
            'dialogue_col': None,
            'audio_instruction_col': None,
            'filming_instruction_col': None,
            'editing_instruction_col': None
        }
        
        # 最初の20行でヘッダーを検索
        for row_idx in range(min(20, len(df))):
            row = df.iloc[row_idx]
            found_headers = 0
            
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    
                    # キャラクター列検出
                    if any(keyword in value_str for keyword in self.header_keywords['character']):
                        structure['character_col'] = col_idx
                        found_headers += 1
                    
                    # セリフ列検出
                    elif any(keyword in value_str for keyword in self.header_keywords['dialogue']):
                        structure['dialogue_col'] = col_idx
                        found_headers += 1
                    
                    # 音声指示列検出
                    elif any(keyword in value_str for keyword in self.header_keywords['audio_instruction']):
                        structure['audio_instruction_col'] = col_idx
                        found_headers += 1
                    
                    # 撮影指示列検出
                    elif any(keyword in value_str for keyword in self.header_keywords['filming_instruction']):
                        structure['filming_instruction_col'] = col_idx
                        found_headers += 1
                    
                    # 編集指示列検出
                    elif any(keyword in value_str for keyword in self.header_keywords['editing_instruction']):
                        structure['editing_instruction_col'] = col_idx
                        found_headers += 1
            
            # キャラクターとセリフ列が見つかったらヘッダー行として採用
            if structure['character_col'] is not None and structure['dialogue_col'] is not None:
                structure['header_row'] = row_idx
                break
        
        return structure
    
    def extract_sheet_data(self, sheet_url):
        """
        スプレッドシートからデータを抽出
        """
        try:
            # URL解析
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, sheet_url)
            gid_match = re.search(gid_pattern, sheet_url)
            
            if not sheet_match:
                return None
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV取得
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            response = requests.get(csv_url, timeout=15)
            
            if response.status_code != 200:
                return None
            
            # DataFrame作成
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # ヘッダー構造検出
            structure = self.detect_header_structure(df)
            
            if structure['header_row'] is None:
                self.log_message(f"❌ ヘッダー行が検出できませんでした")
                return None
            
            self.log_message(f"✅ ヘッダー検出: 行{structure['header_row']+1}, キャラ列{structure['character_col']+1}, セリフ列{structure['dialogue_col']+1}")
            
            # データ抽出
            extracted_data = []
            start_row = structure['header_row'] + 1
            
            for row_idx in range(start_row, len(df)):
                row = df.iloc[row_idx]
                
                # キャラクター名取得
                character_name = ""
                if structure['character_col'] is not None and structure['character_col'] < len(row):
                    char_val = row.iloc[structure['character_col']]
                    if pd.notna(char_val):
                        character_name = str(char_val).strip()
                
                # セリフ取得
                dialogue_text = ""
                if structure['dialogue_col'] is not None and structure['dialogue_col'] < len(row):
                    dialogue_val = row.iloc[structure['dialogue_col']]
                    if pd.notna(dialogue_val):
                        dialogue_text = str(dialogue_val).strip()
                
                # 指示取得
                filming_instruction = ""
                if structure['filming_instruction_col'] is not None and structure['filming_instruction_col'] < len(row):
                    filming_val = row.iloc[structure['filming_instruction_col']]
                    if pd.notna(filming_val):
                        filming_instruction = str(filming_val).strip()
                
                # 有効なデータの場合のみ追加
                if character_name or dialogue_text:
                    extracted_data.append({
                        'row_number': row_idx + 1,
                        'character_name': character_name,
                        'dialogue_text': dialogue_text,
                        'filming_instruction': filming_instruction
                    })
            
            return extracted_data
            
        except Exception as e:
            self.log_message(f"❌ 抽出エラー: {str(e)}")
            return None
    
    def test_single_script(self, management_id):
        """
        単一スクリプトでテスト
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT script_url FROM scripts WHERE management_id = ?", (management_id,))
            result = cursor.fetchone()
            
            if not result:
                self.log_message(f"❌ {management_id} が見つかりません")
                return
            
            sheet_url = result[0]
            self.log_message(f"🎯 {management_id} のテスト抽出開始")
            
            extracted_data = self.extract_sheet_data(sheet_url)
            
            if extracted_data:
                self.log_message(f"✅ {len(extracted_data)}件のデータを抽出")
                
                # 最初の5件を表示
                for i, data in enumerate(extracted_data[:5]):
                    self.log_message(f"  行{data['row_number']}: '{data['character_name']}' → '{data['dialogue_text'][:50]}...'")
            else:
                self.log_message(f"❌ データ抽出に失敗")
            
            conn.close()
            return extracted_data
            
        except Exception as e:
            self.log_message(f"❌ テストエラー: {str(e)}")

def main():
    """テスト実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = DynamicHeaderExtractor(db_path)
    
    print("=== 動的ヘッダー検出テスト ===")
    
    # B1780でテスト
    extractor.test_single_script("B1780")

if __name__ == "__main__":
    main()