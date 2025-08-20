#!/usr/bin/env python3
"""
スプレッドシートサンプル確認ツール

実際のスプレッドシートでゲストキャラクターが
どのように入力されているかを調査する。
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class SpreadsheetSampleChecker:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/spreadsheet_sample_check.txt"
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def get_sample_scripts_with_guests(self, limit=5):
        """ゲストキャラクターが含まれるスクリプトのサンプル取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # プリル、そーだおじさん等のゲストキャラクターが含まれるスクリプト
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE cdu.character_name IN ('プリル', 'そーだおじさん', 'ゾンビ', 'サンタ', 'アイス姉さん')
                AND s.script_url IS NOT NULL AND s.script_url != ''
                ORDER BY s.management_id
                LIMIT ?
            """, (limit,))
            
            sample_scripts = cursor.fetchall()
            conn.close()
            
            return sample_scripts
            
        except Exception as e:
            self.log_message(f"❌ サンプルスクリプト取得エラー: {str(e)}")
            return []
    
    def analyze_spreadsheet_structure(self, script_url, management_id):
        """スプレッドシートの構造を分析"""
        if not script_url or 'docs.google.com' not in script_url:
            return None
        
        try:
            # スプレッドシートIDとGIDを抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return None
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL構築
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                self.log_message(f"⚠️  {management_id}: HTTP {response.status_code}")
                return None
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📊 {management_id}: {len(df)}行 x {len(df.columns)}列のスプレッドシート")
            
            # ヘッダー行の検索
            character_col = None
            dialogue_col = None
            header_row = None
            
            for row_idx in range(min(10, len(df))):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        if 'キャラクター' in value_str:
                            character_col = col_idx
                            header_row = row_idx
                        if 'セリフ' in value_str:
                            dialogue_col = col_idx
                            if header_row is None:
                                header_row = row_idx
            
            if character_col is None:
                self.log_message(f"❌ {management_id}: キャラクターヘッダーが見つかりません")
                return None
            
            self.log_message(f"✅ {management_id}: キャラクター列{character_col}, セリフ列{dialogue_col}, ヘッダー行{header_row}")
            
            # キャラクター列のデータを分析
            start_row = header_row + 1 if header_row is not None else 3
            character_data = []
            
            for index in range(start_row, min(start_row + 50, len(df))):  # 50行まで分析
                if index < len(df):
                    row = df.iloc[index]
                    if character_col < len(row):
                        character_value = row.iloc[character_col]
                        if pd.notna(character_value):
                            char_str = str(character_value).strip()
                            if len(char_str) > 0:
                                # セリフも取得
                                dialogue_text = ""
                                if dialogue_col is not None and dialogue_col < len(row):
                                    dialogue_value = row.iloc[dialogue_col]
                                    if pd.notna(dialogue_value):
                                        dialogue_text = str(dialogue_value).strip()[:100]  # 100文字まで
                                
                                character_data.append({
                                    'row': index,
                                    'character': char_str,
                                    'dialogue': dialogue_text
                                })
            
            return {
                'management_id': management_id,
                'total_rows': len(df),
                'total_cols': len(df.columns),
                'character_col': character_col,
                'dialogue_col': dialogue_col,
                'header_row': header_row,
                'character_data': character_data
            }
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: スプレッドシート分析エラー - {str(e)}")
            return None
    
    def check_character_input_patterns(self, analysis_result):
        """キャラクター入力パターンの確認"""
        if not analysis_result or not analysis_result['character_data']:
            return
        
        management_id = analysis_result['management_id']
        character_data = analysis_result['character_data']
        
        self.log_message(f"\n🔍 {management_id}: キャラクター入力パターン分析")
        
        # 定型キャラクター vs ゲストキャラクター
        standard_chars = {'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'BB', 'ママ', 'パパ', 'みんな', 'SE'}
        
        standard_found = []
        guest_found = []
        problematic_found = []
        
        for data in character_data:
            char_name = data['character']
            
            if char_name in standard_chars:
                standard_found.append(data)
            elif any(marker in char_name for marker in ['・', '（', '）', '→', 'カット', '映像']):
                problematic_found.append(data)
            else:
                guest_found.append(data)
        
        self.log_message(f"  定型キャラクター: {len(standard_found)}個")
        for data in standard_found[:5]:  # 最初の5個
            self.log_message(f"    行{data['row']}: {data['character']}")
        
        self.log_message(f"  ゲストキャラクター候補: {len(guest_found)}個")
        for data in guest_found[:10]:  # 最初の10個
            self.log_message(f"    行{data['row']}: {data['character']} | {data['dialogue'][:50]}...")
        
        self.log_message(f"  問題のある入力: {len(problematic_found)}個")
        for data in problematic_found[:5]:  # 最初の5個
            self.log_message(f"    行{data['row']}: {data['character']}")
        
        # 長いキャラクター名をチェック
        long_names = [data for data in character_data if len(data['character']) > 20]
        if long_names:
            self.log_message(f"  20文字超の名前: {len(long_names)}個")
            for data in long_names[:3]:
                self.log_message(f"    行{data['row']}: {data['character']} ({len(data['character'])}文字)")
    
    def run_sample_analysis(self):
        """サンプル分析実行"""
        self.log_message("スプレッドシートサンプル分析開始")
        
        # サンプルスクリプト取得
        sample_scripts = self.get_sample_scripts_with_guests(5)
        
        if not sample_scripts:
            self.log_message("❌ サンプルスクリプトが見つかりませんでした")
            return
        
        self.log_message(f"📋 {len(sample_scripts)}個のサンプルスクリプトを分析")
        
        for management_id, title, script_url in sample_scripts:
            self.log_message(f"\n" + "=" * 60)
            self.log_message(f"分析対象: {management_id} - {title[:50]}...")
            
            # スプレッドシート構造分析
            analysis = self.analyze_spreadsheet_structure(script_url, management_id)
            
            if analysis:
                # キャラクター入力パターン確認
                self.check_character_input_patterns(analysis)
            else:
                self.log_message(f"❌ {management_id}: 分析に失敗しました")
        
        self.log_message("\n" + "=" * 80)
        self.log_message("📋 サンプル分析完了")
        self.log_message("=" * 80)

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    checker = SpreadsheetSampleChecker(db_path)
    
    print("=== スプレッドシートサンプル確認ツール ===")
    
    # サンプル分析実行
    checker.run_sample_analysis()
    
    print(f"\n✅ サンプル分析完了！")
    print(f"詳細は spreadsheet_sample_check.txt を確認してください。")

if __name__ == "__main__":
    main()