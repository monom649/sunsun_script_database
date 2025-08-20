#!/usr/bin/env python3
"""
URLリスト抽出ツール

シートリストから台本URLを抽出し、
実際の台本の構造と抽出状況を確認する
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class UrlListExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/url_list_extraction.txt"
        
        # URLリストスプレッドシート
        self.url_list_sheet = 'https://docs.google.com/spreadsheets/d/1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8/edit?gid=1092002230#gid=1092002230'
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def extract_urls_from_list(self):
        """URLリストから台本URLを抽出"""
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, self.url_list_sheet)
            gid_match = re.search(gid_pattern, self.url_list_sheet)
            
            if not sheet_match:
                return []
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                return []
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📋 URLリスト: {len(df)}行 x {len(df.columns)}列")
            
            # 台本URL列を特定（通常は5列目）
            script_urls = []
            
            for row_idx in range(len(df)):
                row = df.iloc[row_idx]
                
                # 管理番号（通常は3列目）
                management_id = ""
                if len(row) > 3 and pd.notna(row.iloc[3]):
                    management_id = str(row.iloc[3]).strip()
                
                # タイトル（通常は4列目）
                title = ""
                if len(row) > 4 and pd.notna(row.iloc[4]):
                    title = str(row.iloc[4]).strip()
                
                # 台本URL（通常は5列目）
                script_url = ""
                if len(row) > 5 and pd.notna(row.iloc[5]):
                    url_value = str(row.iloc[5]).strip()
                    if 'docs.google.com' in url_value:
                        script_url = url_value
                
                if management_id and script_url:
                    script_urls.append({
                        'management_id': management_id,
                        'title': title,
                        'script_url': script_url
                    })
            
            self.log_message(f"📊 抽出された台本URL: {len(script_urls)}件")
            return script_urls
            
        except Exception as e:
            self.log_message(f"❌ URLリスト抽出エラー: {str(e)}")
            return []
    
    def analyze_sample_scripts(self, script_urls, sample_count=10):
        """サンプル台本の詳細分析"""
        self.log_message(f"\n📖 サンプル台本分析（{sample_count}件）:")
        
        for i, script_info in enumerate(script_urls[:sample_count]):
            self.log_message(f"\n{'=' * 50}")
            self.log_message(f"サンプル {i+1}: {script_info['management_id']}")
            self.log_message(f"タイトル: {script_info['title'][:50]}...")
            self.log_message(f"{'=' * 50}")
            
            # 台本構造分析
            self.analyze_single_script(script_info)
    
    def analyze_single_script(self, script_info):
        """単一台本の構造分析"""
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_info['script_url'])
            gid_match = re.search(gid_pattern, script_info['script_url'])
            
            if not sheet_match:
                self.log_message(f"❌ {script_info['management_id']}: スプレッドシートID抽出失敗")
                return
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=10)
            if response.status_code != 200:
                self.log_message(f"❌ {script_info['management_id']}: HTTP {response.status_code}")
                return
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📊 {script_info['management_id']}: {len(df)}行 x {len(df.columns)}列")
            
            # 最初の10行を表示
            self.log_message(f"🔍 最初の10行:")
            for row_idx in range(min(10, len(df))):
                row = df.iloc[row_idx]
                row_data = []
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        if len(value_str) > 0:
                            if len(value_str) > 25:
                                value_str = value_str[:25] + "..."
                            row_data.append(f"列{col_idx}:'{value_str}'")
                
                if row_data:
                    self.log_message(f"  行{row_idx}: {' | '.join(row_data[:4])}")
            
            # ヘッダー検索
            character_headers = []
            dialogue_headers = []
            
            for row_idx in range(min(15, len(df))):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        
                        # キャラクターヘッダー
                        if any(keyword in value_str for keyword in ['キャラクター', 'キャラ', 'character']):
                            character_headers.append(f"行{row_idx}列{col_idx}")
                        
                        # セリフヘッダー
                        if any(keyword in value_str for keyword in ['セリフ', 'せりふ', 'dialogue']):
                            dialogue_headers.append(f"行{row_idx}列{col_idx}")
            
            self.log_message(f"📍 ヘッダー情報:")
            self.log_message(f"  キャラクターヘッダー: {character_headers if character_headers else '見つからず'}")
            self.log_message(f"  セリフヘッダー: {dialogue_headers if dialogue_headers else '見つからず'}")
            
            # データベースでの抽出状況確認
            self.check_database_status(script_info['management_id'])
            
        except Exception as e:
            self.log_message(f"❌ {script_info['management_id']}: 分析エラー - {str(e)}")
    
    def check_database_status(self, management_id):
        """データベースでの抽出状況確認"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(cdu.id) as char_count
                FROM scripts s
                LEFT JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE s.management_id = ?
                GROUP BY s.id
            """, (management_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                char_count = result[0]
                self.log_message(f"🗄️  データベース: {char_count}件のキャラクターデータ")
                if char_count < 5:
                    self.log_message(f"  ⚠️  キャラクターデータが少なく、抽出不備の可能性")
                else:
                    self.log_message(f"  ✅ 適切にキャラクターが抽出されています")
            else:
                self.log_message(f"🗄️  データベース: スクリプトが見つかりません")
            
        except Exception as e:
            self.log_message(f"❌ データベース確認エラー: {str(e)}")
    
    def run_comprehensive_analysis(self):
        """包括的分析実行"""
        self.log_message("=" * 80)
        self.log_message("URLリスト台本分析開始")
        self.log_message("=" * 80)
        
        # URLリストから台本URL抽出
        script_urls = self.extract_urls_from_list()
        
        if not script_urls:
            self.log_message("❌ 台本URLが見つかりませんでした")
            return
        
        # サンプル分析
        self.analyze_sample_scripts(script_urls, 10)
        
        # 2019年データの特別確認
        self.log_message(f"\n{'=' * 80}")
        self.log_message("2019年データ特別確認")
        self.log_message(f"{'=' * 80}")
        
        year_2019_scripts = [s for s in script_urls if '19/' in s.get('title', '') or 'B5' in s.get('management_id', '') or 'B6' in s.get('management_id', '')]
        
        if year_2019_scripts:
            self.log_message(f"🗓️ 2019年関連スクリプト: {len(year_2019_scripts)}件発見")
            self.analyze_sample_scripts(year_2019_scripts[:5], 5)
        else:
            self.log_message("🗓️ 2019年関連スクリプトは見つかりませんでした")
        
        self.log_message("=" * 80)
        self.log_message("分析完了")
        self.log_message("=" * 80)

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = UrlListExtractor(db_path)
    
    print("=== URLリスト台本分析ツール ===")
    
    # 包括分析実行
    extractor.run_comprehensive_analysis()
    
    print(f"\n✅ 分析完了！")
    print(f"詳細は url_list_extraction.txt を確認してください。")

if __name__ == "__main__":
    main()