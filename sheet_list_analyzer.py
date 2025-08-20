#!/usr/bin/env python3
"""
シートリスト分析ツール

ユーザー指定の新しいスプレッドシートと
2019年台本データの構造を分析する
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class SheetListAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/sheet_list_analysis.txt"
        
        # 新しく指定されたスプレッドシート
        self.target_sheet = {
            'url': 'https://docs.google.com/spreadsheets/d/1c_txRaInj7yQUFBZLBSj65pLzhKxHofsobLJyM065g8/edit?gid=1092002230#gid=1092002230',
            'name': 'NewSheet'
        }
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def analyze_sheet_detailed(self, sheet_url, sheet_name):
        """詳細なシート分析"""
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, sheet_url)
            gid_match = re.search(gid_pattern, sheet_url)
            
            if not sheet_match:
                self.log_message(f"❌ {sheet_name}: スプレッドシートIDを抽出できません")
                return None
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            self.log_message(f"📊 {sheet_name}: ID={spreadsheet_id}, GID={gid}")
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                self.log_message(f"❌ {sheet_name}: HTTP {response.status_code}")
                return None
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📋 {sheet_name}: {len(df)}行 x {len(df.columns)}列")
            
            # 最初の15行を詳細表示
            self.log_message(f"\n🔍 {sheet_name}: 最初の15行の内容:")
            for row_idx in range(min(15, len(df))):
                row = df.iloc[row_idx]
                row_data = []
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        if len(value_str) > 0:
                            # 長いテキストは短縮
                            if len(value_str) > 30:
                                value_str = value_str[:30] + "..."
                            row_data.append(f"列{col_idx}:'{value_str}'")
                
                if row_data:
                    self.log_message(f"  行{row_idx}: {' | '.join(row_data[:6])}")  # 最初の6列まで
            
            # ヘッダー候補の詳細検索
            header_candidates = {
                'character': [],
                'dialogue': [],
                'filming': [],
                'audio': [],
                'other': []
            }
            
            for row_idx in range(min(20, len(df))):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        original_str = str(value).strip()
                        
                        # キャラクターヘッダー候補
                        char_keywords = ['キャラクター', 'キャラ', 'character', '話者', '登場人物', '登場キャラ']
                        if any(keyword in value_str for keyword in char_keywords):
                            header_candidates['character'].append({
                                'row': row_idx, 'col': col_idx, 'text': original_str
                            })
                        
                        # セリフヘッダー候補
                        dialogue_keywords = ['セリフ', 'せりふ', 'dialogue', 'ダイアログ', '発言', '台詞']
                        if any(keyword in value_str for keyword in dialogue_keywords):
                            header_candidates['dialogue'].append({
                                'row': row_idx, 'col': col_idx, 'text': original_str
                            })
                        
                        # 撮影指示ヘッダー候補
                        filming_keywords = ['撮影指示', '撮影', 'filming', '映像指示', '映像']
                        if any(keyword in value_str for keyword in filming_keywords):
                            header_candidates['filming'].append({
                                'row': row_idx, 'col': col_idx, 'text': original_str
                            })
                        
                        # 音声指示ヘッダー候補
                        audio_keywords = ['音声指示', '音声', 'audio', '効果音']
                        if any(keyword in value_str for keyword in audio_keywords):
                            header_candidates['audio'].append({
                                'row': row_idx, 'col': col_idx, 'text': original_str
                            })
                        
                        # その他の興味深いヘッダー
                        other_keywords = ['no.', 'no', '番号', 'タイトル', 'title', '時間', 'time']
                        if any(keyword in value_str for keyword in other_keywords):
                            header_candidates['other'].append({
                                'row': row_idx, 'col': col_idx, 'text': original_str
                            })
            
            # ヘッダー候補の報告
            self.log_message(f"\n📍 {sheet_name}: ヘッダー候補詳細:")
            for header_type, candidates in header_candidates.items():
                if candidates:
                    self.log_message(f"  {header_type}: {len(candidates)}個")
                    for candidate in candidates:
                        self.log_message(f"    行{candidate['row']}列{candidate['col']}: '{candidate['text']}'")
                else:
                    self.log_message(f"  {header_type}: 見つかりません")
            
            # 潜在的なキャラクター名の検索
            potential_characters = set()
            known_characters = {
                'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'BB', 'プリル', 
                'カエルン', 'ウサッチ', 'モーレツ先生', 'そーだおじさん'
            }
            
            for row_idx in range(len(df)):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        if value_str in known_characters:
                            potential_characters.add(value_str)
            
            if potential_characters:
                self.log_message(f"\n🎭 {sheet_name}: 発見された既知キャラクター:")
                for char in sorted(potential_characters):
                    self.log_message(f"  - {char}")
            else:
                self.log_message(f"\n🎭 {sheet_name}: 既知キャラクターは見つかりませんでした")
            
            return {
                'sheet_name': sheet_name,
                'total_rows': len(df),
                'total_cols': len(df.columns),
                'header_candidates': header_candidates,
                'potential_characters': potential_characters,
                'dataframe': df
            }
            
        except Exception as e:
            self.log_message(f"❌ {sheet_name}: 分析エラー - {str(e)}")
            return None
    
    def check_2019_data_in_database(self):
        """2019年データのデータベース確認"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 2019年のスクリプトを検索
            cursor.execute("""
                SELECT s.management_id, s.title, s.script_url,
                       COUNT(cdu.id) as character_count
                FROM scripts s
                LEFT JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE s.management_id LIKE '%2019%' 
                   OR s.title LIKE '%2019%'
                   OR s.management_id LIKE 'A%'
                GROUP BY s.id, s.management_id, s.title, s.script_url
                ORDER BY s.management_id
                LIMIT 20
            """)
            
            results = cursor.fetchall()
            conn.close()
            
            self.log_message("\n🗓️ 2019年関連データのデータベース状況:")
            if results:
                for mgmt_id, title, script_url, char_count in results:
                    self.log_message(f"  {mgmt_id}: {char_count}件 - {title[:50]}...")
                    if script_url:
                        self.log_message(f"    URL: {script_url[:80]}...")
            else:
                self.log_message("  ❌ 2019年関連データが見つかりませんでした")
            
            return results
            
        except Exception as e:
            self.log_message(f"❌ データベース確認エラー: {str(e)}")
            return []
    
    def run_comprehensive_analysis(self):
        """包括的分析実行"""
        self.log_message("=" * 80)
        self.log_message("新しいスプレッドシートと2019年データの包括分析開始")
        self.log_message("=" * 80)
        
        # 新しいスプレッドシートの分析
        self.log_message(f"\n{'=' * 60}")
        self.log_message(f"新しいスプレッドシート分析")
        self.log_message(f"URL: {self.target_sheet['url']}")
        self.log_message(f"{'=' * 60}")
        
        new_sheet_analysis = self.analyze_sheet_detailed(
            self.target_sheet['url'], 
            self.target_sheet['name']
        )
        
        # 2019年データの確認
        self.log_message(f"\n{'=' * 60}")
        self.log_message(f"2019年データベース確認")
        self.log_message(f"{'=' * 60}")
        
        db_2019_results = self.check_2019_data_in_database()
        
        # 総合判定
        self.log_message("\n" + "=" * 80)
        self.log_message("総合分析結果")
        self.log_message("=" * 80)
        
        if new_sheet_analysis:
            char_headers = len(new_sheet_analysis['header_candidates']['character'])
            dialogue_headers = len(new_sheet_analysis['header_candidates']['dialogue'])
            potential_chars = len(new_sheet_analysis['potential_characters'])
            
            self.log_message(f"📊 新しいスプレッドシート:")
            self.log_message(f"  キャラクターヘッダー: {char_headers}個")
            self.log_message(f"  セリフヘッダー: {dialogue_headers}個")
            self.log_message(f"  発見されたキャラクター: {potential_chars}個")
            
            if char_headers == 0 or dialogue_headers == 0:
                self.log_message(f"  ⚠️  このスプレッドシートは抽出が困難な可能性があります")
            else:
                self.log_message(f"  ✅ このスプレッドシートは抽出可能と思われます")
        
        self.log_message(f"📊 2019年データベース:")
        self.log_message(f"  発見されたスクリプト: {len(db_2019_results)}個")
        
        if db_2019_results:
            low_char_count = sum(1 for _, _, _, count in db_2019_results if count < 10)
            if low_char_count > len(db_2019_results) * 0.5:
                self.log_message(f"  ⚠️  多くのスクリプトでキャラクター数が少なく、抽出不備の可能性があります")
            else:
                self.log_message(f"  ✅ 多くのスクリプトで適切にキャラクターが抽出されています")
        
        self.log_message("=" * 80)

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    analyzer = SheetListAnalyzer(db_path)
    
    print("=== シートリスト分析ツール ===")
    
    # 包括分析実行
    analyzer.run_comprehensive_analysis()
    
    print(f"\n✅ 分析完了！")
    print(f"詳細は sheet_list_analysis.txt を確認してください。")

if __name__ == "__main__":
    main()