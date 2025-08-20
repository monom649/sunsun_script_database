#!/usr/bin/env python3
"""
指定スプレッドシート分析ツール

ユーザーが指定した具体的なスプレッドシートを分析して
キャラクター抽出の問題を特定する。
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class SpecificSheetAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/specific_sheet_analysis.txt"
        
        # ユーザー指定のスプレッドシート
        self.target_sheets = [
            {
                'url': 'https://docs.google.com/spreadsheets/d/1eiHiiJIANjUEm0z1mrQKoaQzFqHAcEiGKzwBVs0fxmc/edit?gid=2036772822#gid=2036772822',
                'name': 'Sheet1'
            },
            {
                'url': 'https://docs.google.com/spreadsheets/d/1ya6f0doYybdHZvyD4DXUJfGijxZ9InDE7C-QMpDJJFM/edit?gid=1384097767#gid=1384097767',
                'name': 'Sheet2'
            }
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def analyze_sheet_structure(self, sheet_url, sheet_name):
        """スプレッドシート構造の詳細分析"""
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
            
            self.log_message(f"📊 {sheet_name}: スプレッドシートID={spreadsheet_id}, GID={gid}")
            
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
            
            # 最初の10行を詳細表示
            self.log_message(f"\n🔍 {sheet_name}: 最初の10行の内容:")
            for row_idx in range(min(10, len(df))):
                row = df.iloc[row_idx]
                row_data = []
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        if len(value_str) > 0:
                            row_data.append(f"列{col_idx}:'{value_str}'")
                
                if row_data:
                    self.log_message(f"  行{row_idx}: {' | '.join(row_data[:5])}")  # 最初の5列まで
            
            # ヘッダー検索
            character_candidates = []
            dialogue_candidates = []
            
            for row_idx in range(min(15, len(df))):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        
                        # キャラクターヘッダー候補
                        if any(keyword in value_str for keyword in ['キャラクター', 'キャラ', 'character', '話者', '登場人物']):
                            character_candidates.append({
                                'row': row_idx,
                                'col': col_idx,
                                'text': str(value).strip()
                            })
                        
                        # セリフヘッダー候補
                        if any(keyword in value_str for keyword in ['セリフ', 'せりふ', 'dialogue', 'ダイアログ', '発言', '台詞']):
                            dialogue_candidates.append({
                                'row': row_idx,
                                'col': col_idx,
                                'text': str(value).strip()
                            })
            
            self.log_message(f"\n📍 {sheet_name}: ヘッダー候補:")
            self.log_message(f"  キャラクター列候補: {len(character_candidates)}個")
            for candidate in character_candidates:
                self.log_message(f"    行{candidate['row']}列{candidate['col']}: '{candidate['text']}'")
            
            self.log_message(f"  セリフ列候補: {len(dialogue_candidates)}個")
            for candidate in dialogue_candidates:
                self.log_message(f"    行{candidate['row']}列{candidate['col']}: '{candidate['text']}'")
            
            # 最も可能性の高いヘッダーを特定
            best_char_header = None
            best_dialogue_header = None
            
            if character_candidates:
                # 「キャラクター」を含む最初の候補
                for candidate in character_candidates:
                    if 'キャラクター' in candidate['text']:
                        best_char_header = candidate
                        break
                if not best_char_header:
                    best_char_header = character_candidates[0]
            
            if dialogue_candidates:
                # 「セリフ」を含む最初の候補
                for candidate in dialogue_candidates:
                    if 'セリフ' in candidate['text']:
                        best_dialogue_header = candidate
                        break
                if not best_dialogue_header:
                    best_dialogue_header = dialogue_candidates[0]
            
            if best_char_header:
                self.log_message(f"\n✅ {sheet_name}: 採用キャラクター列: 行{best_char_header['row']}列{best_char_header['col']} '{best_char_header['text']}'")
                
                # キャラクター列のデータサンプル
                char_col = best_char_header['col']
                start_row = best_char_header['row'] + 1
                
                self.log_message(f"📝 {sheet_name}: キャラクター列データサンプル:")
                sample_count = 0
                
                for row_idx in range(start_row, min(start_row + 20, len(df))):
                    if row_idx < len(df) and char_col < len(df.iloc[row_idx]):
                        value = df.iloc[row_idx, char_col]
                        if pd.notna(value):
                            value_str = str(value).strip()
                            if len(value_str) > 0:
                                self.log_message(f"  行{row_idx}: '{value_str}'")
                                sample_count += 1
                                if sample_count >= 10:
                                    break
            else:
                self.log_message(f"❌ {sheet_name}: キャラクター列が見つかりません")
            
            return {
                'sheet_name': sheet_name,
                'total_rows': len(df),
                'total_cols': len(df.columns),
                'character_header': best_char_header,
                'dialogue_header': best_dialogue_header,
                'dataframe': df
            }
            
        except Exception as e:
            self.log_message(f"❌ {sheet_name}: 分析エラー - {str(e)}")
            return None
    
    def check_database_extraction(self, sheet_url):
        """データベースでの抽出状況確認"""
        try:
            # スプレッドシートIDを抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            sheet_match = re.search(sheet_pattern, sheet_url)
            
            if not sheet_match:
                return None
            
            spreadsheet_id = sheet_match.group(1)
            
            # データベースで該当スクリプト検索
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT s.management_id, s.title, s.script_url,
                       COUNT(cdu.id) as character_count
                FROM scripts s
                LEFT JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE s.script_url LIKE ?
                GROUP BY s.id, s.management_id, s.title, s.script_url
            """, (f'%{spreadsheet_id}%',))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
            
        except Exception as e:
            self.log_message(f"❌ データベース確認エラー: {str(e)}")
            return None
    
    def run_analysis(self):
        """指定スプレッドシート分析実行"""
        self.log_message("=" * 80)
        self.log_message("指定スプレッドシート詳細分析開始")
        self.log_message("=" * 80)
        
        for sheet_info in self.target_sheets:
            self.log_message(f"\n{'=' * 60}")
            self.log_message(f"分析対象: {sheet_info['name']}")
            self.log_message(f"URL: {sheet_info['url']}")
            self.log_message(f"{'=' * 60}")
            
            # スプレッドシート構造分析
            analysis = self.analyze_sheet_structure(sheet_info['url'], sheet_info['name'])
            
            # データベース抽出状況確認
            db_results = self.check_database_extraction(sheet_info['url'])
            
            if db_results:
                self.log_message(f"\n🗄️  {sheet_info['name']}: データベース抽出状況:")
                for mgmt_id, title, script_url, char_count in db_results:
                    self.log_message(f"  {mgmt_id}: {char_count}件のキャラクターデータ")
                    self.log_message(f"    タイトル: {title}")
            else:
                self.log_message(f"\n❌ {sheet_info['name']}: データベースに該当スクリプトが見つかりません")
        
        self.log_message("\n" + "=" * 80)
        self.log_message("指定スプレッドシート分析完了")
        self.log_message("=" * 80)

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    analyzer = SpecificSheetAnalyzer(db_path)
    
    print("=== 指定スプレッドシート分析ツール ===")
    
    # 分析実行
    analyzer.run_analysis()
    
    print(f"\n✅ 分析完了！")
    print(f"詳細は specific_sheet_analysis.txt を確認してください。")

if __name__ == "__main__":
    main()