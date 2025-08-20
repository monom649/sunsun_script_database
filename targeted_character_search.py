#!/usr/bin/env python3
"""
特定キャラクター検索ツール

カエルン、ウサッチ、モーレツ先生を重点的に検索し、
より効率的に発見を試みる。
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class TargetedCharacterSearch:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/targeted_search.txt"
        
        # ターゲットキャラクター
        self.target_characters = ['カエルン', 'ウサッチ', 'モーレツ先生']
        
        # より緩い検証条件
        self.max_length = 50  # より長い名前を許可
        
        # 必須除外パターンのみ
        self.strict_exclusions = [
            r'^TRUE$', r'^FALSE$', r'^セリフ$', r'^不明$',
            r'^https?://', r'^\[=.*\]$'
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_potential_target(self, text):
        """ターゲットキャラクターの可能性チェック"""
        if not text or len(text.strip()) == 0:
            return False
        
        text = text.strip()
        
        # 長すぎる場合は除外
        if len(text) > self.max_length:
            return False
        
        # 必須除外パターン
        for pattern in self.strict_exclusions:
            if re.match(pattern, text):
                return False
        
        # ターゲット文字列を含むかチェック
        for target in self.target_characters:
            if target in text:
                return True
        
        return False
    
    def search_spreadsheet_for_targets(self, script_url, management_id):
        """スプレッドシート内でターゲットキャラクター検索"""
        if not script_url or 'docs.google.com' not in script_url:
            return []
        
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return []
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=10)
            if response.status_code != 200:
                return []
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            found_targets = []
            
            # 全セルをスキャン
            for row_idx in range(len(df)):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        
                        if self.is_potential_target(value_str):
                            # 隣接セルからコンテキスト取得
                            context_cells = []
                            for offset in [-1, 1]:
                                adj_col = col_idx + offset
                                if 0 <= adj_col < len(row):
                                    adj_value = row.iloc[adj_col]
                                    if pd.notna(adj_value):
                                        context_cells.append(str(adj_value).strip()[:50])
                            
                            found_targets.append({
                                'character': value_str,
                                'row': row_idx,
                                'col': col_idx,
                                'context': ' | '.join(context_cells)
                            })
            
            if found_targets:
                self.log_message(f"🎯 {management_id}: {len(found_targets)}個のターゲット候補を発見")
                for target in found_targets:
                    self.log_message(f"  行{target['row']}列{target['col']}: '{target['character']}' | {target['context']}")
            
            return found_targets
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: 検索エラー - {str(e)}")
            return []
    
    def run_targeted_search(self, max_scripts=200):
        """ターゲット検索実行"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # スクリプト一覧取得
            cursor.execute("""
                SELECT DISTINCT management_id, title, script_url
                FROM scripts
                WHERE script_url IS NOT NULL AND script_url != ''
                ORDER BY management_id
                LIMIT ?
            """, (max_scripts,))
            
            scripts = cursor.fetchall()
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("ターゲットキャラクター特化検索開始")
            self.log_message("=" * 80)
            self.log_message(f"検索対象: {', '.join(self.target_characters)}")
            self.log_message(f"処理スクリプト数: {len(scripts)}")
            
            all_findings = []
            
            for i, (management_id, title, script_url) in enumerate(scripts):
                if i % 20 == 0:
                    self.log_message(f"🔍 進行状況: {i+1}/{len(scripts)}")
                
                findings = self.search_spreadsheet_for_targets(script_url, management_id)
                
                if findings:
                    for finding in findings:
                        finding['script_id'] = management_id
                        finding['title'] = title
                        all_findings.append(finding)
            
            # 結果整理
            self.log_message("=" * 80)
            self.log_message("検索結果サマリー")
            self.log_message("=" * 80)
            
            if all_findings:
                # ターゲット別集計
                target_counts = {}
                for target in self.target_characters:
                    matching = [f for f in all_findings if target in f['character']]
                    target_counts[target] = matching
                
                for target, matches in target_counts.items():
                    if matches:
                        self.log_message(f"✅ {target}: {len(matches)}件発見")
                        for match in matches[:5]:  # 最初の5件表示
                            self.log_message(f"  {match['script_id']}: '{match['character']}' | {match['context']}")
                    else:
                        self.log_message(f"❌ {target}: 見つかりませんでした")
                
                self.log_message(f"\n📋 総発見数: {len(all_findings)}件")
            else:
                self.log_message("❌ ターゲットキャラクターは見つかりませんでした")
            
            return all_findings
            
        except Exception as e:
            self.log_message(f"❌ 検索エラー: {str(e)}")
            return []

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    searcher = TargetedCharacterSearch(db_path)
    
    print("=== ターゲットキャラクター特化検索 ===")
    
    # ターゲット検索実行
    results = searcher.run_targeted_search()
    
    if results:
        print(f"\n✅ 検索完了！発見件数: {len(results)}")
    else:
        print(f"\n⚠️  ターゲットキャラクターは見つかりませんでした")
    
    print(f"詳細は targeted_search.txt を確認してください。")

if __name__ == "__main__":
    main()