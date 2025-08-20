#!/usr/bin/env python3
"""
未発見キャラクター発見ツール

指定されたスプレッドシートを詳細分析し、
データベースに存在しない全てのキャラクターを発見する
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class UndiscoveredCharacterFinder:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/undiscovered_characters.txt"
        
        # 指定されたスプレッドシート
        self.target_sheet = 'https://docs.google.com/spreadsheets/d/1ya6f0doYybdHZvyD4DXUJfGijxZ9InDE7C-QMpDJJFM/edit?gid=1384097767#gid=1384097767'
        
        # 除外キーワード
        self.exclusions = {
            'オープニング', 'アバン', 'エンディング', 'TRUE', 'FALSE', 'セリフ', 
            '不明', '登場キャラ', '', '・・・', '→→', 'シーン', 'セット_',
            'インターホン鳴らす', 'カット'
        }
        
        # 明らかな指示文パターン
        self.instruction_patterns = [
            r'^\[=.*\]$',  # Excel数式
            r'^https?://',  # URL
            r'.*撮影.*',  # 撮影指示
            r'.*編集.*',  # 編集指示
            r'.*音声.*',  # 音声指示
            r'.*SE.*',    # SE指示
            r'^\d+$',     # 数字のみ
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def get_database_characters(self):
        """データベース内の全キャラクター取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT character_name, COUNT(*) as count
                FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0
                GROUP BY character_name
                ORDER BY count DESC
            """)
            
            db_characters = set()
            for char_name, count in cursor.fetchall():
                db_characters.add(char_name)
            
            conn.close()
            
            self.log_message(f"📊 データベース内キャラクター: {len(db_characters)}種類")
            return db_characters
            
        except Exception as e:
            self.log_message(f"❌ データベース取得エラー: {str(e)}")
            return set()
    
    def extract_all_characters_from_sheet(self):
        """スプレッドシートから全キャラクター抽出"""
        try:
            # スプレッドシートID、GID抽出
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, self.target_sheet)
            gid_match = re.search(gid_pattern, self.target_sheet)
            
            if not sheet_match:
                return set()
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV出力URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # CSVデータ取得
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                self.log_message(f"❌ HTTP {response.status_code}")
                return set()
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📋 スプレッドシート: {len(df)}行 x {len(df.columns)}列")
            
            # キャラクター列を特定
            character_col = None
            header_row = None
            
            for row_idx in range(min(10, len(df))):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        if 'キャラクター' in value_str:
                            character_col = col_idx
                            header_row = row_idx
                            break
                if character_col is not None:
                    break
            
            if character_col is None:
                self.log_message("❌ キャラクター列が見つかりません")
                return set()
            
            self.log_message(f"✅ キャラクター列: {character_col}, ヘッダー行: {header_row}")
            
            # 全キャラクター名を抽出
            sheet_characters = set()
            character_details = []
            
            start_row = header_row + 1 if header_row is not None else 4
            
            for index in range(start_row, len(df)):
                row = df.iloc[index]
                
                if character_col < len(row):
                    character_value = row.iloc[character_col]
                    if pd.notna(character_value):
                        character_name = str(character_value).strip()
                        
                        if self.is_valid_character_name(character_name):
                            sheet_characters.add(character_name)
                            
                            # セリフも取得（3列目）
                            dialogue = ""
                            if len(row) > 3 and pd.notna(row.iloc[3]):
                                dialogue = str(row.iloc[3]).strip()[:50]
                            
                            character_details.append({
                                'row': index,
                                'name': character_name,
                                'dialogue': dialogue
                            })
            
            self.log_message(f"🎭 スプレッドシート内キャラクター: {len(sheet_characters)}種類")
            
            # サンプル表示
            self.log_message(f"\n📝 発見キャラクターサンプル（最初の20個）:")
            for i, detail in enumerate(character_details[:20]):
                self.log_message(f"  行{detail['row']}: {detail['name']} | {detail['dialogue']}")
            
            return sheet_characters, character_details
            
        except Exception as e:
            self.log_message(f"❌ スプレッドシート分析エラー: {str(e)}")
            return set(), []
    
    def is_valid_character_name(self, character_name):
        """キャラクター名の妥当性チェック"""
        if not character_name or len(character_name.strip()) == 0:
            return False
        
        char_name = character_name.strip()
        
        # 長すぎる場合は除外
        if len(char_name) > 50:
            return False
        
        # 除外キーワード
        if char_name in self.exclusions:
            return False
        
        # 指示文パターン
        for pattern in self.instruction_patterns:
            if re.match(pattern, char_name):
                return False
        
        # 明らかな指示文
        instruction_keywords = [
            'アップで', '手元で', '配置', 'イメージ', '組み立て', '見せる',
            '出す', '鳴らす', '寄り引き', 'スタジオ', '外側', '内側'
        ]
        
        if any(keyword in char_name for keyword in instruction_keywords):
            return False
        
        return True
    
    def find_undiscovered_characters(self):
        """未発見キャラクターの特定"""
        self.log_message("=" * 80)
        self.log_message("未発見キャラクター検索開始")
        self.log_message("=" * 80)
        
        # データベース内キャラクター取得
        db_characters = self.get_database_characters()
        
        # スプレッドシート内キャラクター取得
        sheet_characters, character_details = self.extract_all_characters_from_sheet()
        
        if not sheet_characters:
            self.log_message("❌ スプレッドシートからキャラクターを抽出できませんでした")
            return
        
        # 未発見キャラクターを特定
        undiscovered = sheet_characters - db_characters
        discovered = sheet_characters & db_characters
        
        self.log_message("\n" + "=" * 80)
        self.log_message("分析結果")
        self.log_message("=" * 80)
        
        self.log_message(f"📊 統計:")
        self.log_message(f"  スプレッドシート内: {len(sheet_characters)}種類")
        self.log_message(f"  データベース内: {len(db_characters)}種類")
        self.log_message(f"  既発見: {len(discovered)}種類")
        self.log_message(f"  未発見: {len(undiscovered)}種類")
        
        if discovered:
            self.log_message(f"\n✅ 既にデータベースに存在するキャラクター:")
            for char in sorted(discovered):
                self.log_message(f"  - {char}")
        
        if undiscovered:
            self.log_message(f"\n🔍 未発見キャラクター（データベースに存在しない）:")
            
            # 詳細情報付きで表示
            undiscovered_details = [d for d in character_details if d['name'] in undiscovered]
            
            for detail in undiscovered_details:
                self.log_message(f"  行{detail['row']}: 【{detail['name']}】")
                if detail['dialogue']:
                    self.log_message(f"    セリフ: \"{detail['dialogue']}\"")
            
            self.log_message(f"\n⚠️  これらのキャラクターは以下の理由で未発見の可能性があります:")
            self.log_message(f"  1. 抽出ロジックの制約により除外されている")
            self.log_message(f"  2. まだ再処理されていない")
            self.log_message(f"  3. スプレッドシート構造の問題")
            
        else:
            self.log_message(f"\n✅ 全てのキャラクターがデータベースに存在します！")
        
        return undiscovered, discovered
    
    def run_analysis(self):
        """分析実行"""
        self.log_message("指定スプレッドシート未発見キャラクター分析")
        self.log_message(f"対象: {self.target_sheet}")
        
        # 未発見キャラクター検索
        undiscovered, discovered = self.find_undiscovered_characters()
        
        return undiscovered, discovered

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    finder = UndiscoveredCharacterFinder(db_path)
    
    print("=== 未発見キャラクター発見ツール ===")
    
    # 分析実行
    undiscovered, discovered = finder.run_analysis()
    
    if undiscovered:
        print(f"\n⚠️  {len(undiscovered)}種類の未発見キャラクターがあります！")
    else:
        print(f"\n✅ 全キャラクター発見済みです")
    
    print(f"詳細は undiscovered_characters.txt を確認してください。")

if __name__ == "__main__":
    main()