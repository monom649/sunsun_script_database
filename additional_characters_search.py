#!/usr/bin/env python3
"""
追加キャラクター検索ツール

ジャン、ポールなどの追加ゲストキャラクターを
既存のスプレッドシートから検索する
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class AdditionalCharactersSearch:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/additional_characters_search.txt"
        
        # 追加で検索するキャラクター
        self.additional_characters = {
            'ジャン', 'ポール', 'マリー', 'ピエール', 'ルイ', 'アンヌ',
            'Jean', 'Paul', 'Marie', 'Pierre', 'Louis', 'Anne',
            'じゃん', 'ぽーる', 'まりー', 'ぴえーる', 'るい', 'あんぬ',
            'シャルル', 'フランソワ', 'ニコラ', 'ジャック', 'ミシェル',
            'Charles', 'François', 'Nicolas', 'Jacques', 'Michel',
            'しゃるる', 'ふらんそわ', 'にこら', 'じゃっく', 'みしぇる',
            'エミリー', 'ソフィー', 'クレア', 'ジュリー', 'ナタリー',
            'Emily', 'Sophie', 'Claire', 'Julie', 'Natalie',
            'えみりー', 'そふぃー', 'くれあ', 'じゅりー', 'なたりー'
        }
        
        # 日本のゲストキャラクター候補
        self.japanese_guests = {
            'たろう', 'じろう', 'さぶろう', 'しろう', 'ごろう',
            'はなこ', 'ゆみこ', 'けいこ', 'みちこ', 'のりこ',
            'タロウ', 'ジロウ', 'サブロウ', 'シロウ', 'ゴロウ',
            'ハナコ', 'ユミコ', 'ケイコ', 'ミチコ', 'ノリコ',
            '太郎', '次郎', '三郎', '四郎', '五郎',
            '花子', '由美子', '恵子', '道子', '典子',
            'けんじ', 'ひろし', 'たけし', 'まさし', 'さとし',
            'ケンジ', 'ヒロシ', 'タケシ', 'マサシ', 'サトシ',
            '健二', '浩', '武', '正', '聡'
        }
        
        # 動物系ゲストキャラクター
        self.animal_guests = {
            'ねこちゃん', 'わんちゃん', 'うさちゃん', 'くまちゃん',
            'ネコちゃん', 'ワンちゃん', 'ウサちゃん', 'クマちゃん',
            'ぞうさん', 'らいおん', 'きりんさん', 'ぱんださん',
            'ゾウさん', 'ライオン', 'キリンさん', 'パンダさん',
            'うま', 'とり', 'さかな', 'かめ', 'へび',
            'ウマ', 'トリ', 'サカナ', 'カメ', 'ヘビ',
            '馬', '鳥', '魚', '亀', '蛇'
        }
        
        # 全検索対象
        self.all_search_targets = self.additional_characters | self.japanese_guests | self.animal_guests
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def search_in_database(self):
        """データベースで追加キャラクター検索"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            found_characters = {}
            
            for character in self.all_search_targets:
                cursor.execute("""
                    SELECT COUNT(*) as count, COUNT(DISTINCT script_id) as script_count
                    FROM character_dialogue_unified 
                    WHERE character_name = ? OR character_name LIKE ?
                """, (character, f'%{character}%'))
                
                result = cursor.fetchone()
                if result and result[0] > 0:
                    found_characters[character] = {
                        'count': result[0],
                        'scripts': result[1]
                    }
            
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("データベース内追加キャラクター検索結果")
            self.log_message("=" * 80)
            
            if found_characters:
                self.log_message(f"🎭 発見された追加キャラクター: {len(found_characters)}種類")
                
                # カテゴリ別表示
                self.log_message("\n🌍 外国系キャラクター:")
                for char in found_characters:
                    if char in self.additional_characters:
                        data = found_characters[char]
                        self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
                
                self.log_message("\n🇯🇵 日本系キャラクター:")
                for char in found_characters:
                    if char in self.japanese_guests:
                        data = found_characters[char]
                        self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
                
                self.log_message("\n🐾 動物系キャラクター:")
                for char in found_characters:
                    if char in self.animal_guests:
                        data = found_characters[char]
                        self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
            else:
                self.log_message("❌ 追加キャラクターは現在のデータベースに見つかりませんでした")
            
            return found_characters
            
        except Exception as e:
            self.log_message(f"❌ データベース検索エラー: {str(e)}")
            return {}
    
    def search_sample_spreadsheets(self, sample_count=20):
        """サンプルスプレッドシートでの検索"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # ランダムなスクリプト取得
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                WHERE s.script_url IS NOT NULL AND s.script_url != ''
                ORDER BY RANDOM()
                LIMIT ?
            """, (sample_count,))
            
            scripts = cursor.fetchall()
            conn.close()
            
            self.log_message(f"\n📊 {len(scripts)}個のランダムスプレッドシートで追加キャラクター検索")
            
            found_in_sheets = {}
            
            for i, (management_id, title, script_url) in enumerate(scripts):
                if i % 5 == 0:
                    self.log_message(f"🔍 検索進行: {i+1}/{len(scripts)}")
                
                found = self.search_single_spreadsheet(script_url, management_id)
                if found:
                    found_in_sheets[management_id] = found
            
            self.log_message("\n📋 スプレッドシート検索結果:")
            if found_in_sheets:
                for script_id, characters in found_in_sheets.items():
                    self.log_message(f"  {script_id}: {', '.join(characters)}")
            else:
                self.log_message("  ❌ サンプルスプレッドシートで追加キャラクターは見つかりませんでした")
            
            return found_in_sheets
            
        except Exception as e:
            self.log_message(f"❌ スプレッドシート検索エラー: {str(e)}")
            return {}
    
    def search_single_spreadsheet(self, script_url, management_id):
        """単一スプレッドシートでの検索"""
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
            
            found_characters = []
            
            # 全セルをスキャンして追加キャラクター検索
            for row_idx in range(len(df)):
                row = df.iloc[row_idx]
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        value_str = str(value).strip()
                        
                        # 追加キャラクターかチェック
                        if value_str in self.all_search_targets:
                            found_characters.append(value_str)
            
            return list(set(found_characters))  # 重複除去
            
        except:
            return []
    
    def run_comprehensive_search(self):
        """包括的検索実行"""
        self.log_message("=" * 80)
        self.log_message("追加キャラクター包括検索開始")
        self.log_message("=" * 80)
        self.log_message(f"検索対象: {len(self.all_search_targets)}種類のキャラクター")
        self.log_message(f"  外国系: {len(self.additional_characters)}種類")
        self.log_message(f"  日本系: {len(self.japanese_guests)}種類") 
        self.log_message(f"  動物系: {len(self.animal_guests)}種類")
        
        # 1. データベース検索
        db_results = self.search_in_database()
        
        # 2. スプレッドシート検索
        sheet_results = self.search_sample_spreadsheets(20)
        
        # 3. 結果まとめ
        self.log_message("\n" + "=" * 80)
        self.log_message("検索結果サマリー")
        self.log_message("=" * 80)
        
        total_found = len(db_results) + len([char for chars in sheet_results.values() for char in chars])
        
        if total_found > 0:
            self.log_message(f"✅ 追加キャラクター発見: {total_found}種類")
            if db_results:
                self.log_message(f"  データベース内: {len(db_results)}種類")
            if sheet_results:
                sheet_chars = set([char for chars in sheet_results.values() for char in chars])
                self.log_message(f"  スプレッドシート内: {len(sheet_chars)}種類")
        else:
            self.log_message("❌ 検索対象の追加キャラクターは見つかりませんでした")
            self.log_message("ℹ️  これは以下の理由が考えられます:")
            self.log_message("   - これらのキャラクターがSunSun Kids TVに登場していない")
            self.log_message("   - 異なる名前やスペルで記録されている")
            self.log_message("   - まだ再処理されていないスクリプトに含まれている")
        
        return db_results, sheet_results

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    searcher = AdditionalCharactersSearch(db_path)
    
    print("=== 追加キャラクター検索ツール ===")
    
    # 包括検索実行
    db_results, sheet_results = searcher.run_comprehensive_search()
    
    print(f"\n✅ 検索完了！")
    print(f"詳細は additional_characters_search.txt を確認してください。")

if __name__ == "__main__":
    main()