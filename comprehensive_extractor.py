#!/usr/bin/env python3
"""
包括的キャラクター抽出ツール

全ての問題を解決する改良版抽出ロジック:
1. より柔軟なヘッダー検出
2. 緩和された文字制限
3. より正確な指示文除外
4. ゲストキャラクター特別処理
5. 複数のフォールバック機能
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

class ComprehensiveExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/comprehensive_extraction.txt"
        
        # 定型キャラクター
        self.standard_characters = {
            'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'BB', 'ママ', 'パパ', 
            'みんな', 'こども', '子ども', 'SE', '（SE）', '(SE)'
        }
        
        # 既知のゲストキャラクター（許可リスト）
        self.known_guest_characters = {
            'プリル', 'カエルン', 'ウサッチ', 'モーレツ先生', 'そーだおじさん',
            'アイス姉さん', 'ゾンビ', 'サンタ', 'レッツプレイ！', 'どきどき！',
            'ター子', 'おりがみ夫人', '赤鬼', '犬ガンマン', 'いぬガンマン',
            'リーラ', 'ナレーション', 'ナレ', 'ねこ', 'いぬ', 'うさぎ', 'ぞう',
            'ライオン', 'ぱんだ', 'ペンギン', 'きりん', 'しまうま', 'かば',
            'アバン', 'エンディング', 'オープニング'
        }
        
        # 厳密な除外パターン（完全一致）
        self.strict_exclusions = {
            'TRUE', 'FALSE', 'セリフ', '不明', '登場キャラ', '',
            '・・・', '→→', '＜＜', '＞＞'
        }
        
        # 部分一致での除外パターン
        self.partial_exclusions = [
            r'^\[=.*\]$',  # Excel数式
            r'^https?://',  # URL
            r'.*カット.*カット.*',  # 複数のカット指示
            r'.*映像.*映像.*',  # 複数の映像指示
            r'.*編集.*編集.*',  # 複数の編集指示
            r'^・.*・.*',  # 複数の箇条書き
        ]
        
        # 明らかな指示文キーワード（内容で判断）
        self.instruction_keywords = [
            '以降基本的に', '背景', 'イメージです', '撮影', '編集', 
            'アップで見せたい', '手元で撮影', '配置', 'その手前で'
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
        
        # 1. 長さ制限を大幅緩和（50文字まで）
        if len(char_name) > 50:
            return False
        
        # 2. 厳密な除外（完全一致）
        if char_name in self.strict_exclusions:
            return False
        
        # 3. 既知のキャラクターは常に許可
        if char_name in self.standard_characters or char_name in self.known_guest_characters:
            return True
        
        # 4. 部分一致での除外パターン
        for pattern in self.partial_exclusions:
            if re.match(pattern, char_name):
                return False
        
        # 5. 明らかな指示文の除外
        if any(keyword in char_name for keyword in self.instruction_keywords):
            return False
        
        # 6. 数字のみの場合は除外
        if char_name.isdigit():
            return False
        
        # 7. 特殊記号のみの場合は除外
        if re.match(r'^[・→（）\-\s]+$', char_name):
            return False
        
        return True
    
    def find_headers_flexible(self, df):
        """より柔軟なヘッダー検索"""
        character_col = None
        dialogue_col = None
        filming_col = None
        audio_col = None
        header_row = None
        
        # より多くの行を検索（20行まで）
        search_rows = min(20, len(df))
        
        for row_idx in range(search_rows):
            row = df.iloc[row_idx]
            
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    
                    # キャラクターヘッダー検索（より柔軟に）
                    char_keywords = ['キャラクター', 'キャラ', 'character', '話者', '登場人物', '登場キャラ']
                    if any(keyword in value_str for keyword in char_keywords):
                        # Excel数式でないことを確認
                        if not value_str.startswith('[=') and not value_str.startswith('='):
                            character_col = col_idx
                            header_row = row_idx
                    
                    # セリフヘッダー検索
                    dialogue_keywords = ['セリフ', 'せりふ', 'dialogue', 'ダイアログ', '発言', '台詞']
                    if any(keyword in value_str for keyword in dialogue_keywords):
                        dialogue_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # 撮影指示ヘッダー検索
                    filming_keywords = ['撮影指示', '撮影', 'filming', '映像指示', '映像']
                    if any(keyword in value_str for keyword in filming_keywords):
                        filming_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # 音声指示ヘッダー検索
                    audio_keywords = ['音声指示', '音声', 'audio', '効果音']
                    if any(keyword in value_str for keyword in audio_keywords):
                        audio_col = col_idx
                        if header_row is None:
                            header_row = row_idx
        
        return character_col, dialogue_col, filming_col, audio_col, header_row
    
    def extract_comprehensive_data(self, script_url, management_id):
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
                self.log_message(f"⚠️  {management_id}: HTTP {response.status_code}")
                return None
            
            # CSV解析
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"📊 {management_id}: {len(df)}行 x {len(df.columns)}列")
            
            # 柔軟なヘッダー検索
            character_col, dialogue_col, filming_col, audio_col, header_row = self.find_headers_flexible(df)
            
            if character_col is None:
                # ヘッダーが見つからない場合の代替処理
                self.log_message(f"⚠️  {management_id}: ヘッダー検出失敗、代替抽出を試行")
                return self.fallback_extraction(df, management_id)
            
            self.log_message(f"✅ {management_id}: キャラ列{character_col}, セリフ列{dialogue_col}, ヘッダー行{header_row}")
            
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
                dialogue_text = \"\"\n                if dialogue_col is not None and dialogue_col < len(row):\n                    dialogue_value = row.iloc[dialogue_col]\n                    if pd.notna(dialogue_value):\n                        dialogue_text = str(dialogue_value).strip()\n                \n                # 指示文取得\n                filming_instructions = \"\"\n                if filming_col is not None and filming_col < len(row):\n                    filming_value = row.iloc[filming_col]\n                    if pd.notna(filming_value):\n                        filming_instructions = str(filming_value).strip()\n                \n                audio_instructions = \"\"\n                if audio_col is not None and audio_col < len(row):\n                    audio_value = row.iloc[audio_col]\n                    if pd.notna(audio_value):\n                        audio_instructions = str(audio_value).strip()\n                \n                combined_instructions = \"\"\n                if filming_instructions and audio_instructions:\n                    combined_instructions = f\"{filming_instructions} / {audio_instructions}\"\n                elif filming_instructions:\n                    combined_instructions = filming_instructions\n                elif audio_instructions:\n                    combined_instructions = audio_instructions\n                \n                entries.append({\n                    'row_number': index,\n                    'character_name': character_name,\n                    'dialogue_text': dialogue_text,\n                    'filming_audio_instructions': combined_instructions\n                })\n            \n            # 統計情報\n            standard_count = sum(1 for e in entries if e['character_name'] in self.standard_characters)\n            guest_count = sum(1 for e in entries if e['character_name'] in self.known_guest_characters)\n            other_count = len(entries) - standard_count - guest_count\n            \n            self.log_message(f\"📝 {management_id}: 定型{standard_count}、ゲスト{guest_count}、その他{other_count}件抽出\")\n            \n            # ゲストキャラクターを特別ログ\n            guest_chars = [e['character_name'] for e in entries if e['character_name'] in self.known_guest_characters]\n            if guest_chars:\n                unique_guests = list(set(guest_chars))\n                self.log_message(f\"🎭 {management_id}: ゲストキャラ発見 - {', '.join(unique_guests)}\")\n            \n            return entries if len(entries) > 0 else None\n            \n        except Exception as e:\n            self.log_message(f\"❌ {management_id}: 抽出エラー - {str(e)}\")\n            return None\n    \n    def fallback_extraction(self, df, management_id):\n        \"\"\"ヘッダー検出失敗時の代替抽出\"\"\"\n        self.log_message(f\"🔄 {management_id}: 代替抽出モード\")\n        \n        entries = []\n        \n        # 全セルをスキャンして既知のキャラクターを探す\n        for row_idx in range(len(df)):\n            row = df.iloc[row_idx]\n            for col_idx, value in enumerate(row):\n                if pd.notna(value):\n                    value_str = str(value).strip()\n                    \n                    # 既知のキャラクターかチェック\n                    if (value_str in self.standard_characters or \n                        value_str in self.known_guest_characters or\n                        self.is_valid_character_name(value_str)):\n                        \n                        # 隣接するセルからセリフを探す\n                        dialogue_text = \"\"\n                        for offset in [1, 2, -1, -2]:\n                            adj_col = col_idx + offset\n                            if 0 <= adj_col < len(row):\n                                adj_value = row.iloc[adj_col]\n                                if pd.notna(adj_value):\n                                    adj_text = str(adj_value).strip()\n                                    # セリフらしいテキストかチェック\n                                    if (len(adj_text) > 3 and \n                                        not adj_text in self.strict_exclusions and\n                                        not any(keyword in adj_text for keyword in self.instruction_keywords)):\n                                        dialogue_text = adj_text\n                                        break\n                        \n                        entries.append({\n                            'row_number': row_idx,\n                            'character_name': value_str,\n                            'dialogue_text': dialogue_text,\n                            'filming_audio_instructions': \"\"\n                        })\n        \n        if entries:\n            self.log_message(f\"🎯 {management_id}: 代替抽出で{len(entries)}件を発見\")\n        \n        return entries if entries else None\n    \n    def update_script_data(self, script_info):\n        \"\"\"スクリプトデータの更新\"\"\"\n        try:\n            # 新しいデータ抽出\n            new_data = self.extract_comprehensive_data(\n                script_info['script_url'], \n                script_info['management_id']\n            )\n            \n            if not new_data:\n                return False\n            \n            # データベース更新\n            conn = sqlite3.connect(self.db_path)\n            cursor = conn.cursor()\n            \n            # スクリプトID取得\n            cursor.execute(\"SELECT id FROM scripts WHERE management_id = ?\", (script_info['management_id'],))\n            script_row = cursor.fetchone()\n            if not script_row:\n                conn.close()\n                return False\n            \n            script_id = script_row[0]\n            \n            # 既存エントリ削除\n            cursor.execute(\"DELETE FROM character_dialogue_unified WHERE script_id = ?\", (script_id,))\n            deleted_count = cursor.rowcount\n            \n            # 新しいデータ挿入\n            inserted = 0\n            for entry in new_data:\n                try:\n                    cursor.execute(\"\"\"\n                        INSERT INTO character_dialogue_unified \n                        (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)\n                        VALUES (?, ?, ?, ?, ?)\n                    \"\"\", (\n                        script_id,\n                        entry['row_number'],\n                        entry['character_name'],\n                        entry['dialogue_text'],\n                        entry['filming_audio_instructions']\n                    ))\n                    inserted += 1\n                except Exception as e:\n                    self.log_message(f\"⚠️  {script_info['management_id']}: 挿入エラー - {str(e)}\")\n            \n            conn.commit()\n            conn.close()\n            \n            self.log_message(f\"✅ {script_info['management_id']}: {deleted_count}件削除、{inserted}件挿入\")\n            return True\n            \n        except Exception as e:\n            self.log_message(f\"❌ {script_info['management_id']}: 更新エラー - {str(e)}\")\n            return False\n    \n    def process_all_scripts(self, max_scripts=500):\n        \"\"\"全スクリプトの包括的処理\"\"\"\n        self.log_message(\"=\" * 80)\n        self.log_message(\"全スクリプト包括的抽出処理開始\")\n        self.log_message(\"=\" * 80)\n        \n        try:\n            conn = sqlite3.connect(self.db_path)\n            cursor = conn.cursor()\n            \n            # 処理対象スクリプト取得\n            cursor.execute(\"\"\"\n                SELECT DISTINCT s.management_id, s.title, s.script_url\n                FROM scripts s\n                WHERE s.script_url IS NOT NULL AND s.script_url != ''\n                ORDER BY s.management_id\n                LIMIT ?\n            \"\"\", (max_scripts,))\n            \n            scripts = []\n            for mgmt_id, title, script_url in cursor.fetchall():\n                scripts.append({\n                    'management_id': mgmt_id,\n                    'title': title,\n                    'script_url': script_url\n                })\n            \n            conn.close()\n            \n            if not scripts:\n                self.log_message(\"❌ 処理対象スクリプトが見つかりませんでした\")\n                return 0\n            \n            self.log_message(f\"🎯 処理対象: {len(scripts)}件\")\n            \n            success_count = 0\n            fail_count = 0\n            guest_found_count = 0\n            \n            for i, script in enumerate(scripts):\n                if i % 10 == 0:\n                    self.log_message(f\"🔧 処理中 ({i+1}/{len(scripts)}): {script['management_id']}\")\n                \n                # データ抽出テスト\n                data = self.extract_comprehensive_data(script['script_url'], script['management_id'])\n                if data is not None:\n                    # ゲストキャラクター発見カウント\n                    guest_chars = [e for e in data if e['character_name'] in self.known_guest_characters]\n                    if guest_chars:\n                        guest_found_count += 1\n                    \n                    if self.update_script_data(script):\n                        success_count += 1\n                    else:\n                        fail_count += 1\n                else:\n                    fail_count += 1\n                \n                # レート制限\n                if i % 10 == 9:\n                    time.sleep(2)\n            \n            self.log_message(\"=\" * 80)\n            self.log_message(f\"包括的抽出結果:\")\n            self.log_message(f\"  処理成功: {success_count}件\")\n            self.log_message(f\"  処理失敗: {fail_count}件\")\n            self.log_message(f\"  ゲストキャラ発見: {guest_found_count}件\")\n            self.log_message(\"=\" * 80)\n            \n            return success_count\n            \n        except Exception as e:\n            self.log_message(f\"❌ 全体処理エラー: {str(e)}\")\n            return 0\n\ndef main():\n    \"\"\"メイン実行\"\"\"\n    db_path = \"/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db\"\n    \n    extractor = ComprehensiveExtractor(db_path)\n    \n    print(\"=== 包括的キャラクター抽出ツール ===\")\n    \n    # 全スクリプト処理\n    success_count = extractor.process_all_scripts(max_scripts=100)  # テスト用に100件に制限\n    \n    if success_count > 0:\n        print(f\"\\n✅ {success_count}件のスクリプトが正常に処理されました！\")\n    else:\n        print(f\"\\n❌ 処理できるスクリプトがありませんでした\")\n    \n    print(f\"詳細は comprehensive_extraction.txt を確認してください。\")\n\nif __name__ == \"__main__\":\n    main()