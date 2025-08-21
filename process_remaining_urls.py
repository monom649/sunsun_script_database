#!/usr/bin/env python3
"""
正しいURLを持つ未処理スクリプトを処理
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

# URLが存在する未処理スクリプトのリスト
scripts_with_urls = [
    ("B1029", "https://docs.google.com/spreadsheets/d/120ypuflQ2s1fggX7BcDIUyEjCqAQf0s1yuR54skt5ws/edit?gid=1384097767#gid=1384097767"),
    ("B1042", "https://docs.google.com/spreadsheets/d/1Qd4eTnFgaSLNxAoowBuZoMnj00WdrZcn3Mc9UjDA_k4/edit?gid=1384097767#gid=1384097767"),
    ("B1054", "https://docs.google.com/spreadsheets/d/1vRpqaNz48H-ywZL3AcIbE1RZRd2ATFQk4gwPT5L-CVY/edit?gid=1384097767#gid=1384097767"),
    ("B1066", "https://docs.google.com/spreadsheets/d/1fOeVRjEWBjtVSxg-inHpEGCZqPkqtX1SIkU-0X9I53c/edit?gid=1384097767#gid=1384097767"),
    ("B1082", "https://docs.google.com/spreadsheets/d/1RsyCwxJi_iTueBYqFWguF27EgeTJiy1TN-QvmGF-85E/edit?gid=1384097767#gid=1384097767"),
    ("B1090", "https://docs.google.com/spreadsheets/d/16iqcYfTDCRMsEV9ppy9j26dCHVs90Midf2G87GS3R3E/edit?gid=1384097767#gid=1384097767"),
    ("B1163", "https://docs.google.com/spreadsheets/d/1Ib7qoY8ZVT_Sb-hxljkQqFIwJdcIcV0ZT7T8kIzV6Wc/edit?gid=1384097767#gid=1384097767"),
    ("B1499", "https://docs.google.com/spreadsheets/d/17J-9lKmXYzsPnyQS5O1dJDTjLwMnRQ8m8JqkBxzHk_o/edit?gid=1115519680#gid=1115519680"),
    ("B1582", "https://docs.google.com/spreadsheets/d/1khxQzcM-CxY3_HusiXXB4nOuGaplyw_Hvg036OYVe94/edit?gid=100444062#gid=100444062"),
    ("B1748", "https://docs.google.com/spreadsheets/d/1d3-JIGEPYFEJbUGl0gycAFb9l36xvJ1bL3lNKsNjnzw/edit?gid=479685579#gid=479685579"),
    ("B1908", "https://docs.google.com/spreadsheets/d/19xlXyh4IU5nJJ3TJ8g_n2mIZD72zVR8WWTp-HjWT33g/edit?gid=1145505507#gid=1145505507"),
    ("B2066", "https://docs.google.com/spreadsheets/d/1NYC66KMQFm06Q-WHW25_a-YuIq97PHVp7AQzjILS61A/edit?gid=1115519680#gid=1115519680"),
    ("B2133", "https://docs.google.com/spreadsheets/d/1BMJyOiX3CkvtapzHmn4zb64E3oS6Tlm1ZBZdvAOIlS0/edit?gid=1115519680#gid=1115519680"),
    ("B2140", "https://docs.google.com/spreadsheets/d/1QANytMDhqOlf4rWVVfqR-GZrX9Ek5oS4MfO52E_icKo/edit?gid=1115519680#gid=1115519680"),
    ("B2142", "https://docs.google.com/spreadsheets/d/1q7gpiEvfAL2fYIGtqddJPtwwdKuTAanurTbQr8wRXqg/edit?gid=975044043#gid=975044043"),
    ("B2147", "https://docs.google.com/spreadsheets/d/1d3l0tPmIOx5rPPJPyuX8DDmeEHI-E0M-LkCuyfKaV3w/edit?gid=1115519680#gid=1115519680"),
    ("B2158", "https://docs.google.com/spreadsheets/d/1I30AcYLf0BUhMPu9uxz7QwI3PXLcceYO44l6K24jjn0/edit?gid=1115519680#gid=1115519680"),
    ("B2189", "https://docs.google.com/spreadsheets/d/1FUr6t9lGo6P5-qmy6QWEyGEt5grE9Toqfr23TsaN0A0/edit?gid=1115519680#gid=1115519680"),
    ("B2193", "https://docs.google.com/spreadsheets/d/1eEDTr7O_2wJGraj7XkrjNiNGXiRA6IU9-mXsoRhHWyo/edit?gid=1115519680#gid=1115519680"),
    ("B2216", "https://docs.google.com/spreadsheets/d/1f3zD3-6S4DhstBXmJACbxhglDTy9kQ46cOV2Lxe83bE/edit?gid=1115519680#gid=1115519680"),
    ("B2230", "https://docs.google.com/spreadsheets/d/1v5VnJOb2UdiX50tad4x5hSOUid6DWTgcBRcA1YX2A6k/edit?gid=1115519680#gid=1115519680"),
    ("B2253", "https://docs.google.com/spreadsheets/d/1Ty-rHvHwvmdet3_s3_9N6zZGN9o6rPqbUY2z9_jIXQE/edit?gid=2036772822#gid=2036772822"),
    ("B687", "https://docs.google.com/spreadsheets/d/1LGcCPd9R5j8E4nRQnY5XdOujHbg5b_rlvWzTYU9hyPQ/edit?gid=1384097767#gid=1384097767"),
    ("B712", "https://docs.google.com/spreadsheets/d/1-3tNzDY52dul7MxRjETQCXl_hkixy1ZI7dmYGavPUgg/edit?gid=1384097767#gid=1384097767"),
    ("B714", "https://docs.google.com/spreadsheets/d/1SRS15PtD_pOsZJ5qg-9X4WmJdfRmNt9JI4nRTP3arys/edit?gid=1384097767#gid=1384097767"),
    ("B718", "https://docs.google.com/spreadsheets/d/1QH6SdAZS36bJQhv5v14F_nBME5D2x6Nl8iNn-IlBzf4/edit?gid=1384097767#gid=1384097767"),
    ("B733", "https://docs.google.com/spreadsheets/d/1fr_MDBznNp2vGxaLNMZuN0r3JnuQ9po9LJoLmPS6ysU/edit?gid=1384097767#gid=1384097767"),
    ("B747", "https://docs.google.com/spreadsheets/d/1RrHsBg6GG4J4PwbRreJJHQe4IfOmyQW1XH31mbQgDUY/edit?gid=1973211497#gid=1973211497"),
    ("B749", "https://docs.google.com/spreadsheets/d/18U3kil4e8JFY-JdngKiaVCyuk0aKdejqQBcaCtiXKsA/edit?gid=1384097767#gid=1384097767"),
    ("B757", "https://docs.google.com/spreadsheets/d/1Ty9gZ1FDJwszi6fnmtBcbC1cxP8UerS4iPpOrZLmB7A/edit?gid=1384097767#gid=1384097767"),
    ("B769", "https://docs.google.com/spreadsheets/d/1w-MhUKxdHksWrCF-ZthEEGjHeNPSl4oh-rGUez2VEfc/edit?gid=345919412#gid=345919412"),
    ("B793", "https://docs.google.com/spreadsheets/d/1FXHaYeFFwLe1IEFbpuKGTdS41qvkDRGFa_bK97o3HHI/edit?gid=1384097767#gid=1384097767"),
    ("B799", "https://docs.google.com/spreadsheets/d/1WlMFCYNlbYY5DgbO86Kqe22JfVr1zBOBmWVHvnxDuNg/edit?gid=1384097767#gid=1384097767"),
    ("B804", "https://docs.google.com/spreadsheets/d/1JE_ui2rZycAazo928jOFQOjzmnXmvaGP-2v5ji1hJig/edit?gid=1384097767#gid=1384097767"),
    ("B854", "https://docs.google.com/spreadsheets/d/1_5mg4IgjMvX6H9SJoTKYvQj1Jxw4wEuwN6cmdO_WS0Q/edit?gid=1384097767#gid=1384097767"),
    ("B882", "https://docs.google.com/spreadsheets/d/1FrsP-WH69b36j6Mx9eO4mECGGXxrOF0VAfqIWbSy0ss/edit?gid=1384097767#gid=1384097767"),
    ("B899", "https://docs.google.com/spreadsheets/d/10k9G2YItIbx1oqpa-_oRFrYMGJalWyf__yu2yn2z6kk/edit?gid=1384097767#gid=1384097767"),
    ("B942", "https://docs.google.com/spreadsheets/d/16g4GsW3bXWNOsHufeV_8xYlTxLUoPSOJCE-CcvYUPgY/edit?gid=1384097767#gid=1384097767"),
    ("B954", "https://docs.google.com/spreadsheets/d/1eJ8iqGH4cHQQsWp2ljISiLWAH_dZsS-IvsKSxZg6szs/edit?gid=1384097767#gid=1384097767"),
    ("B958", "https://docs.google.com/spreadsheets/d/1llGKQJ1trwYoeHCLs6Wcfe3umBsHFpqqu0qar1wWCtQ/edit?gid=1384097767#gid=1384097767"),
    ("B959", "https://docs.google.com/spreadsheets/d/1WoKPfc1PDmowvjiT3utzAIe-y1MjxZPiRHppFs8L1UE/edit?gid=1384097767#gid=1384097767"),
    ("Ｂ1367", "https://docs.google.com/spreadsheets/d/1ooHVgHjPGklS9EePuc7u_wGdyQQHWX28jv8XO4KF3jw/edit?gid=1384097767#gid=1384097767")
]

class RemainingUrlProcessor:
    def __init__(self, db_path):
        self.db_path = db_path
    
    def log_message(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def find_header_row_and_columns(self, df):
        """ヘッダー行とキャラクター・セリフ列を特定"""
        for row_idx in range(min(25, len(df))):
            row = df.iloc[row_idx]
            
            character_col = None
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    if 'キャラクター' in value_str or 'キャラ' == value_str:
                        character_col = col_idx
                        break
            
            if character_col is not None:
                dialogue_col = None
                for col_idx in range(character_col + 1, len(row)):
                    value = row.iloc[col_idx] if col_idx < len(row) else None
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        if 'セリフ' in value_str or 'せりふ' in value_str:
                            dialogue_col = col_idx
                            break
                
                if dialogue_col is not None:
                    return row_idx, character_col, dialogue_col
        
        return None, None, None
    
    def extract_gid_from_url(self, url):
        """URLからgidを抽出"""
        match = re.search(r'gid=(\d+)', url)
        return match.group(1) if match else None
    
    def get_csv_url_from_gid(self, spreadsheet_id, gid):
        """spreadsheet_idとgidからCSV URLを生成"""
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    
    def get_script_id_from_management_id(self, management_id):
        """管理IDからscript_idを取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (management_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def update_script_url(self, script_id, new_url):
        """スクリプトのURLを更新"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE scripts SET script_url = ? WHERE id = ?", (new_url, script_id))
        conn.commit()
        conn.close()
    
    def process_script(self, management_id, script_url):
        """単一スクリプトを処理"""
        try:
            script_id = self.get_script_id_from_management_id(management_id)
            if not script_id:
                self.log_message(f"❌ {management_id}: スクリプトIDが見つからない")
                return False
            
            # spreadsheet_id抽出
            spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', script_url)
            if not spreadsheet_match:
                self.log_message(f"❌ {management_id}: URL解析失敗")
                return False
            
            spreadsheet_id = spreadsheet_match.group(1)
            gid = self.extract_gid_from_url(script_url)
            
            if not gid:
                self.log_message(f"❌ {management_id}: gid抽出失敗")
                return False
            
            csv_url = self.get_csv_url_from_gid(spreadsheet_id, gid)
            
            # CSV取得
            response = requests.get(csv_url, timeout=30)
            if response.status_code != 200:
                self.log_message(f"❌ {management_id}: HTTP {response.status_code}")
                return False
            
            # DataFrame作成
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data, header=None)
            
            if df.empty:
                self.log_message(f"❌ {management_id}: 空データ")
                return False
            
            # ヘッダー検出
            header_row, character_col, dialogue_col = self.find_header_row_and_columns(df)
            
            if header_row is None:
                self.log_message(f"❌ {management_id}: ヘッダー検出失敗")
                return False
            
            # データ抽出
            dialogue_data = []
            for idx in range(header_row + 1, len(df)):
                row = df.iloc[idx]
                
                if character_col < len(row) and dialogue_col < len(row):
                    character = row.iloc[character_col]
                    dialogue = row.iloc[dialogue_col]
                    
                    if pd.notna(character) and pd.notna(dialogue):
                        character_str = str(character).strip()
                        dialogue_str = str(dialogue).strip()
                        
                        if character_str and dialogue_str:
                            dialogue_data.append({
                                'character': character_str,
                                'dialogue': dialogue_str
                            })
            
            if not dialogue_data:
                self.log_message(f"⚠️ {management_id}: データ抽出結果なし")
                return False
            
            # データベース挿入
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for item in dialogue_data:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, character_name, dialogue) 
                    VALUES (?, ?, ?)
                """, (script_id, item['character'], item['dialogue']))
            
            conn.commit()
            conn.close()
            
            # スクリプトURLを更新
            self.update_script_url(script_id, script_url)
            
            self.log_message(f"✅ {management_id}: ヘッダー行{header_row}, キャラ列{character_col}, セリフ列{dialogue_col}, {len(dialogue_data)}件抽出")
            
            return True
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: エラー - {str(e)}")
            return False
    
    def run(self):
        """URLが存在する未処理スクリプトを処理"""
        self.log_message("================================================================================")
        self.log_message("残りURL処理開始")
        self.log_message("================================================================================")
        
        total_scripts = len(scripts_with_urls)
        self.log_message(f"🎯 処理対象: {total_scripts}件")
        
        success_count = 0
        for i, (management_id, script_url) in enumerate(scripts_with_urls, 1):
            self.log_message(f"🔧 {management_id} 処理開始")
            
            if self.process_script(management_id, script_url):
                success_count += 1
            
            if i % 10 == 0:
                self.log_message(f"🔧 進捗: {i}/{total_scripts}")
            
            time.sleep(1)  # レート制限対策
        
        self.log_message("================================================================================")
        self.log_message(f"残りURL処理完了: {success_count}/{total_scripts}件成功")
        self.log_message("================================================================================")

if __name__ == "__main__":
    processor = RemainingUrlProcessor("/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db")
    processor.run()