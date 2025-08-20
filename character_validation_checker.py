#!/usr/bin/env python3
"""
キャラクター名妥当性チェッカー

データベース内の不適切なキャラクター名を特定し、
セリフとして誤抽出されたものを検出する
"""

import sqlite3
import re
from datetime import datetime

class CharacterValidationChecker:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/character_validation.txt"
        
        # セリフの可能性が高いパターン
        self.dialogue_patterns = [
            r'.*じゃん$',      # 「〜じゃん」
            r'.*だよね$',      # 「〜だよね」
            r'.*でしょ$',      # 「〜でしょ」
            r'.*かな$',       # 「〜かな」
            r'.*よ$',         # 「〜よ」
            r'.*ね$',         # 「〜ね」
            r'.*もん$',       # 「〜もん」
            r'.*だもん$',     # 「〜だもん」
            r'.*だって$',     # 「〜だって」
            r'.*なのに$',     # 「〜なのに」
            r'.*けど$',       # 「〜けど」
            r'.*のよ$',       # 「〜のよ」
            r'.*わよ$',       # 「〜わよ」
            r'.*でも$',       # 「〜でも」
            r'.*から$',       # 「〜から」
            r'.*ため$',       # 「〜ため」
        ]
        
        # 明らかに感嘆詞・間投詞
        self.interjections = {
            'あー', 'えー', 'おー', 'うー', 'わー', 'きゃー', 'ひー',
            'あぁ', 'えぇ', 'おぉ', 'うぅ', 'わぁ', 'はぁ', 'ふぅ',
            'ああ', 'ええ', 'おお', 'うう', 'わあ', 'はは', 'ふふ',
            'へー', 'ほー', 'ひゃー', 'ぎゃー', 'うひー', 'げー',
            'まあ', 'そう', 'はい', 'いえ', 'うん', 'ううん'
        }
        
        # 日本語の一般的な単語（キャラクター名ではない）
        self.common_words = {
            'とき', 'とも', 'みな', 'みんな', 'だれ', 'なに', 'どこ',
            'いつ', 'なぜ', 'どう', 'こう', 'そう', 'ああ', 'もう',
            'ずっと', 'また', 'すぐ', 'もっと', 'ちょっと', 'きっと',
            'だけ', 'しか', 'まで', 'から', 'より', 'ほど', 'くらい'
        }
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_likely_dialogue(self, character_name):
        """セリフの可能性が高いかチェック"""
        # セリフパターンマッチ
        for pattern in self.dialogue_patterns:
            if re.match(pattern, character_name, re.IGNORECASE):
                return True, f"セリフパターン: {pattern}"
        
        # 感嘆詞・間投詞
        if character_name.lower() in self.interjections:
            return True, "感嘆詞・間投詞"
        
        # 一般的な単語
        if character_name.lower() in self.common_words:
            return True, "一般的な単語"
        
        # 質問文
        if character_name.endswith('？') or character_name.endswith('?'):
            return True, "質問文"
        
        # 長すぎる（明らかにセリフ）
        if len(character_name) > 30:
            return True, "長すぎる（セリフの可能性）"
        
        # 複数の句読点
        if character_name.count('、') > 1 or character_name.count('。') > 0:
            return True, "複数の句読点"
        
        return False, ""
    
    def analyze_suspicious_characters(self):
        """疑わしいキャラクター名を分析"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT character_name, COUNT(*) as count, COUNT(DISTINCT script_id) as script_count
                FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0
                GROUP BY character_name
                ORDER BY count DESC
            """)
            
            all_characters = cursor.fetchall()
            
            suspicious_characters = []
            valid_characters = []
            
            for char_name, count, script_count in all_characters:
                is_suspicious, reason = self.is_likely_dialogue(char_name)
                
                if is_suspicious:
                    suspicious_characters.append({
                        'name': char_name,
                        'count': count,
                        'scripts': script_count,
                        'reason': reason
                    })
                else:
                    valid_characters.append({
                        'name': char_name,
                        'count': count,
                        'scripts': script_count
                    })
            
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("キャラクター名妥当性分析結果")
            self.log_message("=" * 80)
            
            self.log_message(f"📊 分析統計:")
            self.log_message(f"  全キャラクター: {len(all_characters)}種類")
            self.log_message(f"  妥当なキャラクター: {len(valid_characters)}種類")
            self.log_message(f"  疑わしいキャラクター: {len(suspicious_characters)}種類")
            
            if suspicious_characters:
                self.log_message(f"\n⚠️  疑わしいキャラクター名（セリフの誤抽出の可能性）:")
                for char in suspicious_characters[:20]:  # 上位20個
                    self.log_message(f"  '{char['name']}': {char['count']}回 ({char['scripts']}スクリプト) - {char['reason']}")
                
                if len(suspicious_characters) > 20:
                    self.log_message(f"  ... 他{len(suspicious_characters) - 20}個")
            
            # 特に「じゃん」をチェック
            jan_chars = [c for c in suspicious_characters if 'じゃん' in c['name']]
            if jan_chars:
                self.log_message(f"\n🎯 「じゃん」関連の疑わしいエントリ:")
                for char in jan_chars:
                    self.log_message(f"  '{char['name']}': {char['count']}回 - {char['reason']}")
                    
                    # 具体例を表示
                    self.show_character_examples(char['name'])
            
            return suspicious_characters, valid_characters
            
        except Exception as e:
            self.log_message(f"❌ 分析エラー: {str(e)}")
            return [], []
    
    def show_character_examples(self, character_name, limit=5):
        """特定キャラクターの具体例表示"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT s.management_id, cdu.row_number, cdu.dialogue_text
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE cdu.character_name = ?
                ORDER BY s.management_id, cdu.row_number
                LIMIT ?
            """, (character_name, limit))
            
            examples = cursor.fetchall()
            conn.close()
            
            if examples:
                self.log_message(f"    具体例:")
                for mgmt_id, row_num, dialogue in examples:
                    dialogue_preview = dialogue[:50] + "..." if len(dialogue) > 50 else dialogue
                    self.log_message(f"      {mgmt_id}行{row_num}: セリフ=\"{dialogue_preview}\"")
            
        except Exception as e:
            self.log_message(f"    例の取得エラー: {str(e)}")
    
    def suggest_corrections(self, suspicious_characters):
        """修正提案"""
        self.log_message(f"\n💡 修正提案:")
        
        if suspicious_characters:
            self.log_message(f"  1. 以下のキャラクター名はセリフの誤抽出の可能性があります")
            self.log_message(f"  2. 抽出ロジックの改善により、これらを除外する必要があります")
            self.log_message(f"  3. 特に「〜じゃん」「〜だよね」などの語尾はセリフの特徴です")
            
            # カテゴリ別統計
            pattern_counts = {}
            for char in suspicious_characters:
                reason = char['reason']
                if reason not in pattern_counts:
                    pattern_counts[reason] = 0
                pattern_counts[reason] += 1
            
            self.log_message(f"\n📊 疑わしいパターン別統計:")
            for pattern, count in sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True):
                self.log_message(f"  {pattern}: {count}個")
        
        else:
            self.log_message(f"  ✅ 現在のキャラクター名は適切です")
    
    def run_validation(self):
        """妥当性チェック実行"""
        self.log_message("キャラクター名妥当性チェック開始")
        
        suspicious, valid = self.analyze_suspicious_characters()
        self.suggest_corrections(suspicious)
        
        return suspicious, valid

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    checker = CharacterValidationChecker(db_path)
    
    print("=== キャラクター名妥当性チェッカー ===")
    
    # 妥当性チェック実行
    suspicious, valid = checker.run_validation()
    
    if suspicious:
        print(f"\n⚠️  {len(suspicious)}個の疑わしいキャラクター名を発見")
    else:
        print(f"\n✅ 全キャラクター名が適切です")
    
    print(f"詳細は character_validation.txt を確認してください。")

if __name__ == "__main__":
    main()