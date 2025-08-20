#!/usr/bin/env python3
"""
Comprehensive Validation Tool

This script performs comprehensive validation of dialogue structure fixes
across all keywords and character types, not limited to specific words.
"""

import sqlite3
import random
from datetime import datetime

class ComprehensiveValidator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/comprehensive_validation_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def get_database_overview(self):
        """Get comprehensive database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total entries
            cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified')
            total_entries = cursor.fetchone()[0]
            
            # Character entries with dialogue
            cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0')
            valid_dialogues = cursor.fetchone()[0]
            
            # Empty dialogue entries
            cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified WHERE LENGTH(character_name) > 0 AND (dialogue_text IS NULL OR dialogue_text = "")')
            empty_dialogues = cursor.fetchone()[0]
            
            # Instruction-like content in dialogue
            cursor.execute('''
                SELECT COUNT(*) FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0 
                AND (dialogue_text LIKE '%・%' OR dialogue_text LIKE '%（%' OR dialogue_text LIKE '%）%'
                     OR dialogue_text LIKE '%カット%' OR dialogue_text LIKE '%映像%' 
                     OR dialogue_text LIKE '%テロップ%' OR dialogue_text LIKE '%アニメ%'
                     OR dialogue_text LIKE '%撮影%' OR dialogue_text LIKE '%音声%'
                     OR dialogue_text LIKE '%選手%' OR dialogue_text LIKE '%登場%'
                     OR dialogue_text LIKE '%シーン%' OR dialogue_text LIKE '%→%')
            ''')
            instruction_in_dialogue = cursor.fetchone()[0]
            
            # Character distribution
            cursor.execute('''
                SELECT character_name, COUNT(*) as count
                FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0
                GROUP BY character_name
                ORDER BY count DESC
                LIMIT 10
            ''')
            character_distribution = cursor.fetchall()
            
            conn.close()
            
            return {
                'total_entries': total_entries,
                'valid_dialogues': valid_dialogues,
                'empty_dialogues': empty_dialogues,
                'instruction_in_dialogue': instruction_in_dialogue,
                'character_distribution': character_distribution
            }
            
        except Exception as e:
            self.log_message(f"❌ Database overview error: {str(e)}")
            return {}
    
    def validate_dialogue_quality(self, sample_size=200):
        """Validate dialogue quality by sampling random entries"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get random sample of dialogues
            cursor.execute('''
                SELECT cdu.character_name, cdu.dialogue_text, s.management_id
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0
                ORDER BY RANDOM()
                LIMIT ?
            ''', (sample_size,))
            
            samples = cursor.fetchall()
            
            # Analyze samples
            analysis = {
                'total_samples': len(samples),
                'valid_dialogue': 0,
                'instruction_like': 0,
                'questionable': 0,
                'examples': {
                    'valid': [],
                    'problematic': []
                }
            }
            
            for char_name, dialogue, script_id in samples:
                # Check if dialogue looks like instruction
                instruction_keywords = ['・', '（', '）', '→', 'カット', '映像', 'テロップ', 'アニメ', '撮影', '音声', '編集', 'SE', '選手', '登場', 'シーン', '全種類', '紹介']
                is_instruction_like = any(keyword in dialogue for keyword in instruction_keywords)
                
                if is_instruction_like:
                    analysis['instruction_like'] += 1
                    if len(analysis['examples']['problematic']) < 10:
                        analysis['examples']['problematic'].append({
                            'character': char_name,
                            'dialogue': dialogue[:150],
                            'script': script_id
                        })
                else:
                    # Check if it's proper dialogue
                    dialogue_indicators = ['！', '？', '。', 'だよ', 'です', 'だね', 'ちゃん', 'くん', 'みんな', 'よ～', 'ね～']
                    is_dialogue_like = any(indicator in dialogue for indicator in dialogue_indicators)
                    
                    if is_dialogue_like and len(dialogue) >= 3:
                        analysis['valid_dialogue'] += 1
                        if len(analysis['examples']['valid']) < 10:
                            analysis['examples']['valid'].append({
                                'character': char_name,
                                'dialogue': dialogue[:150],
                                'script': script_id
                            })
                    else:
                        analysis['questionable'] += 1
            
            conn.close()
            return analysis
            
        except Exception as e:
            self.log_message(f"❌ Quality validation error: {str(e)}")
            return {}
    
    def test_search_functionality(self):
        """Test search functionality across various keywords"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Test keywords covering different categories
            test_keywords = [
                # Character names
                'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ',
                # Common words
                'ありがとう', 'すごい', 'だめ', 'よし', 'がんばろう',
                # Actions
                '遊ぶ', '食べる', '見る', '行く', '作る',
                # Objects
                '車', 'おもちゃ', '公園', '家', '学校'
            ]
            
            search_results = {}
            
            for keyword in test_keywords:
                cursor.execute('''
                    SELECT COUNT(*) FROM character_dialogue_unified 
                    WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0 
                    AND dialogue_text LIKE ?
                    AND dialogue_text NOT LIKE '%・%' 
                    AND dialogue_text NOT LIKE '%（%'
                    AND dialogue_text NOT LIKE '%→%'
                    AND dialogue_text NOT LIKE '%カット%'
                    AND dialogue_text NOT LIKE '%映像%'
                ''', (f'%{keyword}%',))
                
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # Get sample results
                    cursor.execute('''
                        SELECT character_name, dialogue_text
                        FROM character_dialogue_unified cdu
                        WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0 
                        AND cdu.dialogue_text LIKE ?
                        AND dialogue_text NOT LIKE '%・%' 
                        AND dialogue_text NOT LIKE '%（%'
                        AND dialogue_text NOT LIKE '%→%'
                        ORDER BY RANDOM()
                        LIMIT 3
                    ''', (f'%{keyword}%',))
                    
                    samples = cursor.fetchall()
                    search_results[keyword] = {
                        'count': count,
                        'samples': samples
                    }
            
            conn.close()
            return search_results
            
        except Exception as e:
            self.log_message(f"❌ Search test error: {str(e)}")
            return {}
    
    def run_comprehensive_validation(self):
        """Run complete validation process"""
        self.log_message("=" * 80)
        self.log_message("包括的セリフ構造修正検証開始")
        self.log_message("=" * 80)
        
        # 1. Database overview
        overview = self.get_database_overview()
        if overview:
            self.log_message("📊 データベース全体統計:")
            self.log_message(f"  総エントリ数: {overview['total_entries']:,}件")
            self.log_message(f"  有効なセリフ: {overview['valid_dialogues']:,}件")
            self.log_message(f"  空のセリフ: {overview['empty_dialogues']:,}件")
            self.log_message(f"  セリフ欄の指示文: {overview['instruction_in_dialogue']:,}件")
            
            success_rate = (overview['valid_dialogues'] / overview['total_entries']) * 100 if overview['total_entries'] > 0 else 0
            instruction_rate = (overview['instruction_in_dialogue'] / overview['valid_dialogues']) * 100 if overview['valid_dialogues'] > 0 else 0
            
            self.log_message(f"  修正成功率: {success_rate:.1f}%")
            self.log_message(f"  指示文混入率: {instruction_rate:.1f}%")
            
            self.log_message("\\n👥 キャラクター別セリフ数:")
            for char_name, count in overview['character_distribution']:
                self.log_message(f"  {char_name}: {count:,}件")
        
        # 2. Quality validation
        quality_analysis = self.validate_dialogue_quality(sample_size=500)
        if quality_analysis:
            self.log_message("\\n🔍 セリフ品質分析 (500件サンプル):")
            self.log_message(f"  正常なセリフ: {quality_analysis['valid_dialogue']}件")
            self.log_message(f"  指示文のようなセリフ: {quality_analysis['instruction_like']}件")
            self.log_message(f"  疑問のあるセリフ: {quality_analysis['questionable']}件")
            
            quality_rate = (quality_analysis['valid_dialogue'] / quality_analysis['total_samples']) * 100
            instruction_contamination = (quality_analysis['instruction_like'] / quality_analysis['total_samples']) * 100
            self.log_message(f"  品質率: {quality_rate:.1f}%")
            self.log_message(f"  指示文汚染率: {instruction_contamination:.1f}%")
            
            if quality_analysis['examples']['valid']:
                self.log_message("\\n✅ 正常なセリフ例:")
                for example in quality_analysis['examples']['valid'][:5]:
                    self.log_message(f"  {example['character']}: {example['dialogue']}")
            
            if quality_analysis['examples']['problematic']:
                self.log_message("\\n❌ 指示文が混入している例:")
                for example in quality_analysis['examples']['problematic'][:10]:
                    self.log_message(f"  {example['character']}: {example['dialogue']}")
        
        # 3. Search functionality test
        search_results = self.test_search_functionality()
        if search_results:
            self.log_message("\\n🔎 検索機能テスト結果 (指示文除外):")
            
            # Group by result count
            high_results = {k: v for k, v in search_results.items() if v['count'] > 100}
            medium_results = {k: v for k, v in search_results.items() if 10 <= v['count'] <= 100}
            low_results = {k: v for k, v in search_results.items() if 1 <= v['count'] < 10}
            
            self.log_message(f"  高頻度キーワード({len(high_results)}個): {', '.join(high_results.keys())}")
            self.log_message(f"  中頻度キーワード({len(medium_results)}個): {', '.join(medium_results.keys())}")
            self.log_message(f"  低頻度キーワード({len(low_results)}個): {', '.join(low_results.keys())}")
            
            # Show examples for high-frequency keywords
            for keyword, data in list(high_results.items())[:3]:
                self.log_message(f"\\n  '{keyword}' 純粋セリフ検索結果例 ({data['count']}件):")
                for char, dialogue in data['samples']:
                    self.log_message(f"    {char}: {dialogue}")
        
        # 4. Overall assessment
        self.log_message("\\n" + "=" * 80)
        self.log_message("📋 総合評価:")
        
        if overview and quality_analysis:
            # Calculate actual dialogue purity
            instruction_contamination = (quality_analysis['instruction_like'] / quality_analysis['total_samples']) * 100
            dialogue_purity = 100 - instruction_contamination
            
            if dialogue_purity >= 90:
                self.log_message("🎉 優秀 - セリフと指示文の分離が非常に成功しています")
            elif dialogue_purity >= 75:
                self.log_message("✅ 良好 - セリフと指示文の分離が概ね成功しています")
            elif dialogue_purity >= 50:
                self.log_message("⚠️  改善必要 - セリフと指示文の分離に重大な問題があります")
            else:
                self.log_message("❌ 失敗 - セリフと指示文の分離ができていません")
            
            self.log_message(f"  セリフ純度: {dialogue_purity:.1f}%")
            self.log_message(f"  指示文汚染: {instruction_contamination:.1f}%")
        
        search_success = len(search_results)
        self.log_message(f"  純粋セリフ検索: {search_success}/20 キーワードで結果あり")
        
        self.log_message("=" * 80)

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    validator = ComprehensiveValidator(db_path)
    validator.run_comprehensive_validation()

if __name__ == "__main__":
    main()
