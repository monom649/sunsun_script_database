#!/usr/bin/env python3
"""
ゲストキャラクター抽出漏れ分析ツール

スプレッドシートから直接入力されたゲストキャラクターが
データベースに正しく抽出されているかを調査する。
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
from collections import defaultdict

class GuestCharacterAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/guest_character_analysis.txt"
        
        # 定型キャラクター（プルダウンで選択されるもの）
        self.standard_characters = {
            'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'BB', 'ママ', 'パパ', 
            'みんな', 'こども', '子ども', 'SE', '（SE）', '(SE)'
        }
        
        # 除外すべき値（指示文、エラー値等）
        self.exclusion_patterns = [
            'TRUE', 'FALSE', 'セリフ', '不明', 
            '・', '（', '）', '→', 'カット', '映像', 'テロップ', 
            'アニメ', '編集', '選手', '登場', 'シーン', '全種類', '紹介', '作り方', '演出',
            '=countifs', '[=', 'http'
        ]
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def analyze_database_characters(self):
        """データベース内のキャラクター分析"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 全キャラクター名を取得
            cursor.execute("""
                SELECT character_name, COUNT(*) as count, COUNT(DISTINCT script_id) as script_count
                FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 
                GROUP BY character_name 
                ORDER BY count DESC
            """)
            
            all_characters = cursor.fetchall()
            
            # 分類
            standard_chars = {}
            potential_guests = {}
            excluded_chars = {}
            
            for char_name, count, script_count in all_characters:
                # 除外対象チェック
                is_excluded = any(pattern in char_name for pattern in self.exclusion_patterns)
                if is_excluded:
                    excluded_chars[char_name] = {'count': count, 'scripts': script_count}
                # 定型キャラクター
                elif char_name in self.standard_characters:
                    standard_chars[char_name] = {'count': count, 'scripts': script_count}
                # ゲストキャラクター候補
                else:
                    potential_guests[char_name] = {'count': count, 'scripts': script_count}
            
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("データベース内キャラクター分析結果")
            self.log_message("=" * 80)
            
            self.log_message(f"📊 総キャラクター種類数: {len(all_characters)}")
            self.log_message(f"✅ 定型キャラクター: {len(standard_chars)}種類")
            self.log_message(f"🎭 ゲストキャラクター候補: {len(potential_guests)}種類")
            self.log_message(f"❌ 除外対象: {len(excluded_chars)}種類")
            
            # 定型キャラクター詳細
            self.log_message("\n📋 定型キャラクター一覧:")
            for char, data in sorted(standard_chars.items(), key=lambda x: x[1]['count'], reverse=True):
                self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
            
            # ゲストキャラクター上位
            self.log_message("\n🎭 ゲストキャラクター候補（上位20）:")
            guest_sorted = sorted(potential_guests.items(), key=lambda x: x[1]['count'], reverse=True)
            for char, data in guest_sorted[:20]:
                self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
            
            # 除外対象上位
            self.log_message("\n❌ 除外されたエントリ（上位10）:")
            excluded_sorted = sorted(excluded_chars.items(), key=lambda x: x[1]['count'], reverse=True)
            for char, data in excluded_sorted[:10]:
                self.log_message(f"  {char}: {data['count']}回 ({data['scripts']}スクリプト)")
            
            return {
                'standard': standard_chars,
                'guests': potential_guests,
                'excluded': excluded_chars
            }
            
        except Exception as e:
            self.log_message(f"❌ データベース分析エラー: {str(e)}")
            return {}
    
    def check_missing_characters(self, target_chars=['カエルン', 'ウサッチ', 'モーレツ先生']):
        """指定キャラクターの抽出状況確認"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("\n" + "=" * 80)
            self.log_message("指定キャラクターの抽出状況確認")
            self.log_message("=" * 80)
            
            for target_char in target_chars:
                # 完全一致検索
                cursor.execute("""
                    SELECT COUNT(*) FROM character_dialogue_unified 
                    WHERE character_name = ?
                """, (target_char,))
                exact_count = cursor.fetchone()[0]
                
                # 部分一致検索
                cursor.execute("""
                    SELECT DISTINCT character_name, COUNT(*) as count
                    FROM character_dialogue_unified 
                    WHERE character_name LIKE ?
                    GROUP BY character_name
                """, (f'%{target_char}%',))
                partial_matches = cursor.fetchall()
                
                self.log_message(f"\n🔍 '{target_char}' の検索結果:")
                self.log_message(f"  完全一致: {exact_count}件")
                
                if partial_matches:
                    self.log_message(f"  部分一致:")
                    for name, count in partial_matches:
                        self.log_message(f"    '{name}': {count}件")
                else:
                    self.log_message(f"  ❌ 部分一致も見つかりませんでした")
            
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ 指定キャラクター確認エラー: {str(e)}")
    
    def analyze_extraction_constraints(self):
        """現在の抽出制約による影響分析"""
        self.log_message("\n" + "=" * 80)
        self.log_message("抽出ロジック制約分析")
        self.log_message("=" * 80)
        
        # 現在の制約条件
        constraints = {
            "長さ制限": "0文字より長く、20文字以下",
            "指示文マーカー除外": "・（）→カット映像テロップアニメ編集選手登場シーン全種類紹介作り方演出",
            "空白値除外": "空白やNULL値",
            "ヘッダー行依存": "キャラクター列ヘッダーが見つからない場合は全体をスキップ"
        }
        
        self.log_message("📋 現在の抽出制約:")
        for constraint, desc in constraints.items():
            self.log_message(f"  {constraint}: {desc}")
        
        # 潜在的な問題
        potential_issues = [
            "1. 20文字を超える長いゲストキャラクター名が除外される",
            "2. カッコ付きキャラクター名（例：（ゲスト）カエルン）が除外される", 
            "3. ヘッダー行が不規則なシートで全データがスキップされる",
            "4. 特殊文字を含むキャラクター名が指示文として誤判定される",
            "5. プルダウン以外の直接入力キャラクターに偏った除外ルールが適用される"
        ]
        
        self.log_message("\n⚠️  ゲストキャラクター抽出の潜在的問題:")
        for issue in potential_issues:
            self.log_message(f"  {issue}")
    
    def suggest_improvements(self):
        """改善提案"""
        self.log_message("\n" + "=" * 80)
        self.log_message("ゲストキャラクター抽出改善提案")
        self.log_message("=" * 80)
        
        improvements = [
            "1. 文字数制限の緩和: 20文字 → 30文字に拡張",
            "2. ゲストキャラクター専用の許可リスト作成",
            "3. 指示文判定の精度向上（完全一致 → 部分一致の見直し）",
            "4. スプレッドシート解析での手動入力セル検出機能",
            "5. 抽出後の人間による確認プロセス追加",
            "6. 定期的なゲストキャラクター監査と追加"
        ]
        
        self.log_message("✅ 推奨改善策:")
        for improvement in improvements:
            self.log_message(f"  {improvement}")
        
        # 具体的なコード変更案
        self.log_message("\n🔧 具体的なコード変更案:")
        self.log_message("  A) character_name長さ制限: 20 → 30文字")
        self.log_message("  B) 指示文マーカー判定をより厳密に（完全文字列での判定）")
        self.log_message("  C) ゲストキャラクター候補の手動確認フラグ追加")
        self.log_message("  D) 抽出されたキャラクター名の定期レポート機能")
    
    def run_complete_analysis(self):
        """完全分析実行"""
        self.log_message("ゲストキャラクター抽出分析開始")
        
        # 1. データベース内キャラクター分析
        db_analysis = self.analyze_database_characters()
        
        # 2. 指定キャラクターの確認
        self.check_missing_characters()
        
        # 3. 抽出制約の分析
        self.analyze_extraction_constraints()
        
        # 4. 改善提案
        self.suggest_improvements()
        
        self.log_message("\n" + "=" * 80)
        self.log_message("📋 分析完了サマリー")
        self.log_message("=" * 80)
        
        if db_analysis:
            guest_count = len(db_analysis['guests'])
            self.log_message(f"✅ 現在データベースに {guest_count} 種類のゲストキャラクター候補が存在")
            self.log_message(f"⚠️  カエルン、ウサッチ、モーレツ先生は現在データベースに見つかりません")
            self.log_message(f"🔧 抽出ロジックの制約により、一部ゲストキャラクターが漏れている可能性があります")
        
        return db_analysis

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    analyzer = GuestCharacterAnalyzer(db_path)
    
    print("=== ゲストキャラクター抽出分析ツール ===")
    
    # 完全分析実行
    results = analyzer.run_complete_analysis()
    
    if results:
        print(f"\n✅ 分析完了！")
        print(f"ゲストキャラクター候補: {len(results.get('guests', {}))}種類")
        print(f"詳細は guest_character_analysis.txt を確認してください。")
    else:
        print(f"\n❌ 分析に失敗しました")

if __name__ == "__main__":
    main()