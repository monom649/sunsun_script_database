#!/usr/bin/env python3
"""
追加指示データの要約チェック
"""

import sqlite3
from datetime import datetime

def instruction_summary_check():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    output_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/instruction_check_summary.txt"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔍 追加指示データ要約チェック開始")
    print("=" * 80)
    
    # 現在の状況確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    current_dialogue_count = cursor.fetchone()[0]
    
    print(f"📊 現在の状況:")
    print(f"  指示データ (is_instruction=1): {current_instruction_count}件")
    print(f"  セリフデータ (is_instruction=0): {current_dialogue_count}件")
    print()
    
    # 疑わしいパターンの要約チェック
    suspicious_patterns = [
        # 技術系
        ('%BGM%', '音響'),
        ('%SE%', '効果音'),
        ('%効果音%', '効果音'),
        ('%カット%', '編集'),
        ('%エフェクト%', '視覚効果'),
        ('%テロップ%', 'テロップ'),
        ('%演出%', '演出'),
        ('%カメラ%', 'カメラワーク'),
        ('%アングル%', 'アングル'),
        ('%トランジション%', '画面切替'),
        # キャラクター系
        ('SE', '効果音キャラ'),
        ('シーン', 'シーン説明'),
        ('%ナレーション%', 'ナレーション'),
        ('%スタッフ%', 'スタッフ指示'),
        # 記号系
        ('★%', '★記号'),
        ('●%', '●記号'),
        ('[%]', '角括弧'),
    ]
    
    report_lines = []
    report_lines.append("追加指示フラグ設定が必要なデータ要約")
    report_lines.append("=" * 80)
    report_lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    report_lines.append(f"現在の状況:")
    report_lines.append(f"  指示データ: {current_instruction_count}件")
    report_lines.append(f"  セリフデータ: {current_dialogue_count}件")
    report_lines.append("")
    
    total_missed = 0
    
    for pattern, category in suspicious_patterns:
        if pattern in ['SE', 'シーン']:
            # キャラクター名完全一致
            cursor.execute("""
                SELECT COUNT(*)
                FROM character_dialogue_unified cdu
                WHERE cdu.is_instruction = 0 
                AND cdu.character_name = ?
            """, (pattern,))
        else:
            # パターンマッチ
            cursor.execute("""
                SELECT COUNT(*)
                FROM character_dialogue_unified cdu
                WHERE cdu.is_instruction = 0 
                AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
                AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
            """, (pattern, pattern))
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"⚠️  {category} ('{pattern}'): {count}件が未フラグ設定")
            report_lines.append(f"⚠️  {category} ('{pattern}'): {count}件")
            total_missed += count
    
    print()
    print(f"📊 追加フラグ設定が必要な総計: {total_missed}件")
    
    report_lines.append("")
    report_lines.append(f"📊 追加フラグ設定が必要な総計: {total_missed}件")
    report_lines.append("")
    
    if total_missed > 0:
        report_lines.append("💡 推奨事項:")
        report_lines.append("  これらのデータを is_instruction=1 にフラグ設定することで")
        report_lines.append("  検索結果から指示データを完全に除外できます")
        report_lines.append("")
        
        # 特に多いもの上位5件の詳細
        print("📈 未フラグ件数が多い上位項目の詳細:")
        report_lines.append("📈 未フラグ件数が多い上位項目の詳細:")
        
        high_count_patterns = [
            ('%SE%', '効果音'),
            ('SE', '効果音キャラ'),
            ('シーン', 'シーン説明'),
            ('%カット%', '編集'),
            ('%エフェクト%', '視覚効果')
        ]
        
        for pattern, category in high_count_patterns:
            if pattern in ['SE', 'シーン']:
                cursor.execute("""
                    SELECT s.management_id, cdu.dialogue_text
                    FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE cdu.is_instruction = 0 
                    AND cdu.character_name = ?
                    LIMIT 3
                """, (pattern,))
            else:
                cursor.execute("""
                    SELECT s.management_id, cdu.dialogue_text
                    FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE cdu.is_instruction = 0 
                    AND (cdu.character_name LIKE ? OR cdu.dialogue_text LIKE ?)
                    AND cdu.character_name NOT IN ('サンサン', 'くもりん', 'プリル', 'ツクモ', 'BB', 'ノイズ')
                    LIMIT 3
                """, (pattern, pattern))
            
            matches = cursor.fetchall()
            if matches:
                print(f"\n  {category} 例:")
                report_lines.append(f"\n  {category} 例:")
                for management_id, dialogue in matches:
                    dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                    print(f"    {management_id}: \"{dialogue_short}\"")
                    report_lines.append(f"    {management_id}: \"{dialogue_short}\"")
    
    # ファイル出力
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\n📝 要約レポートを {output_file} に保存しました")
    
    conn.close()
    return total_missed

if __name__ == "__main__":
    total_missed = instruction_summary_check()
    
    if total_missed > 0:
        print(f"\n🔧 {total_missed}件の追加フラグ設定を推奨します")