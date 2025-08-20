#!/usr/bin/env python3
"""
Unprocessed URL Checker

This script checks for script URLs in the 作業進捗_new data within the date range 
2020-01-01 to 2025-09-14 that haven't been processed yet.
"""

import sqlite3
from datetime import datetime

class UnprocessedURLChecker:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/unprocessed_url_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def check_unprocessed_urls(self):
        """Check for unprocessed script URLs in the date range"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("=" * 80)
            self.log_message("2020-2025年期間内の未処理台本URL確認")
            self.log_message("=" * 80)
            
            # Get all URLs from scripts table within the date range
            # Note: broadcast_date is in YY/MM/DD format
            cursor.execute("""
                SELECT broadcast_date, title, script_url, management_id
                FROM scripts
                WHERE script_url IS NOT NULL 
                AND script_url != ''
                ORDER BY broadcast_date
            """)
            
            progress_urls = cursor.fetchall()
            
            # Filter by date range (convert YY/MM/DD to YYYY format for comparison)
            filtered_urls = []
            for broadcast_date, title, script_url, management_id in progress_urls:
                if broadcast_date:
                    # Convert YY/MM/DD to YYYY
                    parts = broadcast_date.split('/')
                    if len(parts) == 3:
                        year_part = parts[0]
                        # Assume 19 = 2019, 20-25 = 2020-2025
                        if year_part.isdigit():
                            year_int = int(year_part)
                            if year_int >= 19:  # 19 = 2019, 20+ = 2020+
                                full_year = 2000 + year_int
                                if 2020 <= full_year <= 2025:
                                    filtered_urls.append((broadcast_date, title, script_url, management_id))
            
            progress_urls = filtered_urls
            self.log_message(f"📊 2020-2025年期間の台本URL総数: {len(progress_urls)}件")
            
            if not progress_urls:
                self.log_message("❌ 指定期間内に台本URLが見つかりませんでした")
                conn.close()
                return
            
            # Check which ones have dialogue data
            processed_urls = []
            unprocessed_urls = []
            
            for broadcast_date, title, script_url, management_id in progress_urls:
                # Check if this script has dialogue data
                cursor.execute("""
                    SELECT COUNT(*) FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE s.management_id = ?
                """, (management_id,))
                
                has_dialogue = cursor.fetchone()[0] > 0
                
                if has_dialogue:
                    processed_urls.append({
                        'broadcast_date': broadcast_date,
                        'title': title,
                        'script_url': script_url,
                        'management_id': management_id
                    })
                else:
                    unprocessed_urls.append({
                        'broadcast_date': broadcast_date,
                        'title': title,
                        'script_url': script_url,
                        'management_id': management_id
                    })
            
            self.log_message(f"✅ 既に処理済み: {len(processed_urls)}件")
            self.log_message(f"❌ 未処理: {len(unprocessed_urls)}件")
            
            # Show date range analysis
            if progress_urls:
                earliest_date = min(item[0] for item in progress_urls if item[0])
                latest_date = max(item[0] for item in progress_urls if item[0])
                self.log_message(f"📅 配信日範囲: {earliest_date} 〜 {latest_date}")
            
            # Show unprocessed URLs details
            if unprocessed_urls:
                self.log_message("\n🔍 未処理台本URL詳細:")
                for i, url_info in enumerate(unprocessed_urls, 1):
                    self.log_message(f"{i:3d}. {url_info['broadcast_date']} | {url_info['management_id']} | {url_info['title'][:50] if url_info['title'] else 'タイトルなし'}...")
                    self.log_message(f"     URL: {url_info['script_url']}")
                
                # Group by year
                year_counts = {}
                for url_info in unprocessed_urls:
                    if url_info['broadcast_date']:
                        year = url_info['broadcast_date'][:4]
                        year_counts[year] = year_counts.get(year, 0) + 1
                
                self.log_message("\n📈 未処理URL年別分布:")
                for year in sorted(year_counts.keys()):
                    self.log_message(f"  {year}年: {year_counts[year]}件")
            
            # Note: unprocessed_urls already shows scripts without dialogue data
            
            conn.close()
            
            self.log_message("\n" + "=" * 80)
            self.log_message("📋 まとめ:")
            self.log_message(f"  2020-2025年総URL数: {len(progress_urls)}件")
            self.log_message(f"  セリフ抽出済み: {len(processed_urls)}件")
            self.log_message(f"  セリフ未抽出: {len(unprocessed_urls)}件")
            
            if len(unprocessed_urls) > 0:
                self.log_message(f"\n🎯 セリフ抽出推奨: {len(unprocessed_urls)}件の未処理URL")
            
            self.log_message("=" * 80)
            
            return {
                'total_urls': len(progress_urls),
                'processed': len(processed_urls),
                'unprocessed': len(unprocessed_urls),
                'unprocessed_list': unprocessed_urls
            }
            
        except Exception as e:
            self.log_message(f"❌ URL確認エラー: {str(e)}")
            return {}

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    checker = UnprocessedURLChecker(db_path)
    
    print("=== 未処理台本URL確認ツール ===")
    
    # Check for unprocessed URLs
    results = checker.check_unprocessed_urls()
    
    if results:
        print(f"\n✅ 確認完了！")
        print(f"未処理URL: {results['unprocessed']}件")
    else:
        print(f"\n❌ 確認に失敗しました")

if __name__ == "__main__":
    main()