from http.server import BaseHTTPRequestHandler
import json
import sqlite3
import os
import urllib.request
import tempfile

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        try:
            # Set headers
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Dropbox direct download URL
                dropbox_url = os.environ.get('DATABASE_URL', 'https://www.dropbox.com/scl/fi/ofuqpug3tstgpdqu0dvcr/youtube_search_complete_all.db?rlkey=y4al959fd7tdozin51mc9yblz&st=4prlo6zj&dl=1')
                
                # Download database to temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
                    urllib.request.urlretrieve(dropbox_url, temp_db.name)
                    db_path = temp_db.name
                
                # Connect and analyze dates
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get date range for 2025
                cursor.execute("""
                    SELECT MIN(s.broadcast_date) as min_date, MAX(s.broadcast_date) as max_date, COUNT(*) as total_2025_scripts
                    FROM scripts s
                    WHERE s.broadcast_date LIKE '25/%'
                """)
                result_2025 = cursor.fetchone()
                
                # Get overall date range
                cursor.execute("""
                    SELECT MIN(s.broadcast_date) as min_date, MAX(s.broadcast_date) as max_date, COUNT(*) as total_scripts
                    FROM scripts s
                    WHERE s.broadcast_date IS NOT NULL AND s.broadcast_date != ''
                """)
                result_all = cursor.fetchone()
                
                # Get 2025 data by month
                cursor.execute("""
                    SELECT SUBSTR(s.broadcast_date, 4, 2) as month, COUNT(*) as script_count
                    FROM scripts s
                    WHERE s.broadcast_date LIKE '25/%'
                    GROUP BY SUBSTR(s.broadcast_date, 4, 2)
                    ORDER BY month
                """)
                monthly_2025 = cursor.fetchall()
                
                # Get sample of recent dates
                cursor.execute("""
                    SELECT s.broadcast_date, s.title, s.management_id
                    FROM scripts s
                    WHERE s.broadcast_date LIKE '25/%'
                    ORDER BY s.broadcast_date DESC
                    LIMIT 10
                """)
                recent_samples = cursor.fetchall()
                
                conn.close()
                os.unlink(db_path)
                
                response = {
                    'success': True,
                    'year_2025': {
                        'min_date': result_2025[0],
                        'max_date': result_2025[1],
                        'total_scripts': result_2025[2],
                        'monthly_breakdown': dict(monthly_2025),
                        'recent_samples': [
                            {'date': row[0], 'title': row[1], 'management_id': row[2]}
                            for row in recent_samples
                        ]
                    },
                    'database_overall': {
                        'min_date': result_all[0],
                        'max_date': result_all[1],
                        'total_scripts': result_all[2]
                    }
                }
                
            except Exception as db_error:
                response = {
                    'success': False,
                    'error': f'データベースエラー: {str(db_error)}'
                }
            
            # Send response
            response_json = json.dumps(response, ensure_ascii=False)
            self.wfile.write(response_json.encode('utf-8'))
            
        except Exception as e:
            # Error response
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {
                'success': False,
                'error': f'サーバーエラー: {str(e)}'
            }
            self.wfile.write(json.dumps(error_response, ensure_ascii=False).encode('utf-8'))