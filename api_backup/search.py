from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        try:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Get request data
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8')) if post_data else {}
            
            # Get keyword
            keyword = data.get('keyword', '').strip()
            if not keyword:
                response = {
                    'success': False,
                    'error': 'キーワードを入力してください'
                }
            else:
                # Return demo data
                demo_results = [
                    {
                        'management_id': 'DEMO001',
                        'title': f'「{keyword}」を含むデモ動画',
                        'broadcast_date': '25/08/18',
                        'character_name': 'サンサン',
                        'dialogue': f'こんにちは！今日は{keyword}について話すよ！'
                    },
                    {
                        'management_id': 'DEMO002', 
                        'title': f'「{keyword}」デモ動画2',
                        'broadcast_date': '25/08/17',
                        'character_name': 'くもりん',
                        'dialogue': f'{keyword}って面白いよね！みんなも一緒に覚えよう！'
                    }
                ]
                
                response = {
                    'success': True,
                    'keyword': keyword,
                    'results': demo_results,
                    'count': len(demo_results)
                }
            
            self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {
                'success': False,
                'error': f'サーバーエラー: {str(e)}'
            }
            self.wfile.write(json.dumps(error_response).encode('utf-8'))