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
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        try:
            # Set headers
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                post_data = self.rfile.read(content_length)
                try:
                    data = json.loads(post_data.decode('utf-8'))
                except:
                    data = {}
            else:
                data = {}
            
            # Get search parameters
            keyword = data.get('keyword', '').strip()
            character_filter = data.get('character_filter', '').strip()
            sort_order = data.get('sort_order', 'management_id_asc')
            limit = data.get('limit', 50)
            offset = data.get('offset', 0)  # For pagination
            search_type = data.get('search_type', 'all')  # 'all', 'title_only', 'dialogue_only'
            
            if not keyword:
                response = {
                    'success': False,
                    'error': 'キーワードを入力してください'
                }
            else:
                # Search in real database from Dropbox
                try:
                    # Dropbox direct download URL - reorganized database
                    dropbox_url = 'https://www.dropbox.com/scl/fi/ofuqpug3tstgpdqu0dvcr/youtube_search_complete_all.db?rlkey=y4al959fd7tdozin51mc9yblz&st=juxz0zgt&dl=1'
                    
                    # Download database to temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
                        urllib.request.urlretrieve(dropbox_url, temp_db.name)
                        db_path = temp_db.name
                    
                    # Connect and search
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    # Build WHERE clause based on search type
                    if search_type == 'title_only':
                        # Only search in titles
                        base_where = "WHERE s.title LIKE ?"
                        where_params = [f'%{keyword}%']
                    elif search_type == 'dialogue_only':
                        # Only search in dialogue and character names
                        base_where = "WHERE (cd.dialogue_text LIKE ? OR cd.character_name LIKE ?)"
                        where_params = [f'%{keyword}%', f'%{keyword}%']
                    else:
                        # Search all fields (default)
                        base_where = "WHERE (cd.dialogue_text LIKE ? OR cd.character_name LIKE ? OR s.title LIKE ?)"
                        where_params = [f'%{keyword}%', f'%{keyword}%', f'%{keyword}%']
                    
                    # Add character filter
                    if character_filter:
                        base_where += " AND cd.character_name LIKE ?"
                        where_params.append(f'%{character_filter}%')
                    
                    # First, get total count
                    count_query = f"""
                    SELECT COUNT(*) 
                    FROM character_dialogue cd
                    JOIN scripts s ON cd.script_id = s.id
                    {base_where}
                    """
                    
                    cursor.execute(count_query, where_params)
                    total_count = cursor.fetchone()[0]
                    
                    # For debugging: get breakdown by search type
                    debug_info = {}
                    if search_type == 'all':
                        # Count title matches
                        title_query = f"""
                        SELECT COUNT(*) 
                        FROM character_dialogue cd
                        JOIN scripts s ON cd.script_id = s.id
                        WHERE s.title LIKE ?
                        """
                        cursor.execute(title_query, [f'%{keyword}%'])
                        debug_info['title_matches'] = cursor.fetchone()[0]
                        
                        # Count dialogue matches
                        dialogue_query = f"""
                        SELECT COUNT(*) 
                        FROM character_dialogue cd
                        JOIN scripts s ON cd.script_id = s.id
                        WHERE (cd.dialogue_text LIKE ? OR cd.character_name LIKE ?)
                        """
                        cursor.execute(dialogue_query, [f'%{keyword}%', f'%{keyword}%'])
                        debug_info['dialogue_matches'] = cursor.fetchone()[0]
                        
                        # Count overlapping matches (both title and dialogue)
                        overlap_query = f"""
                        SELECT COUNT(*) 
                        FROM character_dialogue cd
                        JOIN scripts s ON cd.script_id = s.id
                        WHERE s.title LIKE ? AND (cd.dialogue_text LIKE ? OR cd.character_name LIKE ?)
                        """
                        cursor.execute(overlap_query, [f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])
                        debug_info['overlap_matches'] = cursor.fetchone()[0]
                    
                    # Then get paginated results
                    data_query = f"""
                    SELECT s.management_id, s.title, s.broadcast_date, cd.character_name, cd.dialogue_text, cd.voice_instruction, '', '', s.script_url, cd.row_number
                    FROM character_dialogue cd
                    JOIN scripts s ON cd.script_id = s.id
                    {base_where}
                    """
                    
                    # Add sorting
                    sort_map = {
                        'management_id_asc': 'ORDER BY s.management_id ASC',
                        'management_id_desc': 'ORDER BY s.management_id DESC',
                        'broadcast_date_asc': 'ORDER BY s.broadcast_date ASC',
                        'broadcast_date_desc': 'ORDER BY s.broadcast_date DESC'
                    }
                    
                    order_clause = sort_map.get(sort_order, 'ORDER BY s.management_id ASC')
                    data_query += f" {order_clause} LIMIT ? OFFSET ?"
                    
                    query_params = where_params + [limit, offset]
                    cursor.execute(data_query, query_params)
                    results = cursor.fetchall()
                    conn.close()
                    
                    # Clean up temporary file
                    os.unlink(db_path)
                    
                    # Format results
                    formatted_results = []
                    for row in results:
                        formatted_results.append({
                            'management_id': row[0] or '',
                            'title': row[1] or '',
                            'broadcast_date': row[2] or '',
                            'character_name': row[3] or '',
                            'dialogue': row[4] or '',
                            'voice_instruction': row[5] or '',
                            'filming_instruction': row[6] or '',
                            'editing_instruction': row[7] or '',
                            'script_url': row[8] or '',
                            'row_number': row[9] or 0,
                            'content_type': 'dialogue'  # All results from character_dialogue table
                        })
                    
                    response = {
                        'success': True,
                        'keyword': keyword,
                        'character_filter': character_filter,
                        'sort_order': sort_order,
                        'limit': limit,
                        'offset': offset,
                        'search_type': search_type,
                        'results': formatted_results,
                        'count': len(formatted_results),
                        'total_count': total_count,
                        'has_more': total_count > (offset + len(formatted_results)),
                        'debug_info': debug_info if debug_info else None,
                        'database_info': f'検索対象: 整理済みデータベース（実際のキャラクターセリフのみ、状況説明文は除外済み）'
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