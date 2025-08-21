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
            offset = data.get('offset', 0)
            search_type = data.get('search_type', 'all')
            
            if not keyword:
                response = {
                    'success': False,
                    'error': 'キーワードを入力してください'
                }
            else:
                try:
                    # Updated Dropbox URL - fully recovered database (277,270 dialogues)
                    dropbox_url = os.environ.get('DATABASE_URL', 'https://www.dropbox.com/scl/fi/ofuqpug3tstgpdqu0dvcr/youtube_search_complete_all.db?rlkey=y4al959fd7tdozin51mc9yblz&st=yxkhp32b&dl=1')
                    
                    # Download database to temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
                        urllib.request.urlretrieve(dropbox_url, temp_db.name)
                        db_path = temp_db.name
                    
                    # Connect and search using optimized queries for perfect separation
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    # Build WHERE clause - optimized for perfectly separated data
                    if search_type == 'title_only':
                        # Only search in titles
                        base_where = "WHERE s.title LIKE ?"
                        where_params = [f'%{keyword}%']
                        # Count query for titles
                        count_query = """
                        SELECT COUNT(DISTINCT s.id)
                        FROM scripts s
                        WHERE s.title LIKE ?
                        """
                    elif search_type == 'dialogue_only':
                        # Search ONLY in dialogue (perfectly separated dialogue data)
                        base_where = """WHERE cdu.is_instruction = 0 AND (
                            cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ?
                        )"""
                        where_params = [f'%{keyword}%', f'%{keyword}%']
                        # Count query for dialogue only
                        count_query = """
                        SELECT COUNT(*) 
                        FROM character_dialogue_unified cdu
                        JOIN scripts s ON cdu.script_id = s.id
                        WHERE cdu.is_instruction = 0 AND (
                            cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ?
                        )
                        """
                    else:
                        # Search all (titles + dialogue, instructions hidden)
                        base_where = """WHERE (
                            s.title LIKE ? OR 
                            (cdu.is_instruction = 0 AND (cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ?))
                        )"""
                        where_params = [f'%{keyword}%', f'%{keyword}%', f'%{keyword}%']
                        # Count query for combined search
                        count_query = """
                        SELECT COUNT(*) 
                        FROM character_dialogue_unified cdu
                        JOIN scripts s ON cdu.script_id = s.id
                        WHERE (
                            s.title LIKE ? OR 
                            (cdu.is_instruction = 0 AND (cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ?))
                        )
                        """
                    
                    # Add character filter (only applies to dialogue data)
                    if character_filter:
                        if search_type == 'title_only':
                            # For title search, character filter doesn't apply
                            pass
                        else:
                            base_where += " AND cdu.character_name LIKE ?"
                            where_params.append(f'%{character_filter}%')
                            if search_type == 'dialogue_only':
                                count_query += " AND cdu.character_name LIKE ?"
                            else:
                                count_query += " AND (s.title LIKE ? OR cdu.character_name LIKE ?)"
                    
                    # Get total count
                    if search_type == 'title_only':
                        cursor.execute(count_query, [f'%{keyword}%'])
                    else:
                        cursor.execute(count_query, where_params)
                    total_count = cursor.fetchone()[0]
                    
                    # Debug info for perfectly separated data
                    debug_info = {}
                    if search_type == 'all':
                        # Count title matches
                        cursor.execute("SELECT COUNT(DISTINCT s.id) FROM scripts s WHERE s.title LIKE ?", [f'%{keyword}%'])
                        debug_info['title_matches'] = cursor.fetchone()[0]
                        
                        # Count dialogue matches (perfectly separated dialogue only)  
                        cursor.execute("""
                            SELECT COUNT(*) FROM character_dialogue_unified cdu
                            WHERE cdu.is_instruction = 0 AND (cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ?)
                        """, [f'%{keyword}%', f'%{keyword}%'])
                        debug_info['dialogue_matches'] = cursor.fetchone()[0]
                        
                        # Count instruction matches (perfectly separated, hidden from results)
                        cursor.execute("""
                            SELECT COUNT(*) FROM character_dialogue_unified cdu
                            WHERE cdu.is_instruction = 1 AND cdu.filming_audio_instructions LIKE ?
                        """, [f'%{keyword}%'])
                        debug_info['instruction_matches_hidden'] = cursor.fetchone()[0]
                    
                    # Get paginated results - optimized query for perfect separation
                    if search_type == 'title_only':
                        data_query = f"""
                        SELECT DISTINCT s.management_id, s.title, s.broadcast_date, '', '', '', s.script_url, 0, 0, 'title'
                        FROM scripts s
                        {base_where}
                        """
                    else:
                        data_query = f"""
                        SELECT s.management_id, s.title, s.broadcast_date, cdu.character_name, cdu.dialogue_text, 
                               '', s.script_url, cdu.row_number, cdu.is_instruction, 'dialogue'
                        FROM character_dialogue_unified cdu
                        JOIN scripts s ON cdu.script_id = s.id
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
                    
                    # Format results - optimized for perfectly separated data
                    formatted_results = []
                    for row in results:
                        management_id, title, broadcast_date, character_name, dialogue_text, _, script_url, row_number, is_instruction, content_type = row
                        
                        # All results are valid (perfect separation means no contamination)
                        if content_type == 'title':
                            formatted_results.append({
                                'management_id': management_id or '',
                                'title': title or '',
                                'broadcast_date': broadcast_date or '',
                                'character_name': '',
                                'dialogue': '',
                                'voice_instruction': '',
                                'filming_instruction': '',
                                'editing_instruction': '',
                                'script_url': script_url or '',
                                'row_number': 0,
                                'content_type': 'title',
                                'is_instruction': False
                            })
                        else:
                            # Dialogue entry (perfectly separated)
                            formatted_results.append({
                                'management_id': management_id or '',
                                'title': title or '',
                                'broadcast_date': broadcast_date or '',
                                'character_name': character_name or '',
                                'dialogue': dialogue_text or '',
                                'voice_instruction': '',
                                'filming_instruction': '',  # Always empty (perfect separation)
                                'editing_instruction': '',
                                'script_url': script_url or '',
                                'row_number': row_number or 0,
                                'content_type': 'dialogue',
                                'is_instruction': False  # Always false for search results
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
                        'database_info': '完全復旧データベース（セリフ277,270件、指示4,774件）- 全データ復旧済み'
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