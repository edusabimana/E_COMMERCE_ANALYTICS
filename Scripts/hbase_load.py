import happybase
import json
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_sessions_to_hbase():
    try:
        connection = happybase.Connection('localhost')
        connection.open()
        table = connection.table('user_sessions')
        session_files = [f for f in os.listdir('data/sessions') if f.endswith('.json')]

        for file in session_files:
            with open(f'data/sessions/{file}', 'r') as f:
                sessions = json.load(f)
            with table.batch(batch_size=1000) as batch:
                for session in sessions:
                    user_id = session['user_id']
                    timestamp = datetime.fromisoformat(session['start_time']).timestamp()
                    reverse_ts = f"{int(999999999999 - timestamp)}"
                    row_key = f"{user_id}:{reverse_ts}"
                    data = {
                        'session_data:session_id': session['session_id'],
                        'session_data:start_time': session['start_time'],
                        'session_data:end_time': session['end_time'],
                        'session_data:duration': str(session['duration_seconds']),
                        'session_data:geo_data': json.dumps(session['geo_data']),
                        'session_data:device_profile': json.dumps(session['device_profile']),
                        'session_data:viewed_products': json.dumps(session['viewed_products']),
                        'session_data:conversion_status': session['conversion_status']
                    }
                    for i, pv in enumerate(session['page_views']):
                        data[f'page_views:pv_{i}_timestamp'] = pv['timestamp']
                        data[f'page_views:pv_{i}_page_type'] = pv['page_type']
                        data[f'page_views:pv_{i}_product_id'] = pv.get('product_id', '')
                        data[f'page_views:pv_{i}_category_id'] = pv.get('category_id', '')
                        data[f'page_views:pv_{i}_view_duration'] = str(pv['view_duration'])
                    batch.put(row_key, data)
            logger.info(f"Loaded {len(sessions)} sessions from {file}")
        connection.close()
    except Exception as e:
        logger.error(f"Error loading sessions to HBase: {str(e)}")
        raise

if __name__ == "__main__":
    load_sessions_to_hbase()