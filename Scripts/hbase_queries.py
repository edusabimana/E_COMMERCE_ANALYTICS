import happybase
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_user_sessions(user_id, start_date, end_date):
    try:
        connection = happybase.Connection('localhost')
        connection.open()
        table = connection.table('user_sessions')
        start_ts = datetime.fromisoformat(end_date).timestamp()
        end_ts = datetime.fromisoformat(start_date).timestamp()
        start_row = f"{user_id}:{int(999999999999 - start_ts)}"
        end_row = f"{user_id}:{int(999999999999 - end_ts)}"
        rows = table.scan(row_start=start_row, row_stop=end_row)
        result = [(row[0].decode(), row[1]) for row in rows]
        connection.close()
        logger.info(f"Retrieved {len(result)} sessions for user {user_id}")
        return result
    except Exception as e:
        logger.error(f"Error querying user sessions: {str(e)}")
        raise

if __name__ == "__main__":
    sessions = get_user_sessions('user_000042', '2025-03-01', '2025-03-31')
    print("User Sessions:", sessions)