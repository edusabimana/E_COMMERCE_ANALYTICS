import happybase
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_hbase_tables():
    try:
        logger.info("Attempting to connect to HBase at localhost:9090")
        connection = happybase.Connection('localhost', port=9090)
        logger.info("Connection established, checking tables")
        
        # List existing tables to verify connection
        tables = connection.tables()
        logger.info(f"Existing tables: {tables}")

        # Create user_sessions table
        if b'user_sessions' not in tables:
            connection.create_table(
                'user_sessions',
                {
                    'session_data': dict(max_versions=3),
                    'page_views': dict(max_versions=3)
                }
            )
            logger.info("Created user_sessions table")

        # Create product_metrics table
        if b'product_metrics' not in tables:
            connection.create_table(
                'product_metrics',
                {'metrics': dict(max_versions=3)}
            )
            logger.info("Created product_metrics table")

        connection.close()
        logger.info("Connection closed")
    except Exception as e:
        logger.error(f"Error creating HBase tables: {str(e)}")
        raise

if __name__ == "__main__":
    create_hbase_tables()
    