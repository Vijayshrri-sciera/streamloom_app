import datetime
import json
from snowflake.connector import connect, DictCursor
from email_utils import notify_subscribers, notify_developers
from config import get_snowflake_connection
import math


def fetch_results_and_update_config():
    try:
        # Connect to Snowflake
        # logging.info("Script started")
        print("Script started")
        # logging.info("Connecting to Snowflake...")
        print("Connecting to Snowflake...")

        conn = get_snowflake_connection()
        # logging.info("Connected to Snowflake")
        print("Connected to Snowflake")

        # Get today's date in UTC
        today = datetime.datetime.now(datetime.timezone.utc).date()
        # logging.info(f"Today's date (UTC): {today}")
        print(f"Today's date (UTC): {today}")

        with conn.cursor(DictCursor) as cursor:
            # Fetch configurations
            query = """
                SELECT ID, SCRIPT_ID, SOURCE_ID, SOURCE_NAME, QUERY_STRING, QUEUE_TYPE, PRIORITY, 
                CREATED_BY, START_DATE, LIVE_PROCESS_STATUS, MAXCOUNT_PER_DAY
                FROM STRL_QUEUE_CONFIG
                WHERE LIVE_PROCESS_STATUS in ('Processing', 'Error')
                AND IS_ACTIVE_STATUS = 'Y'
                ORDER BY PRIORITY
            """
            # logging.debug(f"Executing query: {query}")
            print(f"Executing query: {query}")

            cursor.execute(query)
            result = cursor.fetchall()
            # logging.debug(f"Query result: {result}")
            print(f"Query result: {result}")

            configs = result
            # logging.info(f"Fetched {len(configs)} configurations")
            print(f"Fetched {len(configs)} configurations")

            if not configs:
                # logging.info("No configurations found for processing")
                print("No configurations found for processing")

            for config in configs:
                # logging.info(f"Retrieved config: {config}")
                print(f"Retrieved config: {config}")

                config_id = config.get('ID')
                query_string = config.get('QUERY_STRING')

                if not config_id or not query_string:
                    error_msg = f"QUERY_STRING is missing for config ID {config_id}"
                    # logging.error(error_msg)
                    print(error_msg)
                    notify_developers(f"Error in Config {config_id}", error_msg)
                    continue

                try:
                    # logging.info(f"Executing query for config ID {config_id}: {query_string}")
                    print(f"Executing query for config ID {config_id}: {query_string}")
                    cursor.execute(query_string)
                    result = cursor.fetchall()
                    payloads = result
                    # logging.info(f"Query result for config ID {config_id}: {payloads}")
                    print(f"Query result for config ID {config_id}: {payloads}")

                    # Calculate target days based on input count and max count per day
                    max_count_per_day = config.get('MAXCOUNT_PER_DAY')
                    input_count = len(payloads)
                    target_days = math.ceil(input_count / max_count_per_day)

                    # Prepare data for batch insert into STRL_PAYLOAD_MASTER
                    insert_data = []
                    for payload in payloads:
                        payload_json = json.dumps(payload)  # Convert dictionary to JSON string
                        insert_data.append({
                            'SOURCE_ID': config['SOURCE_ID'],
                            'SCRIPT_ID': config['SCRIPT_ID'],
                            'CONFIG_ID': config['ID'],
                            'PRIORITY': config['PRIORITY'],
                            'PAYLOAD_INPUT': payload_json,
                            'CREATED_BY': config['CREATED_BY'],
                            'QUEUE_DATE': datetime.datetime.now(datetime.timezone.utc),  # UTC timestamp
                            'IS_QUEUED': 'N',
                            'IS_AGGREGATED': 'N',
                            'IS_PARSED': 'N',
                            'IS_ACTIVE_STATUS': 'Y',
                            'LAST_UPDATED_DATETIME': datetime.datetime.now(datetime.timezone.utc)  # UTC timestamp
                        })

                    # Batch insert payloads into STRL_PAYLOAD_MASTER
                    cursor.executemany("""
                        INSERT INTO STRL_PAYLOAD_MASTER (SOURCE_ID, SCRIPT_ID, CONFIG_ID, PRIORITY, PAYLOAD_INPUT, CREATED_BY, QUEUE_DATE, IS_QUEUED, IS_AGGREGATED, IS_PARSED, LAST_UPDATED_DATETIME, IS_ACTIVE_STATUS)
                        VALUES (%(SOURCE_ID)s, %(SCRIPT_ID)s, %(CONFIG_ID)s, %(PRIORITY)s, %(PAYLOAD_INPUT)s, %(CREATED_BY)s, %(QUEUE_DATE)s, %(IS_QUEUED)s, %(IS_AGGREGATED)s, %(IS_PARSED)s, %(LAST_UPDATED_DATETIME)s, %(IS_ACTIVE_STATUS)s)
                    """, insert_data)

                    # Update config status and target days
                    cursor.execute("""
                        UPDATE STRL_QUEUE_CONFIG 
                        SET LIVE_PROCESS_STATUS = 'Fetched', INPUT_COUNT = %(input_count)s, TARGET_DAYS = %(target_days)s, LAST_UPDATED_DATETIME = %(last_updated_datetime)s
                        WHERE ID = %(config_id)s
                    """, {
                        'input_count': input_count,
                        'target_days': target_days,
                        'last_updated_datetime': datetime.datetime.now(datetime.timezone.utc),
                        'config_id': config['ID']
                    })

                    notify_subscribers(f"Config {config_id} Fetched", f"Config {config_id} has been fetched successfully and updated with {len(payloads)} records.")
                
                except Exception as e:
                    error_msg = f"Error processing config ID {config_id}: {e}"
                    # logging.error(error_msg)
                    print(error_msg)
                    cursor.execute("""
                        UPDATE STRL_QUEUE_CONFIG 
                        SET LIVE_PROCESS_STATUS = 'Error', ERROR_STRING = %(error_string)s, LAST_UPDATED_DATETIME = %(last_updated_datetime)s
                        WHERE ID = %(config_id)s
                    """, {
                        'error_string': str(e),
                        'last_updated_datetime': datetime.datetime.now(datetime.timezone.utc),
                        'config_id': config['ID']
                    })
                    notify_developers(f"Error in Config {config_id}", error_msg)

        conn.commit()
        # logging.info("All transactions committed successfully")
        print("All transactions committed successfully")

    except Exception as e:
        # logging.critical(f"Unhandled exception: {e}", exc_info=True)
        notify_developers("Critical Error in fetch_update_convert.py", str(e))
        raise

    finally:
        conn.close()
        # logging.info("Connection closed")
        print("Connection closed")

if __name__ == "__main__":
    fetch_results_and_update_config()


