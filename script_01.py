from config import get_snowflake_connection
from datetime import datetime, timezone
import pandas as pd
from collections import namedtuple

def check_duplicate_config(cursor, configs):
    """Check for duplicate configurations and prepare updates if found, with case-insensitive comparison."""
    duplicate_updates = []

    for config in configs:
        # Check for duplicate entries in the database with case-insensitive comparison
        cursor.execute("""
            SELECT ID FROM strl_queue_config
            WHERE SOURCE_ID = %s 
              AND SCRIPT_ID = %s 
              AND QUERY_STRING ILIKE %s 
              AND IS_ACTIVE_STATUS = 'Y'
        """, (config.source_id, config.script_id, config.query_string))

        duplicate = cursor.fetchone()

        if duplicate and duplicate[0] != config.id:
            duplicate_updates.append((duplicate[0], config.id))
            print(f"Duplicate config found: {config.id} marked as inactive, original config: {duplicate[0]} ")

    # Return list of duplicates to update
    return duplicate_updates

def are_priorities_unique(cursor):
    """Check if all active configurations have unique priorities."""
    cursor.execute("""
        SELECT PRIORITY, COUNT(*)
        FROM strl_queue_config
        WHERE IS_ACTIVE_STATUS = 'Y'
        GROUP BY PRIORITY
        HAVING COUNT(*) > 1
    """)
    
    non_unique_priorities = cursor.fetchall()
    
    if non_unique_priorities:
        print("Non-unique priorities found. Need to adjust priorities.")
        return False

    return True

def custom_sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts a DataFrame with specific logic:
    - PRIORITY: Sorted from smallest to largest, with priority 0 moved to the end.
    - LIVE_PROCESS_STATUS: "Assign_priority_pending" before "Processing".
    - IS_PRIORITY_UPDATED: 'Y' before 'N'.
    """
    sorted_df = df.sort_values(
        by=[
            'PRIORITY',
            'LIVE_PROCESS_STATUS',
            'IS_PRIORITY_UPDATED'
        ],
        key=lambda col: (
            col.map({'Y': 0, 'N': 1, 'Assign_priority_pending': 0, 'Processing': 1})
            if col.name in ['LIVE_PROCESS_STATUS', 'IS_PRIORITY_UPDATED']
            else col.replace(0, float('inf'))
        )
    )
    return sorted_df

def assign_priorities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns priorities in order (1, 2, 3, ...) and checks if the total number of 
    configurations matches the maximum priority assigned.
    """
    # Assign priorities from 1 to the number of configurations
    df['PRIORITY'] = range(1, len(df) + 1)

    # Check if the total number of configurations matches the maximum priority
    max_priority = df['PRIORITY'].max()
    total_configs = len(df)

    if total_configs != max_priority:
        raise ValueError(f"Total configurations ({total_configs}) do not match the maximum priority assigned ({max_priority}).")

    return df

def log_priority_changes(old_df: pd.DataFrame, new_df: pd.DataFrame, updated_by: str, cursor) -> None:
    """
    Compares two DataFrames and logs priority changes, and updates the database with new priorities.
    """
    # Ensure both DataFrames are aligned by ID
    old_df = old_df.set_index('CONFIG_ID')
    new_df = new_df.set_index('CONFIG_ID')
    
    # Collect data for batch update
    update_values = []
    insert_values = []
    additional_updates = []
    current_utc_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print("Executing log of priority for configs")

    for config_id in old_df.index:
        old_priority = old_df.at[config_id, 'PRIORITY']
        new_priority = new_df.at[config_id, 'PRIORITY']
        live_process_status = old_df.at[config_id, 'LIVE_PROCESS_STATUS']
        is_priority_updated = old_df.at[config_id, 'IS_PRIORITY_UPDATED']
        
        # Check for priority change
        if old_priority != new_priority:
            insert_values.append(
                (config_id, old_priority, new_priority, updated_by, current_utc_timestamp)
            )
            update_values.append((
                new_priority,
                'Processing' if live_process_status == 'Assign_Priority_Pending' else live_process_status,
                'N' if is_priority_updated == 'Y' else is_priority_updated,
                config_id
            ))

            # Prepare additional updates for other tables
            additional_updates.append((new_priority, config_id))

            print(f"Priority auto-updated for config {config_id}: from {old_priority} to {new_priority}")
        else: 
            print(f"Priority {old_priority} is unique & unchanged.")

    # Execute batch updates if there are any changes
    if update_values:
        # Update the STRL_QUEUE_CONFIG table
        update_statement = """
        UPDATE STRL_QUEUE_CONFIG 
        SET PRIORITY = %s, LIVE_PROCESS_STATUS = %s, IS_PRIORITY_UPDATED = %s 
        WHERE ID = %s
        """
        cursor.executemany(update_statement, update_values)
        print("Batch update executed for priority changes in STRL_QUEUE_CONFIG.")

        # Update the STRL_PAYLOAD_MASTER table
        update_payload_statement = """
        UPDATE STRL_PAYLOAD_MASTER
        SET PRIORITY = %s
        WHERE CONFIG_ID = %s
        """
        cursor.executemany(update_payload_statement, additional_updates)
        print("Batch update executed for priority changes in STRL_PAYLOAD_MASTER.")

        # Update the STRL_QUEUE_MASTER table
        update_queue_master_statement = """
        UPDATE STRL_QUEUE_MASTER
        SET PRIORITY = %s
        WHERE CONFIG_ID = %s
        """
        cursor.executemany(update_queue_master_statement, additional_updates)
        print("Batch update executed for priority changes in STRL_QUEUE_MASTER.")

        # # Update the STRL_QUEUE_REPROCESS table
        # update_queue_reprocess_statement = """
        # UPDATE STRL_QUEUE_REPROCESS
        # SET PRIORITY = %s
        # WHERE CONFIG_ID = %s
        # """
        # cursor.executemany(update_queue_reprocess_statement, additional_updates)
        # print("Batch update executed for priority changes in STRL_QUEUE_REPROCESS.")

    # Execute batch insert for priority logs
    if insert_values:
        insert_statement = """
        INSERT INTO STRL_PRIORITY_LOG (CONFIG_ID, OLD_PRIORITY, NEW_PRIORITY, UPDATED_BY, UPDATED_DATETIME) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_statement, insert_values)
        print("Batch insert executed for priority logs.")

# Define the namedtuple with all the required fields
Config = namedtuple('Config', [
    'id', 'script_id', 'source_id', 'source_name', 'query_string', 'is_active_status'
])

def update_priorities():
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cursor:
            print("Starting priority update process...")

            # Fetch all configurations
            cursor.execute("""
                SELECT ID, SCRIPT_ID, SOURCE_ID, SOURCE_NAME, QUERY_STRING, IS_ACTIVE_STATUS 
                FROM strl_queue_config
            """)
            configs = cursor.fetchall()
            print(f"Fetched {len(configs)} configurations from the database.")

            configs = [Config(*config) for config in configs]

            # Check for duplicates
            duplicate_updates = check_duplicate_config(cursor, configs)

            if duplicate_updates:
                cursor.executemany("""
                    UPDATE strl_queue_config 
                    SET IS_ACTIVE_STATUS = 'N', LAST_UPDATED_DATETIME = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 
                        ERROR_STRING = 'Duplicate config detected', ERROR_DESC = 'Original config ID is %s' 
                    WHERE ID = %s
                """, duplicate_updates)
                print(f"{len(duplicate_updates)} Duplicate configs deactivated.")

            # Check if priorities are unique
            if are_priorities_unique(cursor):
                print("All active configs have unique priorities. No further action needed.")
                return  # Exit function if no further action is needed

            # Fetch all active configurations for priority updates
            cursor.execute("""
                SELECT ID, PRIORITY, IS_PRIORITY_UPDATED, LIVE_PROCESS_STATUS 
                FROM STRL_QUEUE_CONFIG 
                WHERE IS_ACTIVE_STATUS = 'Y'
            """)
            configs = cursor.fetchall()

            old_df = pd.DataFrame(configs, columns=['CONFIG_ID', 'PRIORITY', 'IS_PRIORITY_UPDATED', 'LIVE_PROCESS_STATUS'])

            print("Initial active configurations:", old_df)  

            df = custom_sort_dataframe(old_df)

            print("Sorted configurations:", df)  

            new_df = assign_priorities(df)

            print("Updated configurations:", new_df)  

            # Log changes and update the database
            log_priority_changes(old_df, new_df, updated_by='system', cursor=cursor)

            # Commit changes
            conn.commit()
    except Exception as e:
        print(f"An error occurred while updating priorities: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    update_priorities()
