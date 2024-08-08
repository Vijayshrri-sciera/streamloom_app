from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from config import get_snowflake_connection, SECRET_KEY
from datetime import datetime, timezone
from forms import LoginForm
from script_01 import check_duplicate_config, custom_sort_dataframe, assign_priorities, log_priority_changes, update_priorities
from script_02 import fetch_results_and_update_config

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, email):
        self.id = email
        self.email = email

@login_manager.user_loader
def load_user(email):
    return User(email)

@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data
        if authenticate_user(email, password):
            user = User(email)
            login_user(user)
            flash('Logged in successfully.', 'success')
            return redirect(url_for('index'))
        else:
            flash('Invalid email or password.', 'danger')
    return render_template('login.html', form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))

def authenticate_user(email, password):
    return email.endswith('@sciera.com')

def fetch_data(query):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/source_master', methods=['GET', 'POST'])
@login_required
def source_master():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_SOURCE_MASTER"
    if search:
        query += f" WHERE SOURCE_NAME ILIKE '%{search}%' OR ID::TEXT ILIKE '%{search}%'"
    sources = fetch_data(query)
    return render_template('source_master.html', sources=sources)

@app.route('/script_master', methods=['GET', 'POST'])
@login_required
def script_master():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_SCRIPT_MASTER"
    if search:
        query += f" WHERE SCRIPT_NAME ILIKE '%{search}%' OR ID::TEXT ILIKE '%{search}%'"
    scripts = fetch_data(query)
    return render_template('script_master.html', scripts=scripts)

@app.route('/queue_config', methods=['GET', 'POST'])
@login_required
def queue_config():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_QUEUE_CONFIG"
    if search:
        query += f" WHERE SOURCE_NAME ILIKE '%{search}%' OR SCRIPT_ID::TEXT ILIKE '%{search}%'"
    queue_configs = fetch_data(query)
    return render_template('queue_config.html', queue_configs=queue_configs)

@app.route('/queue_master', methods=['GET', 'POST'])
@login_required
def queue_master():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_QUEUE_MASTER"
    if search:
        query += f" WHERE QUEUE_NAME ILIKE '%{search}%' OR ID::TEXT ILIKE '%{search}%'"
    queue_masters = fetch_data(query)
    return render_template('queue_master.html', queue_masters=queue_masters)


@app.route('/payload_master', methods=['GET', 'POST'])
@login_required
def payload_master():
    search = request.args.get('search')
    base_query = "SELECT * FROM STRL_PAYLOAD_MASTER"
    count_query = "SELECT COUNT(*) FROM STRL_PAYLOAD_MASTER"
    
    if search:
        search_filter = f" WHERE QUEUE_NAME ILIKE '%{search}%' OR ID::TEXT ILIKE '%{search}%'"
        query = base_query + search_filter
    else:
        query = base_query
    
    payloads = fetch_data(query)
    total_count = len(payloads)  # Fetch the count of all records
    
    return render_template('payload_master.html', payloads=payloads, total_count=total_count)

@app.route('/fetch_payload')
def fetch_payload():
    try:
        # Call the function defined in script_02.py
        fetch_results_and_update_config()
        # return "Payload fetched successfully", 200
        query = "SELECT * FROM STRL_PAYLOAD_MASTER"
        payloads = fetch_data(query)
        return render_template('payload_master.html', payloads=payloads)
    except Exception as e:
        return str(e), 500
    
@app.route('/queue_reprocess', methods=['GET', 'POST'])
@login_required
def queue_reprocess():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_QUEUE_REPROCESS"
    if search:
        query += f" WHERE CONFIG_ID::TEXT ILIKE '%{search}%' OR SOURCE_ID::TEXT ILIKE '%{search}%'"
    reprocesses = fetch_data(query)
    return render_template('queue_reprocess.html', reprocesses=reprocesses)

@app.route('/priority_log', methods=['GET', 'POST'])
@login_required
def priority_log():
    search = request.args.get('search')
    query = "SELECT * FROM STRL_PRIORITY_LOG"
    if search:
        query += f" WHERE CONFIG_ID::TEXT ILIKE '%{search}%'"
    priority_logs = fetch_data(query)
    return render_template('priority_log.html', priority_logs=priority_logs)

@app.route('/add_source', methods=['GET', 'POST'])
@login_required
def add_source():
    if request.method == 'POST':
        source_name = request.form['source_name']
        source_domain = request.form['source_domain']
        description = request.form['description']
        maxcount_per_day = request.form['maxcount_per_day']
        is_active_status = request.form['is_active_status']
        created_by = current_user.email
        created_userid = current_user.email.split('@')[0]
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO STRL_SOURCE_MASTER (SOURCE_NAME, SOURCE_DOMAIN, DESCRIPTION, MAXCOUNT_PER_DAY, IS_ACTIVE_STATUS, CREATED_BY, CREATED_USERID) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (source_name, source_domain, description, maxcount_per_day, is_active_status, created_by, created_userid))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('source_master'))
    return render_template('add_source.html')

@app.route('/add_script', methods=['GET', 'POST'])
@login_required
def add_script():
    if request.method == 'POST':
        source_id = request.form['source_id']
        source_code_path = request.form['source_code_path']
        script_name = request.form['script_name']
        version = request.form['version']
        description = request.form['description']
        created_by = current_user.email
        is_active_status = request.form['is_active_status']
        dependency_description = request.form['dependency_description']
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO STRL_SCRIPT_MASTER (SOURCE_ID, SOURCE_CODE_PATH, SCRIPT_NAME, VERSION, DESCRIPTION, CREATED_BY, IS_ACTIVE_STATUS, DEPENDENCY_DESCRIPTION) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (source_id, source_code_path, script_name, version, description, created_by, is_active_status, dependency_description))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('script_master'))
    return render_template('add_script.html')

@app.route('/add_queue_config', methods=['GET', 'POST'])
@login_required
def add_queue_config():
    if request.method == 'POST':
        script_id = request.form['script_id']
        source_id = request.form['source_id']
        source_name = request.form['source_name']
        query_string = request.form['query_string']
        queue_type = request.form['queue_type']
        priority = request.form.get('priority', '0')  # Default to '0' if not provided
        description = request.form['description']
        frequency = request.form['frequency']
        cron_logic = request.form['cron_logic']
        maxcount_per_day = request.form['maxcount_per_day']

        # Get the current UTC timestamp
        current_utc_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        start_date = request.form.get('start_date', '').strip()
        end_date = request.form.get('end_date', '').strip()

        # Use current UTC timestamp if start_date or end_date is not provided or is empty
        if not start_date:
            start_date = current_utc_timestamp
        if not end_date:
            end_date = current_utc_timestamp

        is_active_status = request.form.get('is_active_status', 'Y')
        created_by = current_user.email
        live_process_status = "Assign_priority_pending"

        # Ensure all parameters are the correct types
        script_id = int(script_id)
        source_id = int(source_id)
        priority = int(priority)  # Convert to int
        maxcount_per_day = int(maxcount_per_day)

        # # Properly escape single quotes in the query_string
        # query_string = query_string.replace("'", "''")

        params = (
            script_id, source_id, source_name, query_string, queue_type, priority, description, 
            frequency, cron_logic, start_date, end_date, is_active_status, 
            'N', current_utc_timestamp, created_by, current_utc_timestamp, created_by, live_process_status, maxcount_per_day
        )

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Insert the new configuration and get the new ID
        cursor.execute("""
            INSERT INTO STRL_QUEUE_CONFIG (
                SCRIPT_ID, SOURCE_ID, SOURCE_NAME, QUERY_STRING, QUEUE_TYPE, PRIORITY, DESCRIPTION, 
                FREQUENCY, CRON_LOGIC, START_DATE, END_DATE, IS_ACTIVE_STATUS, IS_PRIORITY_UPDATED, 
                CREATED_DATETIME, CREATED_BY, LAST_UPDATED_DATETIME, UPDATED_BY, LIVE_PROCESS_STATUS,
                MAXCOUNT_PER_DAY
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, params)

        # Retrieve the current value of the sequence
        cursor.execute("SELECT MAX(ID) FROM STRL_QUEUE_CONFIG")
        config_id = cursor.fetchone()[0]

        # Insert the initial priority log
        log_params = (
            config_id, 0, priority, created_by, current_utc_timestamp
        )
        
        cursor.execute("""
            INSERT INTO STRL_PRIORITY_LOG (CONFIG_ID, OLD_PRIORITY, NEW_PRIORITY, UPDATED_BY, UPDATED_DATETIME) 
            VALUES (%s, %s, %s, %s, %s)
        """, log_params)

        # Check existence of the priority
        if priority == 0:
            print('Priority is 0')
            update_priorities()

        else:
            print('Priority is not 0')

            # Prepare the query to check if the priority exists in the table
            query = """
            SELECT COUNT(*) 
            FROM STRL_QUEUE_CONFIG 
            WHERE PRIORITY = %s 
            AND ID != %s;
            """

            # Execute the query with the specified priority
            cursor.execute(query, (priority, config_id))

            # Fetch the result
            result = cursor.fetchone()
            priority_count = result[0] if result else 0

            print(f"Priority count for {priority}: {priority_count}")

            if priority_count > 0:
                print('Priority seems to be duplicated, hence updating all priorities')
                update_priorities()
            else:
                print('Priority is new to the list, hence added')

        # Update all the queue configurations to 'Processing'
        cursor.execute("""
            UPDATE STRL_QUEUE_CONFIG 
            SET LIVE_PROCESS_STATUS = 'Processing';
        """)

        # Ensure to commit after all operations
        conn.commit()

        cursor.close()
        conn.close()

        return redirect(url_for('queue_config'))
    
    return render_template('add_queue_config.html')

@app.route('/add_queue_master', methods=['GET', 'POST'])
@login_required
def add_queue_master():
    if request.method == 'POST':
        source_id = request.form['source_id']
        script_id = request.form['script_id']
        source_name = request.form['source_name']
        queue_name = request.form['queue_name']
        queue_date = request.form['queue_date']
        queue_type = request.form['queue_type']
        priority = request.form['priority']
        process_status = request.form['process_status']
        is_queued = request.form['is_queued']
        is_aggregated = request.form['is_aggregated']
        is_parsed = request.form['is_parsed']
        created_by = current_user.email
        is_dropped = request.form['is_dropped']
        dropped_date = request.form['dropped_date']
        input_data_index = request.form['input_data_index']
        error_details = request.form['error_details']
        retry_count = request.form['retry_count']
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO STRL_QUEUE_MASTER (SOURCE_ID, SCRIPT_ID, SOURCE_NAME, QUEUE_NAME, QUEUE_DATE, QUEUE_TYPE, PRIORITY, PROCESS_STATUS, IS_QUEUED, IS_AGGREGATED, IS_PARSED, CREATED_BY, IS_DROPPED, DROPPED_DATE, INPUT_DATA_INDEX, ERROR_DETAILS, RETRY_COUNT) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (source_id, script_id, source_name, queue_name, queue_date, queue_type, priority, process_status, is_queued, is_aggregated, is_parsed, created_by, is_dropped, dropped_date, input_data_index, error_details, retry_count))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('queue_master'))
    return render_template('add_queue_master.html')

@app.route('/edit_source/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_source(id):
    if request.method == 'POST':
        source_name = request.form['source_name']
        source_domain = request.form['source_domain']
        description = request.form['description']
        maxcount_per_day = request.form['maxcount_per_day']
        is_active_status = request.form['is_active_status']
        updated_by = current_user.email
        updated_userid = current_user.email.split('@')[0]
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE STRL_SOURCE_MASTER 
            SET SOURCE_NAME=%s, SOURCE_DOMAIN=%s, DESCRIPTION=%s, MAXCOUNT_PER_DAY=%s, IS_ACTIVE_STATUS=%s, UPDATED_BY=%s, UPDATED_USERID=%s, LAST_UPDATED_DATETIME=convert_timezone('UTC', current_timestamp())
            WHERE ID=%s
        """, (source_name, source_domain, description, maxcount_per_day, is_active_status, updated_by, updated_userid, id))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('source_master'))
    query = f"SELECT * FROM STRL_SOURCE_MASTER WHERE ID={id}"
    source = fetch_data(query)[0]
    return render_template('edit_source.html', source=source)

@app.route('/edit_script/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_script(id):
    if request.method == 'POST':
        source_code_path = request.form['source_code_path']
        script_name = request.form['script_name']
        version = request.form['version']
        description = request.form['description']
        is_active_status = request.form['is_active_status']
        dependency_description = request.form['dependency_description']
        updated_by = current_user.email
        updated_userid = current_user.email.split('@')[0]
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE STRL_SCRIPT_MASTER 
            SET SOURCE_CODE_PATH=%s, SCRIPT_NAME=%s, VERSION=%s, DESCRIPTION=%s, IS_ACTIVE_STATUS=%s, DEPENDENCY_DESCRIPTION=%s, UPDATED_BY=%s, UPDATED_USERID=%s, LAST_UPDATED_DATETIME=convert_timezone('UTC', current_timestamp())
            WHERE ID=%s
        """, (source_code_path, script_name, version, description, is_active_status, dependency_description, updated_by, updated_userid, id))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('script_master'))
    
    query = f"SELECT * FROM STRL_SCRIPT_MASTER WHERE ID={id}"
    script = fetch_data(query)
    
    if script:
        script = script[0]
    else:
        script = {}
    
    print("Fetched script data: ", script)  # Debugging statement
    
    return render_template('edit_script.html', script=script)

@app.route('/edit_queue_config/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_queue_config(id):
    # Fetch existing queue configuration to get the old priority and status
    query = f"SELECT * FROM STRL_QUEUE_CONFIG WHERE ID={id}"
    queue_config = fetch_data(query)[0]
    old_priority = queue_config[6]  # Assuming priority is the 7th column (index 6)
    old_active_status = queue_config[27]  # Assuming IS_ACTIVE_STATUS is the 28th column (index 27)
    print(f"Editing config {id}: old_priority={old_priority}, old_active_status={old_active_status}")  # Debugging statement

    if request.method == 'POST':
        # Fetch form data
        script_id = request.form['script_id']
        source_id = request.form['source_id']
        source_name = request.form['source_name']
        query_string = request.form['query_string']
        queue_type = request.form['queue_type']
        new_priority = int(request.form.get('priority', '0'))  # New priority from the form
        description = request.form['description']
        frequency = request.form['frequency']
        cron_logic = request.form['cron_logic']
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        new_active_status = request.form['is_active_status']
        maxcount_per_day = request.form['maxcount_per_day']
        updated_by = current_user.email
        current_utc_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Update the queue config
        cursor.execute("""
            UPDATE STRL_QUEUE_CONFIG 
            SET SCRIPT_ID=%s, SOURCE_ID=%s, SOURCE_NAME=%s, QUERY_STRING=%s, QUEUE_TYPE=%s, PRIORITY=%s, 
                DESCRIPTION=%s, FREQUENCY=%s, CRON_LOGIC=%s, START_DATE=%s, END_DATE=%s, IS_ACTIVE_STATUS=%s, 
                LAST_UPDATED_DATETIME=CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), UPDATED_BY=%s, MAXCOUNT_PER_DAY=%s
            WHERE ID=%s
        """, (script_id, source_id, source_name, query_string, queue_type, new_priority, description, frequency, cron_logic, start_date, end_date, new_active_status, updated_by, maxcount_per_day, id))

        
        # Check if priority was updated
        if new_priority != old_priority:
            cursor.execute("UPDATE STRL_QUEUE_CONFIG SET IS_PRIORITY_UPDATED='Y', LAST_UPDATED_DATETIME = %s WHERE ID=%s", (current_utc_timestamp,id,))
            cursor.execute("""
            INSERT INTO STRL_PRIORITY_LOG (CONFIG_ID, OLD_PRIORITY, NEW_PRIORITY, UPDATED_BY, UPDATED_DATETIME) 
            VALUES (%s, %s, %s, %s, %s)""", (id, old_priority, new_priority, updated_by, current_utc_timestamp))
            print(f"Priority updated for config {id}: old_priority={old_priority}, new_priority={new_priority}")  # Debugging statement
    
        print(f"Config {id} updated as per edit request with priority {new_priority}")  # Debugging statement
        
        conn.commit()
        cursor.close()
        conn.close()

        # Update priorities after editing a config
        update_priorities()

        return redirect(url_for('queue_config'))

    return render_template('edit_queue_config.html', queue_config=queue_config)

@app.route('/delete_queue_config/<int:id>', methods=['POST'])
@login_required
def delete_queue_config(id):
    """Deletes a specific queue configuration."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM STRL_QUEUE_CONFIG WHERE ID = %s", (id,))
    conn.commit()
    cursor.close()
    conn.close()

    # Update priorities after deleting a config
    update_priorities()

    return redirect(url_for('queue_config'))

@app.route('/edit_queue_master/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_queue_master(id):
    if request.method == 'POST':
        source_id = request.form['source_id']
        script_id = request.form['script_id']
        source_name = request.form['source_name']
        queue_name = request.form['queue_name']
        queue_date = request.form['queue_date']
        queue_type = request.form['queue_type']
        priority = request.form['priority']
        process_status = request.form['process_status']
        is_queued = request.form['is_queued']
        is_aggregated = request.form['is_aggregated']
        is_parsed = request.form['is_parsed']
        created_by = current_user.email
        is_dropped = request.form['is_dropped']
        dropped_date = request.form['dropped_date']
        input_data_index = request.form['input_data_index']
        error_details = request.form['error_details']
        retry_count = request.form['retry_count']
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE STRL_QUEUE_MASTER SET SOURCE_ID=%s, SCRIPT_ID=%s, SOURCE_NAME=%s, QUEUE_NAME=%s, QUEUE_DATE=%s, QUEUE_TYPE=%s, PRIORITY=%s, PROCESS_STATUS=%s, IS_QUEUED=%s, IS_AGGREGATED=%s, IS_PARSED=%s, CREATED_BY=%s, IS_DROPPED=%s, DROPPED_DATE=%s, INPUT_DATA_INDEX=%s, ERROR_DETAILS=%s, RETRY_COUNT=%s, LAST_UPDATED_DATETIME=convert_timezone('UTC', current_timestamp()) WHERE ID=%s
        """, (source_id, script_id, source_name, queue_name, queue_date, queue_type, priority, process_status, is_queued, is_aggregated, is_parsed, created_by, is_dropped, dropped_date, input_data_index, error_details, retry_count, id))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('queue_master'))
    query = f"SELECT * FROM STRL_QUEUE_MASTER WHERE ID={id}"
    queue_master = fetch_data(query)[0]
    return render_template('edit_queue_master.html', queue_master=queue_master)


if __name__ == '__main__':
    app.run(debug=True)
