import snowflake.connector

def get_snowflake_connection():
    return snowflake.connector.connect(
        user='USER',
        password='pass_key',
        account='SCIERA',
        warehouse='warehouse',
        database='db',
        schema='SCHEMA',
        role = 'ROLE'
    )

# Secret key for Flask sessions
SECRET_KEY = 'your_secret_key'

# List of subscriber email addresses
SUBSCRIBER_EMAILS = [
    "vijayshrris@sciera.com"
]

# List of developer email addresses
DEVELOPER_EMAILS = [
    "vijayshrris@sciera.com"
]


# Email API details
EMAIL_API_URL = "https://o7qnqjrf4g.execute-api.us-east-1.amazonaws.com/v1/email"
EMAIL_API_KEY = 'email_api_key'

# Additional email API parameters
APP_NAME = 'strl-model'  
APP_PAGE = 'auto-mailer'  
EMAIL_TYPE = 'strl-model'  
