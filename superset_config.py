import os

# Secure key for signing session cookies and encrypting sensitive data
SECRET_KEY = 'oKeZfzwtm_KR703J9s-r3h6ald42h0WiklvdW350SrNuOiVcvbL4nV-k'

# PostgreSQL connection string for Superset metadata
SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:your_password@postgres:5432/superset_metadata'

# Limit rows in SQL query results
ROW_LIMIT = 5000

# Flask-WTF CSRF settings
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365  # 1 year
