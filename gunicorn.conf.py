# Gunicorn config - ensures APScheduler works correctly
preload_app = True
workers = 2
timeout = 120
