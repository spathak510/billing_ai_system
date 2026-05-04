# app/main.py
from __future__ import annotations

import logging


from flask import Flask

from app.api import register_api_routes
from app.utils.error_notifier import send_error_notification

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

# ------------------------------------------------------------------
# App
# ------------------------------------------------------------------
app = Flask(__name__)

# Register API routes (also ensures runtime directories exist)
register_api_routes(app)


# ------------------------------------------------------------------
# Error handler for unhandled exceptions
# ------------------------------------------------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    # Log the error
    logging.exception("Unhandled exception in Flask app: %s", e)
    # Send error notification
    send_error_notification(
        subject="[Billing AI System] Unhandled Exception in Flask App",
        error=e,
        context="Flask app global error handler"
    )
    return {"error": "An internal error occurred. The admin has been notified."}, 500

# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------
if __name__ == "__main__":
    #app.run(debug=True, port=8000)
    app.run(host="0.0.0.0", port=4443, debug=True)
