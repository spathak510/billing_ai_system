# app/main.py
from __future__ import annotations

import logging

from flask import Flask

from app.api import register_api_routes

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
# Run
# ------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=8000)
