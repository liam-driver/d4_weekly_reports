from datetime import datetime
import os

def log_error(message):
    ts = datetime.now()
    os.makedirs("errors", exist_ok=True)
    with open("errors/error.txt", "a", encoding="utf-8") as f:
        f.write(f"{ts} | {message}\n")