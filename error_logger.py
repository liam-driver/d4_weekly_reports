from datetime import datetime
import json

def log_error(message):
    ts = datetime.now()
    with open("errors/error.txt","a", encoding="utf-8")as f:
        f.write(f"{ts} | {message}\n")