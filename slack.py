import os
import sys
from dotenv import load_dotenv, dotenv_values
from slack_sdk import WebClient

load_dotenv()

def slack():
    slack_token = os.environ['SLACK_TOKEN']
    slack_client = WebClient(token=slack_token)
    slack_client.chat_postMessage(channel='#tests', text="Hello world")

slack()
