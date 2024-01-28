
import os
import ssl

from dotenv import load_dotenv
from slack_sdk import WebClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def mysql_connection() -> Engine:
    """Connect to MySQL. This involves loading environment variables from file"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv()
    engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:3306"
    )
    return engine

def slack_client(token: str) -> WebClient:
    """Instantiate Slack Web client"""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return WebClient(token=token, ssl=ssl_context)
