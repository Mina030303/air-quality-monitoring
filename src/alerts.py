"""
Discord alerting system for the Taiwan Air Quality Monitoring Platform.

Provides functionality to send notifications to Discord when important events
occur in the data pipeline (e.g., errors, anomalies, completion).
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Configure logging
logger = logging.getLogger(__name__)


def send_notification(message: str) -> None:
    """Placeholder notifier for future Discord/LINE webhook integration."""
    print(message)


def evaluate_and_notify(
    predicted_pm25: float,
    current_time: datetime,
    last_alert_time: datetime,
) -> datetime:
    """
    Evaluate routine and urgent PM2.5 alerts, then return updated last_alert_time.

    Rules:
    - Routine update at hours 7, 12, and 17.
    - Urgent alert when predicted PM2.5 > 54.5 and cooldown >= 2 hours.
    """
    routine_hours = {7, 12, 17}

    if current_time.hour in routine_hours:
        send_notification(f"Routine Update: Predicted PM2.5 is {predicted_pm25}")

    if predicted_pm25 > 54.5:
        hours_diff = (current_time - last_alert_time).total_seconds() / 3600
        if hours_diff >= 2:
            send_notification(f"URGENT: PM2.5 spike predicted ({predicted_pm25})")
            return current_time

    return last_alert_time


def send_discord_alert(message: str) -> bool:
    """
    Send an alert message to Discord via webhook.
    
    Reads the Discord webhook URL from the DISCORD_WEBHOOK_URL environment variable.
    If the webhook is not configured, the alert is silently skipped.
    
    If the alert fails to send (e.g., network error, invalid URL), logs the error
    but does not raise an exception, allowing the main pipeline to continue.
    
    Args:
        message: The alert message to send to Discord
        
    Returns:
        True if alert was sent successfully, False otherwise
        
    Example:
        >>> send_discord_alert("Pipeline failed: Unable to fetch hourly AQI data")
        True
    """
    webhook_url: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")
    
    # If webhook is not configured, silently skip
    if not webhook_url:
        logger.debug("DISCORD_WEBHOOK_URL not configured, skipping alert")
        return False
    
    try:
        # Prepare the payload for Discord
        payload = {
            "content": message
        }
        
        # Send the alert to Discord
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        
        # Check if request was successful
        if response.status_code == 204:
            logger.info(f"Discord alert sent successfully: {message[:50]}...")
            return True
        else:
            logger.error(
                f"Discord alert failed with status {response.status_code}: {response.text}"
            )
            return False
            
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Failed to send Discord alert (connection error): {e}")
        return False
    except requests.exceptions.Timeout as e:
        logger.error(f"Failed to send Discord alert (timeout): {e}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Discord alert (request error): {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while sending Discord alert: {e}")
        return False


if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test sending an alert
    test_message = "Test Alert from AQI Pipeline!"
    success = send_discord_alert(test_message)
    
    if success:
        print(f"✓ Alert sent successfully: {test_message}")
    else:
        print(f"✗ Alert failed or webhook not configured")
