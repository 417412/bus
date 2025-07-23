#!/usr/bin/env python3
"""
ETL Monitoring script.

This script monitors the ETL daemon's status file and logs to detect issues
and send alerts when necessary.
"""

import os
import sys
import json
import time
import logging
import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

# Import configuration
from src.config.settings import LOGGING_CONFIG

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG.get("level", "INFO")),
    format=LOGGING_CONFIG.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.FileHandler("logs/etl_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_monitor")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Monitor ETL daemon status and send alerts"
    )
    parser.add_argument(
        "--status-file",
        type=str,
        default="etl_status.json",
        help="ETL status file to monitor (default: etl_status.json)"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=900,  # 15 minutes
        help="Check interval in seconds (default: 900)"
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=7200,  # 2 hours
        help="Maximum age of status file in seconds before alerting (default: 7200)"
    )
    parser.add_argument(
        "--error-threshold",
        type=float,
        default=0.1,  # 10%
        help="Error rate threshold before alerting (0-1, default: 0.1)"
    )
    parser.add_argument(
        "--slack-webhook",
        type=str,
        help="Slack webhook URL for alerts"
    )
    parser.add_argument(
        "--email-to",
        type=str,
        help="Email address to send alerts to"
    )
    parser.add_argument(
        "--email-from",
        type=str,
        default="etl_monitor@localhost",
        help="Email address to send alerts from"
    )
    parser.add_argument(
        "--smtp-server",
        type=str,
        default="localhost",
        help="SMTP server for sending email alerts"
    )
    parser.add_argument(
        "--smtp-port",
        type=int,
        default=25,
        help="SMTP port for sending email alerts"
    )
    return parser.parse_args()


def read_status_file(status_file: str) -> Optional[Dict[str, Any]]:
    """Read the ETL status file."""
    try:
        if not os.path.exists(status_file):
            logger.warning(f"Status file {status_file} does not exist")
            return None
            
        with open(status_file, 'r') as f:
            status = json.load(f)
            
        # Convert string timestamps to datetime objects
        for key in ['start_time', 'end_time']:
            if key in status and isinstance(status[key], str):
                status[key] = datetime.fromisoformat(status[key])
                
        return status
    except Exception as e:
        logger.error(f"Error reading status file: {e}")
        return None


def check_etl_status(status: Dict[str, Any], max_age: int, error_threshold: float) -> List[str]:
    """
    Check ETL status for issues.
    
    Args:
        status: ETL status dictionary
        max_age: Maximum age of status file in seconds before alerting
        error_threshold: Error rate threshold before alerting (0-1)
        
    Returns:
        List of alert messages
    """
    alerts = []
    now = datetime.now()
    
    # Check status file age
    if 'end_time' in status:
        status_age = (now - status['end_time']).total_seconds()
        if status_age > max_age:
            alerts.append(f"ETL status file is {status_age//60:.0f} minutes old")
    
    # Check status
    if status.get('status') == 'failed':
        alerts.append(f"ETL job failed: {status.get('error', 'Unknown error')}")
    
    # Check error rate
    if 'processed_records' in status and status['processed_records'] > 0:
        error_count = status.get('error_count', 0)
        processed = status['processed_records']
        error_rate = error_count / processed
        
        if error_rate > error_threshold:
            alerts.append(f"High error rate: {error_rate:.2%} ({error_count}/{processed})")
    
    # Check duration if job is running too long
    if status.get('status') == 'running' and 'start_time' in status:
        running_time = (now - status['start_time']).total_seconds()
        if running_time > max_age:
            alerts.append(f"ETL job has been running for {running_time//60:.0f} minutes")
    
    return alerts


def send_email_alert(alerts: List[str], args: argparse.Namespace) -> bool:
    """Send email alert."""
    if not args.email_to:
        return False
        
    try:
        msg = MIMEMultipart()
        msg['From'] = args.email_from
        msg['To'] = args.email_to
        msg['Subject'] = f"ETL Alert: {alerts[0]}"
        
        body = "ETL Monitoring has detected the following issues:\n\n"
        body += "\n".join([f"- {alert}" for alert in alerts])
        body += "\n\nPlease check the ETL daemon logs for more information."
        
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(args.smtp_server, args.smtp_port) as server:
            server.send_message(msg)
            
        logger.info(f"Sent email alert to {args.email_to}")
        return True
    except Exception as e:
        logger.error(f"Error sending email alert: {e}")
        return False


def send_slack_alert(alerts: List[str], args: argparse.Namespace) -> bool:
    """Send Slack alert."""
    if not args.slack_webhook:
        return False
        
    try:
        import requests
        
        message = "*ETL Alert*\n\n"
        message += "\n".join([f"• {alert}" for alert in alerts])
        
        payload = {
            "text": message,
            "username": "ETL Monitor",
            "icon_emoji": ":warning:"
        }
        
        response = requests.post(args.slack_webhook, json=payload)
        response.raise_for_status()
        
        logger.info(f"Sent Slack alert")
        return True
    except ImportError:
        logger.error("Requests library not installed, unable to send Slack alert")
        return False
    except Exception as e:
        logger.error(f"Error sending Slack alert: {e}")
        return False


def main():
    """Main function."""
    args = parse_args()
    logger.info("ETL Monitor starting")
    
    while True:
        try:
            # Read status file
            status = read_status_file(args.status_file)
            
            if status:
                # Check for issues
                alerts = check_etl_status(status, args.max_age, args.error_threshold)
                
                if alerts:
                    logger.warning(f"Detected {len(alerts)} issues: {', '.join(alerts)}")
                    
                    # Send alerts
                    email_sent = send_email_alert(alerts, args)
                    slack_sent = send_slack_alert(alerts, args)
                    
                    if not email_sent and not slack_sent:
                        logger.warning("No alerts were sent (no alert methods configured)")
                else:
                    logger.info("ETL status check passed, no issues detected")
            
            # Sleep until next check
            logger.debug(f"Sleeping for {args.check_interval} seconds")
            time.sleep(args.check_interval)
            
        except KeyboardInterrupt:
            logger.info("ETL Monitor stopping")
            break
        except Exception as e:
            logger.error(f"Error in ETL Monitor: {e}")
            time.sleep(60)  # Sleep for a minute before retrying
    
    return 0


if __name__ == "__main__":
    exit(main())