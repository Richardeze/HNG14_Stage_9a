import asyncio
import logging
import random
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class EmailHandler:
    """
    Simulates sending an email.
    Real logic runs — we validate the payload, simulate network delay,
    and randomly fail 20% of the time to test retry logic.
    """

    async def handle(self, payload: dict) -> dict:
        # Validate required fields
        to = payload.get("to")
        subject = payload.get("subject")
        body = payload.get("body", "No body provided")

        if not to or not subject:
            raise ValueError("Email payload missing 'to' or 'subject'")

        if "@" not in to:
            raise ValueError(f"Invalid email address: {to}")

        delay = random.uniform(0.5, 2.0)
        await asyncio.sleep(delay)

        if random.random() < 0.20:
            raise ConnectionError("SMTP server unavailable (simulated failure)")

        result = {
            "status": "sent",
            "to": to,
            "subject": subject,
            "body": body,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "simulated_message_id": f"msg_{random.randint(100000, 999999)}",
        }

        logger.info(
            "Email sent: to=%s subject=%s message_id=%s",
            to,
            subject,
            result["simulated_message_id"],
        )

        return result
    
class WebhookHandler:
    """
    Simulates delivering a webhook to an external URL.
    Validates the payload, simulates HTTP POST, random 15% failure rate.
    """

    async def handle(self, payload: dict) -> dict:
        url = payload.get("url")
        data = payload.get("data", {})

        if not url:
            raise ValueError("Webhook payload missing 'url'")

        if not url.startswith("http"):
            raise ValueError(f"Invalid webhook URL: {url}")

        delay = random.uniform(0.3, 1.5)
        await asyncio.sleep(delay)

        if random.random() < 0.15:
            raise ConnectionError("Webhook endpoint unreachable (simulated failure)")

        result = {
            "status": "delivered",
            "url": url,
            "data": data,
            "delivered_at": datetime.now(timezone.utc).isoformat(),
            "response_code": 200,
        }

        logger.info("Webhook delivered: url=%s", url)
        return result


class LogProcessor:
    """
    Simulates processing a log entry.
    Parses the log, checks severity, simulates writing to a log store.
    """

    async def handle(self, payload: dict) -> dict:
        log_entry = payload.get("log_entry")
        severity = payload.get("severity", "info").lower()

        if not log_entry:
            raise ValueError("Log payload missing 'log_entry'")

        valid_severities = {"debug", "info", "warning", "error", "critical"}
        if severity not in valid_severities:
            raise ValueError(f"Invalid severity: {severity}")

        await asyncio.sleep(random.uniform(0.1, 0.5))

        if random.random() < 0.10:
            raise RuntimeError("Log store write failed (simulated failure)")

        result = {
            "status": "processed",
            "log_entry": log_entry,
            "severity": severity,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "word_count": len(log_entry.split()),
        }

        logger.info("Log processed: severity=%s words=%d", severity, result["word_count"])
        return result


HANDLERS = {
    "send_email": EmailHandler(),
    "webhook_delivery": WebhookHandler(),
    "log_processing": LogProcessor(),
}


async def run_handler(job_type: str, payload: dict) -> dict:
    """
    Main entry point. Called by the worker for every job.
    Looks up the right handler and runs it.
    Raises ValueError if job type is unknown.
    """
    handler = HANDLERS.get(job_type)
    if not handler:
        raise ValueError(f"No handler registered for job type: '{job_type}'")

    logger.info("Running handler: job_type=%s", job_type)
    result = await handler.handle(payload)
    return result