"""Email sending utility for AstroAcoustics applications.

Configuration is read from environment variables so that the same code works
across all projects (CallTrackersAdmin, SoundClass, DataUploader, etc.):

    CALLTRACKERS_SMTP_SERVER    SMTP hostname      (default: smtp.gmail.com)
    CALLTRACKERS_SMTP_PORT      SMTP port          (default: 587, STARTTLS)
    CALLTRACKERS_SMTP_USERNAME  Login / From address
    CALLTRACKERS_SMTP_PASSWORD  Login password / app-password
    CALLTRACKERS_SMTP_FROM      Displayed "From" address (default: USERNAME)
"""
from __future__ import annotations

import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from loguru import logger


def send_email(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
) -> bool:
    """Send an email via SMTP using STARTTLS.

    If *body_html* is provided a ``multipart/alternative`` message is sent so
    that clients that support HTML will render it; others fall back to plain
    text.  When *body_html* is ``None`` a plain-text message is sent.

    Returns ``True`` on success, ``False`` on failure (error is logged).
    """
    server   = os.getenv("CALLTRACKERS_SMTP_SERVER",   "smtp.gmail.com")
    port     = int(os.getenv("CALLTRACKERS_SMTP_PORT", "587"))
    username = os.getenv("CALLTRACKERS_SMTP_USERNAME",  "")
    password = os.getenv("CALLTRACKERS_SMTP_PASSWORD",  "")
    from_addr = os.getenv("CALLTRACKERS_SMTP_FROM", username)

    if not username or not password:
        logger.error(
            "SMTP credentials not configured "
            "(CALLTRACKERS_SMTP_USERNAME / CALLTRACKERS_SMTP_PASSWORD); "
            f"email not sent to {to}"
        )
        return False

    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text, "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html",  "utf-8"))
    else:
        msg = MIMEText(body_text, "plain", "utf-8")

    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(server, port, timeout=15) as smtp:
            smtp.starttls(context=context)
            smtp.login(username, password)
            smtp.sendmail(from_addr, [to], msg.as_string())
        logger.info(f"Email sent to {to}: {subject}")
        return True
    except Exception as exc:
        logger.error(f"Failed to send email to {to}: {exc}")
        return False