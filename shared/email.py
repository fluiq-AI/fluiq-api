import asyncio
import logging
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from typing import Optional

import config

logger = logging.getLogger(__name__)


class EmailService:
    """Async wrapper around stdlib smtplib for transactional emails.

    When ``SMTP_HOST`` is empty the message body is logged instead of
    being delivered, so local development works without an SMTP server.
    """

    def __init__(
        self,
        host: str = config.SMTP_HOST,
        port: int = config.SMTP_PORT,
        username: str = config.SMTP_USER,
        password: str = config.SMTP_PASSWORD,
        use_tls: bool = config.SMTP_USE_TLS,
        from_email: str = config.SMTP_FROM_EMAIL,
        from_name: str = config.SMTP_FROM_NAME,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.from_email = from_email
        self.from_name = from_name

    @property
    def is_configured(self) -> bool:
        return bool(self.host)

    async def send_email(
        self,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
    ) -> None:
        message = EmailMessage()
        message["From"] = formataddr((self.from_name, self.from_email))
        message["To"] = to
        message["Subject"] = subject
        message.set_content(text or _html_to_text(html))
        message.add_alternative(html, subtype="html")

        if not self.is_configured:
            logger.info(
                "[EMAIL][dev] SMTP not configured; would send to=%s subject=%r\n%s",
                to, subject, text or html,
            )
            return

        await asyncio.to_thread(self._send_sync, message)

    def _send_sync(self, message: EmailMessage) -> None:
        try:
            if self.port == 465:
                with smtplib.SMTP_SSL(self.host, self.port, timeout=15) as smtp:
                    if self.username:
                        smtp.login(self.username, self.password)
                    smtp.send_message(message)
            else:
                with smtplib.SMTP(self.host, self.port, timeout=15) as smtp:
                    smtp.ehlo()
                    if self.use_tls:
                        smtp.starttls()
                        smtp.ehlo()
                    if self.username:
                        smtp.login(self.username, self.password)
                    smtp.send_message(message)
            logger.info("[EMAIL] sent to=%s subject=%r", message["To"], message["Subject"])
        except Exception as exc:
            logger.exception("[EMAIL] delivery failed: %s", exc)
            raise

    async def send_password_reset_email(
        self,
        to: str,
        name: str,
        otp: str,
        reset_url: str,
        expires_in_minutes: int,
    ) -> None:
        subject = "Reset your Fluiq password"
        text = (
            f"Hi {name},\n\n"
            f"We received a request to reset your Fluiq password.\n\n"
            f"Your one-time code is: {otp}\n"
            f"Or open this link: {reset_url}\n\n"
            f"This code expires in {expires_in_minutes} minutes. "
            f"If you didn't request a reset you can safely ignore this email.\n"
        )
        html = f"""
        <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:520px;margin:0 auto;padding:24px;color:#0f172a">
          <h2 style="margin:0 0 16px">Reset your Fluiq password</h2>
          <p>Hi {name},</p>
          <p>We received a request to reset your Fluiq password. Use the one-time code below or click the button to continue.</p>
          <div style="font-family:ui-monospace,Menlo,monospace;font-size:28px;letter-spacing:6px;font-weight:600;background:#f1f5f9;padding:16px;border-radius:8px;text-align:center;margin:24px 0">{otp}</div>
          <p style="text-align:center;margin:24px 0">
            <a href="{reset_url}" style="background:#0f172a;color:#ffffff;padding:12px 20px;border-radius:8px;text-decoration:none;display:inline-block">Reset password</a>
          </p>
          <p style="color:#64748b;font-size:13px">This code expires in {expires_in_minutes} minutes. If you didn't request this, you can safely ignore this email.</p>
        </div>
        """
        await self.send_email(to=to, subject=subject, html=html, text=text)


def _html_to_text(html: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", html).strip()


email_service = EmailService()


__all__ = ["EmailService", "email_service"]
