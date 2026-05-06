import asyncio
import logging
import requests
from email.utils import formataddr
from typing import Optional
import re

import config

logger = logging.getLogger(__name__)


class EmailService:
    def __init__(
        self,
        api_key: str = config.RESEND_API_KEY,
        from_email: str = config.SMTP_FROM_EMAIL,
        from_name: str = config.SMTP_FROM_NAME,
    ) -> None:
        self.api_key = api_key
        self.from_email = from_email
        self.from_name = from_name

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def send_email(
        self,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
    ) -> None:
        if not self.is_configured:
            logger.info(
                "[EMAIL][dev] Resend not configured; would send to=%s subject=%r\n%s",
                to, subject, text or html,
            )
            return

        await asyncio.to_thread(self._send_sync, to, subject, html, text)

    def _send_sync(self, to: str, subject: str, html: str, text: Optional[str]) -> None:
        try:
            response = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "from": formataddr((self.from_name, self.from_email)),
                    "to": [to],
                    "subject": subject,
                    "html": html,
                    "text": text or _html_to_text(html),
                },
                timeout=15,
            )
            response.raise_for_status()
            logger.info("[EMAIL] sent to=%s subject=%r", to, subject)
        except requests.HTTPError as exc:
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
    return re.sub(r"<[^>]+>", "", html).strip()


email_service = EmailService()

__all__ = ["EmailService", "email_service"]