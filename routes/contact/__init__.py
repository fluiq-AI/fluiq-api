import html as html_lib
import logging

from fastapi import APIRouter
from pydantic import BaseModel

from shared.email import email_service

logger = logging.getLogger(__name__)
router = APIRouter()

CONTACT_TO = "fluiqai@gmail.com"


class ContactPayload(BaseModel):
    name: str
    email: str
    subject: str
    message: str


@router.post("/contact")
async def contact(payload: ContactPayload):
    name    = html_lib.escape(payload.name.strip())
    email   = html_lib.escape(payload.email.strip())
    subject = html_lib.escape(payload.subject.strip())
    message = html_lib.escape(payload.message.strip())

    html = f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:600px;margin:0 auto;padding:32px 24px;color:#0f172a">
      <div style="margin-bottom:24px">
        <span style="display:inline-block;background:#0f172a;color:#fff;font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;padding:4px 10px;border-radius:4px">Fluiq</span>
      </div>
      <h2 style="margin:0 0 6px;font-size:22px;font-weight:700">New contact form submission</h2>
      <p style="margin:0 0 24px;color:#64748b;font-size:14px">Someone reached out via the Fluiq website.</p>

      <table style="width:100%;border-collapse:collapse;margin-bottom:24px">
        <tr>
          <td style="padding:10px 0;color:#64748b;font-size:13px;font-weight:600;width:90px;vertical-align:top;border-bottom:1px solid #f1f5f9">Name</td>
          <td style="padding:10px 0;font-size:14px;font-weight:600;border-bottom:1px solid #f1f5f9">{name}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;color:#64748b;font-size:13px;font-weight:600;vertical-align:top;border-bottom:1px solid #f1f5f9">Email</td>
          <td style="padding:10px 0;font-size:14px;border-bottom:1px solid #f1f5f9">
            <a href="mailto:{email}" style="color:#0f172a">{email}</a>
          </td>
        </tr>
        <tr>
          <td style="padding:10px 0;color:#64748b;font-size:13px;font-weight:600;vertical-align:top">Subject</td>
          <td style="padding:10px 0;font-size:14px">{subject}</td>
        </tr>
      </table>

      <div style="background:#f8fafc;border-radius:10px;border-left:3px solid #0f172a;padding:18px 20px">
        <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#94a3b8">Message</p>
        <p style="margin:0;font-size:14px;line-height:1.7;white-space:pre-wrap">{message}</p>
      </div>

      <p style="margin:32px 0 0;font-size:12px;color:#94a3b8">
        Sent via getfluiq.com contact form · Reply directly to <a href="mailto:{email}" style="color:#94a3b8">{email}</a>
      </p>
    </div>
    """

    text = (
        f"Name: {payload.name}\n"
        f"Email: {payload.email}\n"
        f"Subject: {payload.subject}\n\n"
        f"{payload.message}"
    )

    await email_service.send_email(
        to=CONTACT_TO,
        subject=f"[Fluiq Contact] {payload.subject}",
        html=html,
        text=text,
    )
    logger.info("[CONTACT] message from=%s subject=%r", payload.email, payload.subject)
    return {"ok": True}
