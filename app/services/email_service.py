from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage

from app.config import load_environment


load_environment()


def send_user_invite_email(
    recipient_email: str,
    username: str,
    role_label: str,
    login_url: str,
) -> tuple[bool, str]:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_from = os.getenv("SMTP_FROM_EMAIL", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    use_tls = os.getenv("SMTP_USE_TLS", "true").strip().lower() != "false"

    if not smtp_host or not smtp_from:
        return False, "SMTP is not configured. User was created but invite email was not sent."

    message = EmailMessage()
    message["Subject"] = "Database Migrator Access Granted"
    message["From"] = smtp_from
    message["To"] = recipient_email
    message.set_content(
        "\n".join(
            [
                f"Hello {username},",
                "",
                f"You have been granted {role_label} access in Database Migrator.",
                "",
                "Use the login link below to access the application:",
                login_url,
                "",
                "After signing in with your assigned username and password, you will be redirected to the home page.",
                "",
                "Regards,",
                "Database Migrator",
            ]
        )
    )

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
            if use_tls:
                smtp.starttls()
            if smtp_username and smtp_password:
                smtp.login(smtp_username, smtp_password)
            smtp.send_message(message)
    except Exception as exc:
        return False, f"User was created but invite email failed: {exc}"

    return True, "User created and invite email sent successfully."
