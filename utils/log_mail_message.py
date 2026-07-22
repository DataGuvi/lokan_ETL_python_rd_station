import smtplib
from email.message import EmailMessage
import os
import urllib.parse
from dotenv import load_dotenv


def send_email(subject, body):
    # Retrieve credentials from environment variables for security
    load_dotenv()
    sender_email = os.getenv("EMAIL_SENDER")
    app_password = os.getenv("SENDER_PASSWORD")
    to_email = os.getenv("EMAIL_RECEIVER")
    # Parse the app password (using unquote to decode any URL-encoded characters)
    # app_password = urllib.parse.unquote(app_password)

    if not sender_email or not app_password:
        print("Error: Please set SENDER_EMAIL and SENDER_PASSWORD environment variables.")
        return

    # print(sender_email)
    # print(app_password)
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = f"Logs <{sender_email}>"
    msg['To'] = to_email
    # print('Sender name: {}'.format(msg['from'].addresses[0].display_name))

    try:
        # Using Email em Nuvem SMTP server.
        with smtplib.SMTP_SSL('smtp.emailemnuvem.com.br', 465) as smtp:
            smtp.login(sender_email, app_password)
            smtp.send_message(msg)
            print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")
