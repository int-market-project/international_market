# mail_sample.py
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sender_email = "rowan.dempsey.rd@gmail.com"
app_password = "mozx rjtn jzzs ptgg"   # Gmail App Password

def send_email(subject: str, html_message: str, receiver_email: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email

    # Plain-text fallback message
    text_part = MIMEText(
        "This email contains HTML content. Please use an HTML-compatible email client.",
        "plain"
    )

    # Actual HTML content
    html_part = MIMEText(html_message, "html")

    msg.attach(text_part)
    msg.attach(html_part)

    # Send email
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, app_password)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()

    print(f"Email sent to {receiver_email}!")

if __name__ == "__main__":
    test_html = """
    <h1>Hello from FastAPI Project</h1>
    <p>This is a <b>test email</b> sent using Python.</p>
    """

    send_email(
        subject="Test Email",
        html_message=test_html,
        receiver_email="rayyan.tanveer1020@gmail.com"
    )
