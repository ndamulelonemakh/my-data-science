"""Send emails programmatically"""

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# Replace with your actual details
sender_email = "accounts@mydomain.com"
receiver_email = "mycustomer@gmail.com"
password = "<sender-password>"  # Your app password or regular password if 2FA off
smtp_server = "<mail server e.g. smtp.gmail.com>"
port = 587  # For starttls

message = MIMEMultipart("alternative")
message["Subject"] = f"Test Email from Python - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
message["From"] = sender_email
message["To"] = receiver_email

# Create the plain-text and HTML version of your message
text = """\
Hi,

This is a test email sent from Python."""
html = """\
<html>
  <body>
    <p>Hi,<br><br>This is a test email sent from Python.</p>
  </body>
</html>
"""

# Turn these into plain/html MIMEText objects
part1 = MIMEText(text, "plain")
part2 = MIMEText(html, "html")

# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
message.attach(part1)
message.attach(part2)

# Create secure connection with server and send email
context = ssl.create_default_context()
try:
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)  # Secure the connection
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )
    print("Email sent successfully!")

except Exception as e:
    print(f"An error occurred: {e}")
