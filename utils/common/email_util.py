import os
import smtplib
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from botocore.exceptions import ClientError


def send_email(ses_client, sender_email, recipient_email, subject, body_text, charset="UTF-8"):
    """
    Send an email with the log content in the body using AWS SES.

    Args:
        ses_client (boto3.client): Boto3 SES client object.
        sender_email (str): The email address to send the email from (must be verified in SES).
        recipient_email (str): The email address to send the email to.
        subject (str): The subject line of the email.
        body_text (str): The plain text body of the email.
        charset (str, optional): The character set for the email. Defaults to 'UTF-8'.

    Returns:
        dict: The response from the SES send_email API.

    Raises:
        ClientError: If there is an issue with the SES request.
    """
    try:
        response = ses_client.send_email(
            Source=sender_email,
            Destination={'ToAddresses': [recipient_email]},
            Message={
                'Subject': {'Data': subject, 'Charset': charset},
                'Body': {'Text': {'Data': body_text, 'Charset': charset}}
            }
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
        return response
    except ClientError as e:
        print(f"Failed to send email: {e.response['Error']['Message']}")
        raise e


def send_email_with_attachment(
        ses_client, sender_email, recipient_email, subject, body_text, attachment_path, charset="UTF-8"):
    """
    Send an email with an attachment using AWS SES.

    Args:
        ses_client (boto3.client): Boto3 SES client object.
        sender_email (str): The email address to send the email from (must be verified in SES).
        recipient_email (str): The email address to send the email to.
        subject (str): The subject line of the email.
        body_text (str): The plain text body of the email.
        attachment_path (str): The file path of the attachment to be sent.
        charset (str, optional): The character set for the email. Defaults to 'UTF-8'.

    Returns:
        dict: The response from the SES send_raw_email API.

    Raises:
        ClientError: If there is an issue with the SES request.
    """
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = recipient_email

        msg.attach(MIMEText(body_text, "plain", charset))

        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
            msg.attach(part)

        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={"Data": msg.as_string()}
        )
        print(f"Email with attachment sent! Message ID: {response['MessageId']}")
        return response
    except ClientError as e:
        print(f"Failed to send email with attachment: {e.response['Error']['Message']}")
        raise e


def create_email_body_from_log(log_file_path):
    """
    Create the body of the email by reading the log file.

    Args:
        log_file_path (str): The path to the log file.

    Returns:
        str: The content of the log file as the body of the email.
    """
    try:
        with open(log_file_path, 'r') as log_file:
            return log_file.read()
    except FileNotFoundError:
        print(f"Log file not found: {log_file_path}")
        return "Log file not found."


def prepare_log_email(logs_dir, log_file_name="tests.log"):
    """
    Prepare the log email by finding the latest log file and composing the email body.

    Args:
        logs_dir (str): Directory where log files are stored.
        log_file_name (str, optional): Name of the log file. Defaults to "tests.log".

    Returns:
        tuple: The log file path and email body.
    """
    log_file = os.path.join(logs_dir, log_file_name)
    email_body = "Please find the attached log file for the test session."
    return log_file, email_body


def send_email_via_smtp(from_addr, to_addr, subject, body, smtp_server, smtp_port, attachment_file_path):
    """
    Send an email with or without an attachment using an SMTP server.

    Args:
        from_addr (str): The sender's email address.
        to_addr (str | list): The recipient's email address or a list of addresses.
        subject (str): The subject line of the email.
        body (str): The plain text body of the email.
        smtp_server (str): The SMTP server address.
        smtp_port (int): The SMTP server port.
        attachment_file_path (str, optional): Path to the attachment file. Defaults to None.

    Raises:
        Exception: If sending the email fails.
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = ", ".join(to_addr) if isinstance(to_addr, list) else to_addr
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        if attachment_file_path:
            with open(attachment_file_path, 'rb') as attachment:
                part = MIMEApplication(attachment.read(), Name=attachment_file_path)
                part['Content-Disposition'] = f'attachment; filename="{attachment_file_path}"'
                msg.attach(part)

        to_addrs = to_addr if isinstance(to_addr, list) else [to_addr]

        with smtplib.SMTP(smtp_server, int(smtp_port)) as server:
            server.sendmail(from_addr, to_addrs, msg.as_string())

        print(f"Email sent successfully to {', '.join(to_addrs)}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
