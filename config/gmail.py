


from email.mime.text import MIMEText

import os 

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request



## PULL api keys
dir_keys = os.environ.get('AUTH_DIR') 
api_name= 'fuel-automation-v1-credentials.json'


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


flow = InstalledAppFlow.from_client_secrets_file(
  '{a}/{b}'.format(a=dir_keys,b=api_name), SCOPES)


flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)


creds = flow.run_local_server(port=0)


def send_email(sender,to,subject, message_text):
    message = MIMEText("hi")
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_string())}


def send_message(service, user_id, message):
  """Send an email message.

  Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

  Returns:
    Sent Message.
  """
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print 'Message Id: %s' % message['id']
    return message
  except errors.HttpError, error:
    print 'An error occurred: %s' % error