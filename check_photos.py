#!/usr/bin/env python3
'''
Check that photos, etc. have been uploaded successfully
'''
from __future__ import print_function
import os
import argparse
import httplib2

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = ('https://www.googleapis.com/auth/drive.metadata.readonly '
          'https://www.googleapis.com/auth/drive.photos.readonly')
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

def get_credentials(flags):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


class CheckPhotos():
    '''
    Check that media files (photos for now) were uploaded to google drive/photos,
    verifying with the md5Checksum.
    '''

    def __init__(self, files):
        self._files = files

    def check(self, fname, checksum):
        ''' check one file or tree '''
        query = "name contains 'PIC00028.jpg'"
        fields = "nextPageToken, files(id, name, md5Checksum, parents)"
        entries = self._files.list(corpus='user',
                                   spaces='photos,drive',
                                   q=query,
                                   fields=fields).execute()
        files = entries.get('files', [])
        for f in files:
            print(f, checksum, fname)

def main():
    """
    Check that pictures have been successfully uploaded to Google Photos or Drive.

    Ensures that the file has been uploaded to Google photos or drive by comparing the md5sum to
    that of all the files in drive and photos.
    """
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('dir_or_file', nargs='+', help='directory or file to check')
    flags = parser.parse_args()

    credentials = get_credentials(flags)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    checker = CheckPhotos(service.files())

    checker.check("PIC00028.jpg", "783763de6b07bb8d210a078d77016b7e")

if __name__ == '__main__':
    main()
