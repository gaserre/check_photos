#!/usr/bin/env python3
'''
Check that photos, etc. have been uploaded successfully
'''
from __future__ import print_function
import os
import argparse
import collections
import hashlib
import operator
import logging
import pickle

import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

MB = 1024 * 1024

LOG = logging.getLogger('check_photos')
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler())

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = ('https://www.googleapis.com/auth/drive.metadata.readonly '
          'https://www.googleapis.com/auth/drive.photos.readonly')
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

FILE_FIELDS = 'id, name, md5Checksum'
DriveFile = collections.namedtuple('DriveFile', FILE_FIELDS)

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
        LOG.info('Storing credentials to ' + credential_path)
    return credentials

class CheckPhotos(object):
    '''
    Check that media files (photos for now) were uploaded to google drive/photos,
    verifying with the md5Checksum.
    '''

    def __init__(self, files):
        self._files = files
        self._drive_files = None
        self._already_uploaded = 0
        self._not_uploaded = 0
        self._not_uploaded_file = None

    def load(self, db_path):
        '''
        Load the hash of photos on drive from a previous run if db_file exists
        or from google if not.
        '''
        self._load_file(db_path)
        if self._drive_files != None:
            LOG.info("Using cached info from %s", db_path)
            return
        self._load_drive()
        self._save(db_path)

    def _save(self, db_path):
        with open(db_path, 'wb') as f:
            pickle.dump(self._drive_files, f)

    def _load_file(self, db_path):
        try:
            with open(db_path, 'rb') as f:
                drive_files = pickle.load(f)
        except FileNotFoundError:
            LOG.info("db_path %s not found", db_path)
            return
        self._drive_files = drive_files

    def _load_drive(self):
        ''' Load the drive file list and checksums to local cache '''
        corpora = 'user'
        # query = "name contains 'PIC00028.jpg'"
        query = None
        # spaces = 'photos, drive'
        spaces = 'photos'
        fields = "nextPageToken, files({})".format(FILE_FIELDS)
        page_token = None
        drive_files = {}
        getter = operator.itemgetter(*DriveFile._fields)
        while True:
            LOG.info("Reading files from Google")
            entries = self._files.list(corpora=corpora,
                                       spaces=spaces,
                                       q=query,
                                       fields=fields).execute()
            page_token = entries.get('nextPageToken', None)
            files = entries.get('files', [])
            LOG.info("Read %s files from drive", len(files))
            for f in files:
                drive_file = DriveFile(*getter(f))
                drive_files[drive_file.md5Checksum] = drive_file
            if not page_token:
                break
        self._drive_files = drive_files

    def check(self, path, not_uploaded_path):
        ''' check one file or tree '''
        with open(not_uploaded_path, 'w') as f:
            self._not_uploaded_file = f
            self._check(path)
        print("Not uploaded:", self._not_uploaded, "in", not_uploaded_path)
        print("Already uploaded:", self._already_uploaded)

    def _check(self, path):
        if os.path.islink(path):
            LOG.info("Not checking link %s", path)
            return
        if os.path.isfile(path):
            self._check_file(path)
            return

        for root, _dirs, files in os.walk(path):
            for f in files:
                if not f.endswith(".jpg"):
                    continue
                self._check_file(os.path.join(root, f))

    def _check_file(self, path):
        drive_file = self._drive_files.get(self._md5(path), None)
        if drive_file:
            self._already_uploaded += 1
        else:
            print(path, "not uploaded")
            self._not_uploaded += 1
            print(path, file=self._not_uploaded_file)

    @staticmethod
    def _md5(path):
        md5 = hashlib.md5()
        with open(path, "rb") as f:
            while True:
                buf = f.read(MB)
                if not buf:
                    break
                md5.update(buf)
        return md5.hexdigest()

def main():
    """
    Check that pictures have been successfully uploaded to Google Photos or Drive.

    Ensures that the file has been uploaded to Google photos or drive by
    comparing the md5sum to that of all the files in drive and photos.
    """
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('path', nargs='+', help='directory or file to check')
    flags = parser.parse_args()

    credentials = get_credentials(flags)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    checker = CheckPhotos(service.files())

    checker.load('cached_drive_photos.pickle')
    for path in flags.path:
        checker.check(path, "not_uploaded.txt")

if __name__ == '__main__':
    main()
