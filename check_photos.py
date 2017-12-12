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
DRIVE_CACHE_DB = 'cached_drive_photos.pickle'
NOT_UPLOADED_FILE = 'not_uploaded.txt'

MEDIA_EXTENSIONS = {'.jpg': True,
                    '.mpg': True,
                    '.wav': True,
                    '.tif': True,
                    '.m4a': True,
                    '.bmp': True,
                    '.mp4': True,
                    '.mp3': True,
                    '.aac': True,
                    '.jpeg': True,
                    '.dsc': True,
                    '.gif': True,
                    '.m4v': True,
                    '.png': True,
                    '.avi': True,
                    '.wmv': True,
                    '.mov': True,
                    '.mts': True}

LOG = logging.getLogger('check_photos')
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler())

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = ('https://www.googleapis.com/auth/drive.readonly '
          'https://www.googleapis.com/auth/drive.photos.readonly '
          'https://picasaweb.google.com/data/')


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
        print("read files from cache,", len(self._drive_files), "files")

    def _load_drive(self):
        ''' Load the drive file list and checksums to local cache '''
        corpora = 'user'
        # no query (q in files.list)
        # 2017-12-12: spaces = 'photos' doesn't retrieve all photos, and
        # will be "sunsetted" soon.  The photos it doesn't retrieve appear to
        # be those uploaded via Google's Backup and Sync app.
        spaces = 'drive'
        fields = "nextPageToken, files({})".format(FILE_FIELDS)
        page_size = 1000
        page_token = None
        drive_files = {}
        getter = operator.itemgetter(*DriveFile._fields)
        count = 0
        while True:
            LOG.info("Reading files from Google")
            entries = self._files.list(corpora=corpora,
                                       pageToken=page_token,
                                       pageSize=page_size,
                                       spaces=spaces,
                                       fields=fields).execute()
            page_token = entries.get('nextPageToken', None)
            files = entries.get('files', [])
            LOG.info("Read %s files from drive", len(files))
            for f in files:
                count += 1
                try:
                    drive_file = DriveFile(*getter(f))
                except KeyError:
                    # no checksum, which means it's a folder or something.
                    continue
                drive_files[drive_file.md5Checksum] = drive_file
                count += 1
            if not page_token:
                break
        self._drive_files = drive_files
        print("Finished reading from google, read", count, "files")

    def check(self, path, not_uploaded_path):
        ''' check one file or tree '''
        print("Checking.  Files not uploaded will be printed and written to file.")
        with open(not_uploaded_path, 'w') as f:
            self._not_uploaded_file = f
            self._check(path)
        print("Not uploaded:", self._not_uploaded, "listed in", not_uploaded_path)
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
                ext = os.path.splitext(f)[1].lower()
                if not MEDIA_EXTENSIONS.get(ext, False):
                    continue
                fpath = os.path.join(root, f)
                if os.path.islink(fpath):
                    continue
                self._check_file(fpath)

    def _check_file(self, path):
        drive_file = self._drive_files.get(self._md5(path), None)
        if drive_file:
            self._already_uploaded += 1
        else:
            print(path)
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

    def dump_extensions(self, db_path):
        '''
        Dump a list of extensions found in the cache file to assist
        in creating the list of media extensions.
        '''
        self._drive_files = {}
        self._load_file(db_path)
        extensions = {}
        for f in iter(self._drive_files.values()):
            ext = os.path.splitext(f.name)[1].lower()
            if not ext:
                continue
            extensions[ext] = True
        print("Extensions in files in local cache:")
        print(extensions.keys())

def main():
    """
    Check that pictures have been successfully uploaded to Google Photos or Drive.

    Ensures that the file has been uploaded to Google photos or drive by
    comparing the md5sum to that of all the files in drive and photos.
    """
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('path', nargs='+', help='directory or file to check')
    parser.add_argument('--dump_extensions',
                        action='store_true',
                        help='print extensions in cached files then exit.')
    flags = parser.parse_args()

    if flags.dump_extensions:
        checker = CheckPhotos(None)
        checker.dump_extensions(DRIVE_CACHE_DB)

    credentials = get_credentials(flags)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    checker = CheckPhotos(service.files())

    checker.load(DRIVE_CACHE_DB)
    for path in flags.path:
        checker.check(path, NOT_UPLOADED_FILE)

if __name__ == '__main__':
    main()
