import requests
from .requests_kerberos import HTTPKerberosAuth, OPTIONAL
import os
from ftplib import FTP

BASE_DIR = os.path.dirname(__file__)


class WebHDFSClient(object):
    """
    reference:
    https://hadoop.apache.org/docs/r1.2.1/webhdfs.html
    https://github.com/requests/requests-kerberos
    http://docs.python-requests.org
    """

    def __init__(self, http_url, auth=None):
        self.http_url = http_url
        if auth == 'use_kerberos':
            self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        else:
            self.auth = None

    def get_file(self, src, dst):
        fullpath = self.http_url + src + '?op=OPEN'
        resp = requests.get(fullpath, auth=self.auth)

        with open(dst, 'wb') as fh:
                for chunk in resp.iter_content(chunk_size=128):
                    fh.write(chunk)

    def put_file(self, src, dst):
        status = {'create': None, 'append': None}

        # create
        fullpath = self.http_url + dst + '?op=CREATE&noredirect=false'
        headers = {'content-type': 'application/octet-stream'}
        resp = requests.put(fullpath, auth=self.auth, headers=headers)
        status['create'] = resp

        # append
        fullpath = self.http_url + dst + '?op=CREATE'
        headers = {'Location': fullpath, 'content-type': 'application/octet-stream'}
        with open(src, 'rb') as f:
            data = f.read()
            resp = requests.put(fullpath, auth=self.auth, headers=headers, data=data)
            print(resp.headers)
        status['append'] = resp

        return status

    def hdfs2ftp(self, src, host, dst_filename, dst_dir=None, username=None, password=None, overwrite=True):
        self.get_file(src, os.path.join(BASE_DIR, 'tmp'))
        fp = open(os.path.join(BASE_DIR, 'tmp'), 'rb')
        return self.upload2ftp(fp, host, dst_filename, dst_dir=dst_dir, username=username, password=password, overwrite=overwrite)

    def upload2ftp(self, fp, host, dst_filename, dst_dir=None, username=None, password=None, overwrite=True):
        """Given a file in unix file system, uploads to an ftp server
           Returns status codes 0, 1 and 2 if function runs with no error
           0 -> overwrite was set to true, file was overwritten
           1 -> overwrite was set to true, file was written
           2 -> overwrite was set to false, file was written
           3 -> overwrite was set to false, nothing was done
        """

        ftp = FTP(host)
        if username is None and password is None:
            ftp.login()
        else:
            ftp.login(user=username, passwd=password, acct='')

        if dst_dir is not None:
            ftp.cwd(dst_dir)
        
        # check if the file exist
        listings = ftp.nlst()
        file_exist = dst_filename in listings

        if overwrite:
            ftp.storbinary('STOR {}'.format(dst_filename), fp)
            if file_exist:
                return 0
            else:
                return 1
        else:
            if not file_exist:
                ftp.storbinary('STOR {}'.format(dst_filename), fp)
                return 2
            else:
                # do nothing
                return 3

        ftp.storbinary('STOR {}'.format(dst_filename), fp)
        fp.close()
        ftp.close()

    def list_dir(self, dirpath):
        """
        {
            'FileStatuses': {
                'FileStatus': [{
                    'pathSuffix': '.Trash',
                    'type': 'DIRECTORY',
                    'length': 0,
                    'owner': 'auser',
                    'group': 'auser',
                    'permission': '700',
                    'accessTime': 0,
                    'modificationTime': 1552975200566,
                    'blockSize': 0,
                    'replication': 0,
                    'aclBit': True
                }, {
                    'pathSuffix': '.sparkStaging',
                    'type': 'DIRECTORY',
                    'length': 0,
                    'owner': 'auser',
                    'group': 'auser',
                    'permission': '775',
                    'accessTime': 0,
                    'modificationTime': 1552672125855,
                    'blockSize': 0,
                    'replication': 0,
                    'aclBit': True
                }, {
                    'pathSuffix': '.staging',
                    'type': 'DIRECTORY',
                    'length': 0,
                    'owner': 'auser',
                    'group': 'auser',
                    'permission': '700',
                    'accessTime': 0,
                    'modificationTime': 1553001374755,
                    'blockSize': 0,
                    'replication': 0,
                    'aclBit': True
                }, {
                    'pathSuffix': 'hbase-staging',
                    'type': 'DIRECTORY',
                    'length': 0,
                    'owner': 'auser',
                    'group': 'auser',
                    'permission': '775',
                    'accessTime': 0,
                    'modificationTime': 1552968658323,
                    'blockSize': 0,
                    'replication': 0,
                    'aclBit': True
                }, {
                    'pathSuffix': 'mapper.py',
                    'type': 'FILE',
                    'length': 1888,
                    'owner': 'auser',
                    'group': 'auser',
                    'permission': '664',
                    'accessTime': 1547655033988,
                    'modificationTime': 1499285691852,
                    'blockSize': 134217728,
                    'replication': 3,
                    'aclBit': True
                }]
            }
        }
        """
        fullpath = self.http_url + dirpath + '?op=LISTSTATUS'
        resp = requests.get(fullpath, auth=self.auth)
        return resp.json()

    def most_recent_file(self, dirpath):
        liststatus = self.list_dir(dirpath)
        result = {'filename': None, 'modification_time': -1}
        for f in liststatus['FileStatuses']['FileStatus']:
            if f['type'] == 'FILE' and f['modificationTime'] > result['modification_time']:
                result['filename'] = f['pathSuffix']
                result['modification_time'] = f['modificationTime']

        if result['filename'] is not None:
            return result

        return None
