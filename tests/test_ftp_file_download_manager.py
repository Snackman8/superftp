""" simple ftp tests """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import filecmp
import os
import Queue
import shutil
import time
from threading import Thread
import unittest

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import MultiprocessFTPServer

from ftp_file_download_manager import FtpFileDownloader


# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestFTPFileDownloadManager(unittest.TestCase):
    """ unit tests for ftp_file_download_manager """
    def _start_ftp_server(self, ftp_root_dir, port=2121):
        """ start the test ftp server

            Args:
                ftp_root_dir - root directory for the ftp server
                port - port number for the ftp server

            Returns:
                thread running the ftp server
        """
        def tw_ftp_server():
            """ thread worker for the ftp server """
            authorizer = DummyAuthorizer()
            authorizer.add_user('user', '12345', ftp_root_dir, perm='elradfmwMT')

            # Instantiate FTP handler class
            handler = FTPHandler
            handler.authorizer = authorizer
            server = MultiprocessFTPServer(('', port), handler)
            server.max_cons = 256
            server.max_cons_per_ip = 5

            # start ftp server
            while self._com_queue.empty():
                server.serve_forever(timeout=0.1, blocking=False)
            self._com_queue.get()
            server.close_all()
            time.sleep(1)

        # launch a thread with the ftp server
        t = Thread(target=tw_ftp_server, args=())
        t.start()
        return t

    def _stop_ftp_server(self):
        """ stop the test ftp server """
        self._com_queue.put('STOP')

    def setUp(self):
        """ start the test ftp server """
        # generate the test data
        self._com_queue = Queue.Queue()
        self._results_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                         'results_ftp_file_download_manager')
        if not os.path.exists(self._results_dir):
            os.mkdir(self._results_dir)
        self._test_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_data')
        shutil.rmtree(self._test_dir)
        os.mkdir(self._test_dir)
        filepath = os.path.join(self._test_dir, 'testfile.txt')
        with open(filepath, 'w') as f:
            for i in range(0, 20):
                f.write(str(i) + '.' * (1024 * 1024) + '\n')
        self._start_ftp_server(self._test_dir, 2121)

    def tearDown(self):
        """ stop the ftp server """
        self._stop_ftp_server()

    @unittest.skip("not implemented")
    def test_chunk_download(self):
        """ test the download of a single chunk in a file comprised of many blocks """
        pass

    def test_file_download(self):
        """ test the download of a small simple file """
        if os.path.exists(os.path.join(self._results_dir, 'testfile.txt')):
            os.remove(os.path.join(self._results_dir, 'testfile.txt'))
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0)
        ftp.download_file('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    @unittest.skip("not implemented")
    def test_bad_server_address(self):
        """ test the handling of a bad server url """
        pass

    @unittest.skip("not implemented")
    def test_bad_credentials(self):
        """ test the handling of a bad server username password """
        pass

    @unittest.skip("not implemented")
    def test_bad_remote_path(self):
        """ test the handling of a bad remote path """
        pass

    @unittest.skip("not implemented")
    def test_bad_local_path(self):
        """ test the handling of a bad local path """
        pass

    @unittest.skip("not implemented")
    def test_local_path_is_directory(self):
        """ test the handling of local_path is a directory """
        pass

    @unittest.skip("not implemented")
    def test_resume_aborted_download(self):
        """ test the handling of resuming a previously aborted download """
        pass

    @unittest.skip("not implemented")
    def test_mirror_single_file(self):
        """ test the mirroring of a single file """
        pass

    @unittest.skip("not implemented")
    def test_mirror_directory(self):
        """ test the mirroring of an entire directory """
        pass
