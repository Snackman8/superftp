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
import ftplib
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
    def __init__(self, *args, **kwargs):
        super(TestFTPFileDownloadManager, self).__init__(*args, **kwargs)
        self._ftp_thread = None
        self._blocks_downloaded = 0

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
            server.max_cons_per_ip = 10

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
        # tearDown in case we had a previous failed test
        self.tearDown()

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
        os.mkdir(os.path.join(self._test_dir, 'a'))
        filepath = os.path.join(os.path.join(self._test_dir, 'a'), 'testfile2.txt')
        with open(filepath, 'w') as f:
            for i in range(0, 2):
                f.write(str(i) + '.' * (1024 * 1024) + '\n')

        self._ftp_thread = self._start_ftp_server(self._test_dir, 2121)

        # give the ftp server some time to start up
        time.sleep(1)

    def tearDown(self):
        """ stop the ftp server """
        if self._ftp_thread:
            self._stop_ftp_server()
            while self._ftp_thread.is_alive():
                time.sleep(0.01)
        self._ftp_thread = None

    @unittest.skip("not implemented")
    def test_chunk_download(self):
        """ test the download of a single chunk in a file comprised of many blocks """
        pass

    def test_file_download(self):
        """ test the download of a small simple file using download_file """
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, True, True)
        ftp.download_file('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    def test_directory_download(self):
        """ test the download of a directory using download_file """
        # clean up the results directory
        dir_path = os.path.join(self._results_dir, 'dir_test')
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

        # download the directory
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, True, True)
        ftp.download('/', dir_path)

        # verify the sub directory was actually created
        self.assertTrue(os.path.exists(os.path.join(dir_path, 'a')))
        self.assertFalse(os.path.isfile(os.path.join(dir_path, 'a')))

        # verify the files were downloaded correctly
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(dir_path, 'testfile.txt'), shallow=False))
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'a/testfile2.txt'),
                                    os.path.join(dir_path, 'a/testfile2.txt'), shallow=False))

    def test_download(self):
        """ test the download of a small simple file using download"""
        if os.path.exists(os.path.join(self._results_dir, 'testfile.txt')):
            os.remove(os.path.join(self._results_dir, 'testfile.txt'))
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, False, True)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    def test_broken_tls(self):
        """ test correct response if server does not support tls """
        if os.path.exists(os.path.join(self._results_dir, 'testfile.txt')):
            os.remove(os.path.join(self._results_dir, 'testfile.txt'))
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, False, False)
        try:
            ftp.download('testfile.txt', self._results_dir)
        except ftplib.error_perm, e:
            self.assertEqual(str(e), '500 Command "AUTH" not understood.')

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

    def test_kill_speed(self):
        """ test that kill_speed does not crash """
        # abort the download after 2 blocks have been downloaded
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0.1, True, True)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    def test_resume_aborted_download(self):
        """ test the handling of resuming a previously aborted download """
        self._blocks_downloaded = 0

        def on_refresh_display(ftp_download_manager, blockmap, _remote_filepath):
            """ on refresh display handler """
            self._blocks_downloaded = self._blocks_downloaded + 1
            # make sure statistics do not crash
            blockmap.get_statistics()
            if self._blocks_downloaded > 2:
                ftp_download_manager.abort_download()

        # abort the download after 2 blocks have been downloaded
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, True, True)
        ftp.on_refresh_display = on_refresh_display
        ftp.download('testfile.txt', self._results_dir)
        self.assertFalse(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                     os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

        # resume the download
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, False, True)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    def test_resume_aborted_download2(self):
        """ test the handling of resuming a previously aborted download with a blocksize change"""
        self._blocks_downloaded = 0

        def on_refresh_display(ftp_download_manager, blockmap, _remote_filepath):
            """ on refresh display handler """
            self._blocks_downloaded = self._blocks_downloaded + 1
            # make sure statistics do not crash
            blockmap.get_statistics()
            if self._blocks_downloaded > 2:
                ftp_download_manager.abort_download()

        # abort the download after 2 blocks have been downloaded with a blocksize of 65536
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 65536, 0, True, True)
        ftp.on_refresh_display = on_refresh_display
        ftp.download('testfile.txt', self._results_dir)
        self.assertFalse(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                     os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

        # resume the download
        ftp = FtpFileDownloader('localhost', 'user', '12345', 4, 2121, 1, 2, 1048576, 0, False, True)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))
