""" simple ftp tests """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import os
from threading import Thread
import unittest
from twisted.protocols.ftp import FTPFactory, FTPRealm
from twisted.cred.portal import Portal
from twisted.cred.checkers import AllowAnonymousAccess
from twisted.internet import reactor
from ftp_file_download_manager import FtpFileDownloader

# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestFTPFileDownloadManager(unittest.TestCase):
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
            p = Portal(FTPRealm(ftp_root_dir), [AllowAnonymousAccess()])
            f = FTPFactory(p)
            reactor.listenTCP(port, f)
            reactor.run(installSignalHandlers=0)

        # launch a thread with the ftp server
        t = Thread(target=tw_ftp_server, args=())
        t.start()
        return t

    def _stop_ftp_server(self):
        """ stop the test ftp server """
        reactor.callFromThread(reactor.stop)

    def setUp(self):
        """ start the test ftp server """
        # generate the test data
        test_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_data')
        if not os.path.exists(test_dir):
            os.mkdir(test_dir)
        filepath = os.path.join(test_dir, 'testfile.txt')
        if not os.path.exists(filepath):
            if os.path.exists(filepath):
                os.remove(filepath)
            with open(filepath, 'w') as f:
                for i in range(0, 512):
                    f.write(str(i) + ' ' * (1024 * 1024))

        self._start_ftp_server(test_dir, 2121)

    def tearDown(self):
        """ stop the ftp server """
        self._stop_ftp_server()

    @unittest.skip("not implemented")
    def test_chunk_download(self):
        """ test the download of a single chunk in a file comprised of many blocks """
        pass

    def test_small_file_download(self):
        """ test the download of a small simple file """
        ftp = FtpFileDownloader('localhost', 'anonymous', 'password', 1, 2121, 1, 2)

        pass

    @unittest.skip("not implemented")
    def test_large_file_download(self):
        """ test the download of a single chunk in a file comprised of many blocks """
        pass

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
