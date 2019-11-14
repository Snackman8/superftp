""" simple ftp tests """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import filecmp
import os
import shutil
import ftplib
import unittest

from superftp.ftp_file_download_manager import FtpFileDownloader
from test_utils import setup_ftp_server, teardown_ftp_server


# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestFTPFileDownloadManager(unittest.TestCase):
    """ unit tests for ftp_file_download_manager """
    def __init__(self, *args, **kwargs):
        super(TestFTPFileDownloadManager, self).__init__(*args, **kwargs)
        self._ftp_thread = None
        self._blocks_downloaded = 0
        self._com_queue = None

    def setUp(self):
        """ start the test ftp server """
        # start the ftp server
        (self._com_queue, self._results_dir,
         self._test_dir, self._ftp_thread) = setup_ftp_server(self._ftp_thread, self._com_queue,
                                                              'results_ftp_file_download_manager')

    def tearDown(self):
        """ stop the ftp server """
        teardown_ftp_server(self._ftp_thread, self._com_queue)
        self._ftp_thread = None

    @unittest.skip("not implemented")
    def test_chunk_download(self):
        """ test the download of a single chunk in a file comprised of many blocks """
        pass

    def test_file_download(self):
        """ test the download of a small simple file using download_file """
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)
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
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)
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
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=False)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

    def test_broken_tls(self):
        """ test correct response if server does not support tls """
        if os.path.exists(os.path.join(self._results_dir, 'testfile.txt')):
            os.remove(os.path.join(self._results_dir, 'testfile.txt'))
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=False, enable_tls=True)
        try:
            ftp.download('testfile.txt', self._results_dir)
        except ftplib.error_perm as e:
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
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)
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
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)
        ftp.on_refresh_display = on_refresh_display
        ftp.download('testfile.txt', self._results_dir)
        self.assertFalse(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                     os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

        # resume the download
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=False)
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
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=65536, kill_speed=0, clean=True)
        ftp.on_refresh_display = on_refresh_display
        ftp.download('testfile.txt', self._results_dir)
        self.assertFalse(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                     os.path.join(self._results_dir, 'testfile.txt'), shallow=False))

        # resume the download
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=False)
        ftp.download('testfile.txt', self._results_dir)
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))
