""" tests for the superftp class """
# DISABLE - Access to a protected member %s of a client class
# pylint: disable=W0212

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import filecmp
import os
import unittest

from superftp import superftp
from superftp.blockmap import Blockmap
from superftp.ftp_file_download_manager import FtpFileDownloader

from test_utils import captured_output, create_blockmap, create_results_dir, setup_ftp_server, teardown_ftp_server


# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestSuperFTP(unittest.TestCase):
    """ tests for superftp class """

    def setUp(self):
        self._results_dir = create_results_dir('results_superftp')

    def test_display_compact(self):
        """ test _display_compact function """
        # create a FtpFileDownloader for the test
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)

        # create a blockmap for the test
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 32)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 15, 2, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 6, 2, Blockmap.PENDING)
        blockmap.change_block_range_status(1024 * 1024 * 1, 2, Blockmap.SAVING)

        # check the display_compact
        with captured_output() as (out, err):
            superftp._display_compact(ftp, blockmap, '\remote\test')
        s = err.getvalue()
        self.assertEqual(s, '')
        s = out.getvalue()
        truth = ('\rETA:infinite        3.2%  0.000MB/sec  \remote\test                              ')
        self.assertEqual(s, truth)

    def test_display_full(self):
        """ test _display_compact function """
        # create a FtpFileDownloader for the test
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)

        # create a blockmap for the test
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 32)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 2, 2, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 24, 2, Blockmap.PENDING)
        blockmap.change_block_range_status(1024 * 1024 * 30, 2, Blockmap.SAVING)

        # check the display_compact
        with captured_output() as (out, err):
            superftp._display_full(ftp, blockmap, '\remote\test', (24, 80))
        s = err.getvalue()
        self.assertEqual(s, '')
        s = out.getvalue()
        truth = ('\x1b[1;0H\x1b[37mETA:infinite        3.2%  0.000MB/sec  \remote\test\x1b[K\x1b[2;0H\x1b[K\x1b[3;0H' +
                 '\x1b[37m[0.000][0.000][0.000][0.000]\x1b[K\n[0.000][0.000][0.000][0.000]\x1b[K\n[0.000][0.000]' +
                 '[0.000][0.000]\x1b[K\n[0.000][0.000][0.000][0.000]\x1b[K\n\x1b[7;0H\x1b[K\x1b[8;0H\x1b[37m..\x1b' +
                 '[92m**\x1b[37m....................\x1b[93m001234__789ABCDEF23456789ABCDEF\x1b[37m.......\x1b[K\r\n' +
                 '\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r' +
                 '\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[K\r\n\x1b[J')
        self.assertEqual(s, truth)

    def test_main(self):
        """ smoke test of main to make sure it does not crash printing out help """
        # check the display_compact
        with captured_output() as (out, err):
            with self.assertRaises(SystemExit):
                superftp.main()
        s = out.getvalue()
        self.assertEqual(s, '')
        s = err.getvalue()
        self.assertTrue(len(s) > 0)

    def test_pretty_blockmap(self):
        """ test pretty blockmap function """
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 32)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 5, 2, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 9, 2, Blockmap.PENDING)
        blockmap.change_block_range_status(1024 * 1024 * 12, 2, Blockmap.SAVING)

        s = superftp._pretty_blockmap(blockmap, 5, 20)
        # print s.encode('string_escape')

        truth = ('\x1b[37m.....\x1b[92m**\x1b[37m..\x1b[93m001__456789\x1b[K\r\nABCDEF23456789ABCDEF\x1b[K\r\n\x1b[37' +
                 'm....................\x1b[K\r\n..\x1b[K\r\n\x1b[K\r\n')
        self.assertEqual(s, truth)

    def test_pretty_dl_speed_fifo(self):
        """ test pretty summary line function """
        # create a FtpFileDownloader for the test
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)

        # create a blockmap for the test
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 32)
        blockmap.init_blockmap()

        # set a download speed below the kill speed and another equal to the kill speed, and one above the kill speed
        ftp._download_threads['0'].private_dl_speed_fifo[0] = 1024 * 1024 * 2
        ftp._download_threads['0'].private_dl_speed_fifo[1] = 1024 * 1024
        ftp._download_threads['0'].private_dl_speed_fifo[2] = 1024 * 512

        # check
        s = superftp._pretty_dl_speed_fifo(ftp, 1.0)
        truth = ('\x1b[37m[0.000][0.000][0.000][0.000]\x1b[K\n\x1b[91m[0.500]\x1b[37m[0.000][0.000][0.000]\x1b[K\n' +
                 '\x1b[92m[1.000]\x1b[37m[0.000][0.000][0.000]\x1b[K\n\x1b[92m[2.000]\x1b[37m[0.000][0.000][0.000]' +
                 '\x1b[K\n')
        self.assertEqual(s, truth)

    def test_pretty_summary_line(self):
        """ test pretty summary line function """
        ftp = FtpFileDownloader(server_url='localhost', username='user', password='12345', port=2121,
                                concurrent_connections=4, min_blocks_per_segment=1, max_blocks_per_segment=2,
                                initial_blocksize=1048576, kill_speed=0, clean=True)

        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 32)
        blockmap.init_blockmap()

        s = superftp._pretty_summary_line(ftp, blockmap, '\remote\test')
        self.assertEqual(s, 'ETA:infinite        0.0%  0.000MB/sec  \remote\test')


class TestSuperFTPRun(unittest.TestCase):
    """ tests for superftp class run method"""
    def __init__(self, *args, **kwargs):
        super(TestSuperFTPRun, self).__init__(*args, **kwargs)
        self._ftp_thread = None
        self._com_queue = None

    def setUp(self):
        """ start the test ftp server """
        # start the ftp server
        (self._com_queue, self._results_dir,
         self._test_dir, self._ftp_thread) = setup_ftp_server(self._ftp_thread, self._com_queue,
                                                              'results_superftp_run')

    def tearDown(self):
        """ stop the ftp server """
        teardown_ftp_server(self._ftp_thread, self._com_queue)
        self._ftp_thread = None

    def test_run(self):
        """ tests that the superftp run function works properly """
        superftp._run({'server': 'localhost',
                       'username': 'user',
                       'password': '12345',
                       'port': 2121,
                       'connections': 4,
                       'min_blocks_per_segment': 1,
                       'max_blocks_per_segment': 8,
                       'blocksize': 1048576,
                       'kill_speed': 1.0,
                       'clean': True,
                       'enable_tls': False,
                       'display_mode': 'compact',
                       'remote_path': '/',
                       'local_path': self._results_dir})

        # verify the sub directory was actually created
        self.assertTrue(os.path.exists(os.path.join(self._results_dir, 'a')))
        self.assertFalse(os.path.isfile(os.path.join(self._results_dir, 'a')))

        # verify the files were downloaded correctly
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'testfile.txt'),
                                    os.path.join(self._results_dir, 'testfile.txt'), shallow=False))
        self.assertTrue(filecmp.cmp(os.path.join(self._test_dir, 'a/testfile2.txt'),
                                    os.path.join(self._results_dir, 'a/testfile2.txt'), shallow=False))
