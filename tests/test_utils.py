""" utility functions for test cases """

# --------------------------------------------------
#    Imports
# --------------------------------------------------
from contextlib import contextmanager
import os
import Queue
import shutil
from StringIO import StringIO
import sys
from threading import Thread
import time

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import MultiprocessFTPServer

from superftp.blockmap import Blockmap


# --------------------------------------------------
#    Utility Functions
# --------------------------------------------------
@contextmanager
def captured_output():
    """ capture stdout and stderr from a block of code """
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


def create_blockmap(results_dir, filesize, delete_if_exists=True):
    """ create a blockmap with the specified filesize

        Args:
            results_dir - directory to save the blockmap to
            filesize - size of the file the blockmap represents
            delete_if_exists - if True and the blockmap already exists, delete the blockmap
    """
    def filesize_func(_):
        """ return a fake filesize """
        return filesize

    # create the blockmap, delete it if it exists already
    blockmap = Blockmap('testfile.txt', os.path.join(results_dir, 'test.txt'), filesize_func, 1, 3, 1048576)
    if delete_if_exists:
        if blockmap.is_blockmap_already_exists():
            blockmap.delete_blockmap()
    return blockmap


def create_results_dir(results_dir):
    """ creates the results directory for a test

        Args:
            results_dir - the short name of the directory

        Returns:
            the absolute path of the results directory
    """
    full_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), results_dir)
    if os.path.exists(full_dir):
        shutil.rmtree(full_dir)
    os.mkdir(full_dir)
    return full_dir


def create_tdata_dir(testdata_dir):
    """ creates test data for the ftp unit tests

        Args:
            testdata_dir - the short name of the directory to save the test data in

        Returns:
            the absolute path of the test directory
    """
    full_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), testdata_dir)
    shutil.rmtree(full_dir)
    os.mkdir(full_dir)
    filepath = os.path.join(full_dir, 'testfile.txt')
    with open(filepath, 'w') as f:
        for i in range(0, 20):
            f.write(str(i) + '.' * (1024 * 1024) + '\n')
    os.mkdir(os.path.join(full_dir, 'a'))
    filepath = os.path.join(os.path.join(full_dir, 'a'), 'testfile2.txt')
    with open(filepath, 'w') as f:
        for i in range(0, 2):
            f.write(str(i) + '.' * (1024 * 1024) + '\n')
    return full_dir


def setup_ftp_server(ftp_thread, com_queue, results_dir):
    """ start the test ftp server """
    # tearDown in case we had a previous failed test
    teardown_ftp_server(ftp_thread, com_queue)

    # generate the test data
    com_queue = Queue.Queue()
    results_dir = create_results_dir(results_dir)
    test_dir = create_tdata_dir('test_data')
    ftp_thread = start_ftp_server(com_queue, test_dir, 2121)

    # give the ftp server some time to start up
    time.sleep(1)

    # return the new objects
    return (com_queue, results_dir, test_dir, ftp_thread)


def start_ftp_server(com_queue, ftp_root_dir, port=2121):
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
        while com_queue.empty():
            server.serve_forever(timeout=0.1, blocking=False)
        com_queue.get()
        server.close_all()
        time.sleep(1)

    # launch a thread with the ftp server
    t = Thread(target=tw_ftp_server, args=())
    t.start()
    return t


def stop_ftp_server(com_queue):
    """ stop the test ftp server """
    com_queue.put('STOP')


def teardown_ftp_server(ftp_thread, com_queue):
    """ stop the ftp server """
    if ftp_thread:
        stop_ftp_server(com_queue)
        while ftp_thread.is_alive():
            time.sleep(0.01)
