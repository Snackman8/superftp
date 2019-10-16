from contextlib import closing
import math
import os
import time
from ftplib import FTP
from multiprocessing import Queue
from threading import Thread
from blockmap import Blockmap

class FtpFileDownloader:    
    """ Performs downloading of a single file using FTP """
    def __init__(self, server_url, username, password, concurrent_connections, port=21):
        """
            Args:
                server_url - url to the ftp server
                username - username to login to ftp server with
                password - password to login to ftp server with
                concurrent_connections - number of concurrent download connections
        """
        # init
        self._server_url = server_url
        self._username = username
        self._password = password
        self._blocksize = 1024 * 1024
        self._max_blocks_per_segment = 2
        self._port = port
        
        # setup the initial dead threads for each download connections
        self._download_threads = [Thread() for _ in range(0, concurrent_connections)]

    def _ftp_get_filesize(self, remote_path):
        ftp = FTP(self._server_url)
        ftp.login(self._username, self._password)        
        ftp.sendcmd("TYPE i")           # Switch to Binary mode
        size = ftp.size(remote_path)    # Get size of file
        ftp.sendcmd("TYPE A")           # Switch to Binary mode
        return size

    def _save_block(self, local_path, byte_offset, data):
        with open(local_path, 'r+b') as f:
            f.seek(byte_offset)
            f.write(job['data'])
            f.close()

    def _tw_ftp_download_segment(self, com_queue_in, com_queue_out, remote_path, byte_offset, blocks, worker_id):
        # open an ftp connection to the server
        with closing(FTP()) as ftp:            
            # connect and login
            ftp.connect(self._server_url, self._port)
            ftp.login(self._username, self._password)

            # switch to FTP binary mode, then initiate a transfer starting at the byte_offset
            ftp.voidcmd('TYPE I')
            conn = ftp.transfercmd('retr %s' % remote_path, byte_offset)
            conn.settimeout(30)            
    
            # loop until the correct amount of data has been transferred
            bytes_received = 0
            data = ''
            while bytes_received < (blocks * self._blocksize):
                # check for a message on in the incoming communication queue
                while not com_queue_in.empty():
                    msg = com_queue_in.get_nowait()
                    if msg['worker_id'] == worker_id and msg['type'] == 'kill':
                        return
                    else:
                        raise Exception('Unhandled incoming message type of "%s"' % msg['type'])
                
                # receive the data
                chunk = conn.recv(self._blocksize)
                
                # EOF if chunk is empty
                if not chunk:
                    # EOF, so break out of the loop and notify about the final partial block of data
                    # send the final data
                    com_queue_out.put({'type': 'data_received', 'byte_offset': byte_offset, 'data': data, 'worker_id': worker_id})
                    return
            
                # save the data
                data = data + chunk
                
                # send data if greater than blocksize
                if len(data) > self._blocksize:
                    block = data[:self._blocksize]
                    data = data[self._blocksize:]
                    com_queue_out.put({'type': 'data_received', 'worker_id': worker_id,
                                       'byte_offset': byte_offset, 'data': block})
                    byte_offset = byte_offset + self._blocksize
                    bytes_received = bytes_received + self._blocksize            
                    
    def download_file(self, remote_path, local_path):
        """ downloads a file from a remote ftp server
        
            Args:
                remote_path - path to the remote file
                local_path - file location to save the remote file to
            
            Returns:
                None
            
            Exceptions:
                Throws exceptions on error
        """
        # sanity check for local_path if it is a directory
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, os.path.basename(remote_path))

        # construct a blockmap, but the blockmap is written to disk until init_blockmap if it does not exist yet
        blockmap = Blockmap(remote_path, local_path, self._max_blocks_per_segment, self._ftp_get_filesize)

        # exit if this file has already been downloaded
        if not blockmap.is_blockmap_already_exists() and os.path.exists(local_path):
            if os.path.getsize(local_path) > 0:
                return

        # create the local file if it does not exist
        if not os.path.exists(local_path):
            f = open(local_path, 'wb')
            f.close()

        # initialize the blockmap
        blockmap.init_blockmap()

        # setup the communication queues
        com_queue_in = Queue()      # from manager to download thread
        com_queue_out = Queue()     # from download  thread to manager

        # loop until file is downloaded
        while not blockmap.is_blockmap_complete():
            # look for an idle download thread
            for i in range(0, len(self._download_threads)):
                if not self._download_threads[i].is_alive():
                    if blockmap.has_available_blocks():
                        worker_id = str(i)
                        # assume any pending blocks for this thread are in a saving state
                        blockmap.set_pending_to_saving(worker_id)
                        # allocate new blocks
                        byte_offset, blocks = blockmap.allocate_segment(worker_id)
                        self._download_threads[i] = Thread(target=self._tw_ftp_download_segment,
                                                           args=(com_queue_in, com_queue_out, remote_path, byte_offset,
                                                                 blocks, worker_id))
                        self._download_threads[i].start()

            # process the message queue from the thread
            while not com_queue_out.empty():
                msg = com_queue_out.get()
                if msg['type'] == 'data_received':
                    # save the block
                    with open(local_path, 'r+b') as f:
                        f.seek(msg['byte_offset'])
                        f.write(msg['data'])
                        f.close()
                    # update the blockmap
                    blockmap.change_block_range_status(msg['byte_offset'], 1, '*')
                else:
                    raise Exception('Unhandled msg type "%s"' % msg['type'])

            # sleep
            time.sleep(1)
        
        # clean up the block map
        blockmap.delete_blockmap()
