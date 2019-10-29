""" class to download a file from an ftp server """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
from contextlib import closing
import os
import time
from ftplib import FTP
from Queue import Queue, PriorityQueue
from threading import Thread
from blockmap import Blockmap


# --------------------------------------------------
#    Classes
# --------------------------------------------------
class FtpFileDownloader(object):
    """ Performs downloading of a single file using FTP

        set the on_block_downloaded to a function handler of type f(blockmap, byte_offset, worker_id)
        set the on_download_speed_calculated to a function handler of type f(speed, worker_id)
    """

    # constants
    HIGH_PRIORITY_MSG = 0
    LOW_PRIORITY_MSG = 100

    # depth of the FIFO to track download speeds
    SPEED_FIFO_SIZE = 4

    def __init__(self, server_url, username, password, concurrent_connections, port, min_blocks_per_segment,
                 max_blocks_per_segment, blocksize):
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
        self._blocksize = blocksize
        self._concurrent_connections = concurrent_connections
        self._port = port
        self._min_blocks_per_segment = min_blocks_per_segment
        self._max_blocks_per_segment = max_blocks_per_segment
        self._com_queue_in = None
        self._com_queue_out = None
        self._worker_dl_speeds = {format(k, 'x'):
                                  [0] * self.SPEED_FIFO_SIZE for k in range(0, concurrent_connections)}

        # handlers
        self.on_block_downloaded = None
        self.on_download_speed_calculated = None

        # setup the initial dead threads for each download connections
        self._download_threads = [Thread() for _ in range(0, concurrent_connections)]

    def _ftp_get_filesize(self, remote_path):
        """ return the file size of an ftp file on the ftp server

            Args:
                remote_path - path to the file on the ftp server

            Returns:
                number of bytes in the file
        """
        ftp = FTP()
        ftp.connect(self._server_url, self._port)
        ftp.login(self._username, self._password)
        ftp.sendcmd("TYPE i")           # Switch to Binary mode
        size = ftp.size(remote_path)    # Get size of file
        ftp.sendcmd("TYPE A")           # Switch to Binary mode
        return size

    def _tw_ftp_download_segment(self, remote_path, byte_offset, blocks, worker_id):
        """ thread worker to download a segment of a file from teh ftp server

            Args:
                remote_path - path to the file to download on the ftp server
                byte_offset - byte offset into the file to start downloading at
                blocks - number of blocks to download, see self._blocksize for size of each block
                worker_id - id of this worker thread, can be and of the characters in the set [0123456789ABCDEF]
        """
        # open an ftp connection to the server
        ftp = FTP()
        with closing(ftp):
            # connect and login
            ftp.connect(self._server_url, self._port)
            ftp.login(self._username, self._password)

            # switch to FTP binary mode, then initiate a transfer starting at the byte_offset
            ftp.voidcmd('TYPE I')
            conn = ftp.transfercmd('retr %s' % remote_path, byte_offset)
            conn.settimeout(30)

            # loop until the correct amount of data has been transferred
            bytes_received = 0
            bytes_since_last_second = 0
            data = ''
            t = time.time()
            while bytes_received < (blocks * self._blocksize):
                # check for a message on in the incoming communication queue
                while not self._com_queue_in.empty():
                    msg = self._com_queue_in.get_nowait()
                    if msg['type'] == 'kill':
                        if msg['worker_id'] == worker_id:
                            return
                        else:
                            self._com_queue_in.put(msg)
                    else:
                        raise Exception('Unhandled incoming message type of "%s"' % msg['type'])

                # receive the data
                chunk = conn.recv(self._blocksize * 8)

                # calculate the speed and save it to the FIFO.  new speeds are pushed in at index 0
                bytes_since_last_second = bytes_since_last_second + len(chunk)
                if (time.time() - t) > 1:
                    speed = bytes_since_last_second / (time.time() - t)
                    t = time.time()
                    bytes_since_last_second = 0
                    self._worker_dl_speeds[worker_id].insert(0, speed)
                    self._worker_dl_speeds[worker_id] = self._worker_dl_speeds[worker_id][:self.SPEED_FIFO_SIZE]

                # save the data
                if chunk:
                    data = data + chunk

                # send data if greater than blocksize
                while (len(data) > self._blocksize) or not chunk:
                    block = data[:self._blocksize]
                    data = data[self._blocksize:]

                    # enqueue a high priority data received for quickly update the UI that the block has been downloaded
                    # and is pending saving
                    self._com_queue_out.put((self.HIGH_PRIORITY_MSG,
                                             {'type': 'data_received_high_priority', 'worker_id': worker_id,
                                              'byte_offset': byte_offset}))
                    # enqueue a low priority data received to actually save the data
                    self._com_queue_out.put((self.LOW_PRIORITY_MSG,
                                             {'type': 'data_received_low_priority', 'worker_id': worker_id,
                                              'byte_offset': byte_offset, 'data': block}))
                    byte_offset = byte_offset + self._blocksize
                    bytes_received = bytes_received + self._blocksize

                    # stop of EOF
                    if not chunk:
                        break

        # set the download speed to zero
        self._worker_dl_speeds[worker_id] = [0] * self.SPEED_FIFO_SIZE

    @property
    def concurrent_connections(self):
        """ return the number of concurrent download connections """
        return self._concurrent_connections

    @property
    def worker_dl_speeds(self):
        """ return an estimate of the current worker download speeds """
        return self._worker_dl_speeds

    @classmethod
    def clean_local_file(cls, remote_path, local_path):
        """ delete the local file and its blockmap """
        # sanity check for local_path if it is a directory
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, os.path.basename(remote_path))

        # erase the downloaded file if it exists
        if os.path.exists(local_path):
            os.remove(local_path)

        # erase the blockmap if it exists
        blockmap = Blockmap(remote_path, local_path, None, 1, 1, 1024)
        if blockmap.is_blockmap_already_exists():
            blockmap.delete_blockmap()

    def abort_download(self):
        """ abort the current download, notify all of the threads to kill themselves """
        for i in range(0, len(self._download_threads)):
            if self._download_threads[i].is_alive():
                self._com_queue_in.put({'worker_id': format(i, 'x'), 'type': 'kill'})

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
        blockmap = Blockmap(remote_path, local_path, self._ftp_get_filesize, self._min_blocks_per_segment,
                            self._max_blocks_per_segment, self._blocksize)

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
        self._com_queue_in = Queue()            # from manager to download thread
        self._com_queue_out = PriorityQueue()   # from download thread to manager

        # loop until file is downloaded
        while not blockmap.is_blockmap_complete():
            # look for an idle download thread
            for i in range(0, len(self._download_threads)):
                if not self._download_threads[i].is_alive():
                    if blockmap.has_available_blocks():
                        # allocate new blocks
                        worker_id = format(i, 'x')
                        byte_offset, blocks = blockmap.allocate_segment(worker_id)
                        self._download_threads[i] = Thread(target=self._tw_ftp_download_segment,
                                                           args=(remote_path, byte_offset, blocks, worker_id))
                        self._download_threads[i].start()

            # process the message queue from the thread
            if not self._com_queue_out.empty():
                # handle all high priority messages
                while True:
                    # get the next message
                    msg = self._com_queue_out.get()

                    # kick out if this is not a high priority message
                    if msg[0] != self.HIGH_PRIORITY_MSG:
                        break

                    # update the blockmap to show the block has been downloaded and is pending save to disk
                    if msg[1]['type'] == 'data_received_high_priority':
                        blockmap.change_block_range_status(msg[1]['byte_offset'], 1, blockmap.SAVING)
                    else:
                        raise Exception('Unhandled msg type "%s"' % msg[1]['type'])

                # handle only one low priority message, then go give time to the threads
                if msg[1]['type'] == 'data_received_low_priority':
                    # save the block
                    with open(local_path, 'r+b') as f:
                        f.seek(msg[1]['byte_offset'])
                        f.write(msg[1]['data'])
                        f.close()
                    # update the blockmap
                    blockmap.change_block_range_status(msg[1]['byte_offset'], 1, blockmap.DOWNLOADED)

                    # disable pyling warning for self.on_block_downloaded not callable
                    # pylint: disable=E1102
                    if self.on_block_downloaded:
                        kwargs = {'blockmap': blockmap, 'byte_offset': byte_offset,
                                  'worker_id': msg[1]['worker_id'], 'ftp_download_manager': self,
                                  'remote_filepath': remote_path}
                        self.on_block_downloaded(self, **kwargs)
                else:
                    raise Exception('Unhandled msg type "%s"' % msg['type'])

            # sleep
            time.sleep(0.001)

        # clean up the block map
        blockmap.delete_blockmap()
