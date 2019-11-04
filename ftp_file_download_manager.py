""" class to download a file from an ftp server """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
from collections import OrderedDict
from contextlib import closing
import os
import time
from ftplib import FTP, FTP_TLS, error_temp, error_perm
from Queue import Queue, PriorityQueue, Empty
from threading import Thread
from blockmap import Blockmap


# --------------------------------------------------
#    Classes
# --------------------------------------------------
class FtpFileDownloader(object):
    """ Performs downloading of a single file using FTP

        set the on_refresh_display to a function handler of type f(blockmap, byte_offset, worker_id)
    """
    # --------------------------------------------------
    # Constants
    # --------------------------------------------------
    HIGH_PRIORITY_MSG = -1  # MSG priority of a high priority message in the message queue

    IDLE = 0                # download thread state - IDLE, waiting for allocation if available
    ACTIVE = 1              # download thread state - ACTIVE, actively retrieving data from the remote FTP server
    ABORTING = 2            # download thread state - ABORTING, thread is in the process of being aborted and killed

    SPEED_FIFO_SIZE = 4     # depth of the FIFO to track download speeds

    # --------------------------------------------------
    # Init
    # --------------------------------------------------
    def __init__(self, server_url, username, password, concurrent_connections, port, min_blocks_per_segment,
                 max_blocks_per_segment, blocksize, kill_speed, clean, disable_tls):
        """
            Args:
                server_url - url to the ftp server
                username - username to login to ftp server with
                password - password to login to ftp server with
                concurrent_connections - number of concurrent download connections
        """
        # TODO: run somethign to generate reference docs
        # TODO: clean this up, assign reasonable defaults and document the parameters
        # init
        self._server_url = server_url
        self._username = username
        self._password = password
        self._blocksize = blocksize
        self._concurrent_connections = concurrent_connections
        self._port = port
        self._min_blocks_per_segment = min_blocks_per_segment
        self._max_blocks_per_segment = max_blocks_per_segment
        self._kill_speed = kill_speed
        self._com_queue_in = None
        self._com_queue_out = None
        self._clean = clean
        self._disable_tls = disable_tls
#         self._worker_dl_speeds = OrderedDict([(format(k, 'x'),
#                                                [0] * self.SPEED_FIFO_SIZE) for k in range(0, concurrent_connections)])

        # handlers
        self.on_refresh_display = lambda _ftp_file_downloader, _blockmap, _remote_filepath: None

        # setup the initial dead threads for each download connections
        self._download_threads = OrderedDict([(format(k, 'x'),
                                               Thread()) for k in range(0, concurrent_connections)])
        for k in self._download_threads.keys():
            self._download_threads[k].private_thread_state = self.IDLE
            self._download_threads[k].private_start_time = time.time()
            self._download_threads[k].private_dl_speed_fifo = [0] * self.SPEED_FIFO_SIZE

    # --------------------------------------------------
    # Private Functions
    # --------------------------------------------------
    def _ftp_connection(self):
        """ throws an exception similar to ftplib.error_perm: 500 Unknown command: "AUTH TLS" if ftp server does not
        support tls """
        if self._disable_tls:
            ftp = FTP()
        else:
            ftp = FTP_TLS()

        ftp.connect(self._server_url, self._port)

        ftp.login(self._username, self._password)

        if not self._disable_tls:
            ftp.prot_p()
        return ftp

    def _ftp_get_filesize(self, remote_path):
        """ return the file size of an ftp file on the ftp server

            Args:
                remote_path - path to the file on the ftp server

            Returns:
                number of bytes in the file
        """
        ftp = self._ftp_connection()
        ftp.sendcmd("TYPE i")           # Switch to Binary mode
        size = ftp.size(remote_path)    # Get size of file
        ftp.sendcmd("TYPE A")           # Switch to Binary mode
        return size

    def _manage_download_threads(self, blockmap, remote_path):
        """ kill underperforming thread and allocate work to idle threads """
        # check if we need to kill any download threads because they have stalled
        # kill speed must be set to greater than zero, and the download thread must be at least 10 seconds old
        # the download speed usually ramps up if the server is far away so we give it time to ramp up before
        # checking if it has stalled or is stuck on a very slow packet path
        if self._kill_speed > 0:
            for k in self.worker_dl_speeds:
                # check the speed if the thread is active
                if (self._download_threads[k].private_thread_state == self.ACTIVE and
                        time.time() - self._download_threads[k].private_start_time > 20):
                    # do not kill if we are still starting up, we can tell since we have 0 speeds
                    if 0 not in self.worker_dl_speeds[k]:
                        # kill if the average speed
                        if max(self.worker_dl_speeds[k]) / 1024 / 1024 < self._kill_speed:
                            self.abort_download(k)

        # look for an idle download thread
        for k in self._download_threads.keys():
            if self._download_threads[k].private_thread_state == self.IDLE:
                _, available_blocks, _, _ = blockmap.get_statistics()
                if available_blocks > 0:
                    # allocate new blocks
                    byte_offset, blocks = blockmap.allocate_segment(k)
                    self._download_threads[k] = Thread(target=self._tw_ftp_download_segment,
                                                       args=(remote_path, byte_offset, blocks, k))
                    self._download_threads[k].private_thread_state = self.ACTIVE
                    self._download_threads[k].private_start_time = time.time()
                    self._download_threads[k].private_dl_speed_fifo = [0] * self.SPEED_FIFO_SIZE
                    self._download_threads[k].start()

    def _process_high_priority_messages(self, blockmap):
        # process all of the available high priority messages
        while not self._com_queue_out.empty():
            # get the next message
            msg = self._com_queue_out.get()

            # if this is not a high priority message, put it back in the queue and exit
            if msg[0] != self.HIGH_PRIORITY_MSG:
                self._com_queue_out.put(msg)
                break

            # process the message
            if msg[1]['type'] == 'data_received_high_priority':
                # update the blockmap to show the block has been downloaded and is pending save to disk
                blockmap.change_block_range_status(msg[1]['byte_offset'], 1, blockmap.SAVING)
            elif msg[1]['type'] in ('aborted_high_priority', 'thread_finished_high_priority'):
                # abort this download thread, so mark all of the blocks as available
                blockmap.change_status(msg[1]['worker_id'], blockmap.AVAILABLE)
                # set the download thread state to IDLE
                self._download_threads[msg[1]['worker_id']].private_thread_state = self.IDLE
                # zero out the download speeds for this thread
                self._download_threads[msg[1]['worker_id']].private_dl_speed_fifo = [0] * self.SPEED_FIFO_SIZE
            elif msg[1]['type'] == 'dl_speed_update_high_priority':
                # a new download speed has been calculated, update the worker dl speed fifo
                self._download_threads[msg[1]['worker_id']].private_dl_speed_fifo.insert(0, msg[1]['dl_speed'])
                self._download_threads[msg[1]['worker_id']].private_dl_speed_fifo.pop(-1)
            else:
                raise Exception('Unhandled msg type "%s"' % msg[1]['type'])

    def _process_low_priority_messages(self, blockmap, local_path):
        # init
        blocks = 0
        data = ''
        next_byte_offset = None
        starting_byte_offset = None

        # loop through the messages and see if we can assemble a chain of consecutive blocks
        while not self._com_queue_out.empty():
            # peek at the next message
            msg = self._com_queue_out.get()
            # if then ext message is not the next block in the data chain, put the msg back and process what we have
            if (msg[0] == self.HIGH_PRIORITY_MSG) or ((next_byte_offset is not None) and
                                                      (msg[1]['byte_offset'] != next_byte_offset)):
                self._com_queue_out.put(msg)
                break

            # sanity check, this is the only low priority message we have
            if msg[1]['type'] != 'data_received_low_priority':
                raise Exception('Unhandled msg type "%s"' % msg[1]['type'])

            # add this message data to the chain
            if starting_byte_offset is None:
                starting_byte_offset = msg[1]['byte_offset']
            blocks = blocks + 1
            data = data + msg[1]['data']
            next_byte_offset = msg[1]['byte_offset'] + blockmap.blocksize

        # save the block
        if starting_byte_offset is not None:
            with open(local_path, 'r+b') as f:
                f.seek(starting_byte_offset)
                f.write(data)
                f.close()
            # update the blockmap
            blockmap.change_block_range_status(starting_byte_offset, blocks, blockmap.DOWNLOADED)

    def _tw_ftp_download_segment(self, remote_path, byte_offset, blocks, worker_id):
        """ thread worker to download a segment of a file from teh ftp server

            Args:
                remote_path - path to the file to download on the ftp server
                byte_offset - byte offset into the file to start downloading at
                blocks - number of blocks to download, see self._blocksize for size of each block
                worker_id - id of this worker thread, can be and of the characters in the set [0123456789ABCDEF]
        """
        # open an ftp connection to the server
        ftp = self._ftp_connection()
        with closing(ftp):
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
                    try:
                        msg = self._com_queue_in.get_nowait()
                    except Empty, _:
                        continue
                    if msg['type'] == 'kill':
                        if msg['worker_id'] == worker_id:
                            self._com_queue_out.put((self.HIGH_PRIORITY_MSG,
                                                     {'type': 'aborted_high_priority', 'worker_id': worker_id}))
                            return
                        else:
                            self._com_queue_in.put(msg)
                    else:
                        raise Exception('Unhandled incoming message type of "%s"' % msg['type'])

                # receive the data
                chunk = conn.recv(self._blocksize * 8)

                # calculate the speed and save it to the FIFO.  new speeds are pushed in at index 0
                bytes_since_last_second = bytes_since_last_second + len(chunk)
                if (time.time() - t) > 1.0:
                    speed = bytes_since_last_second / (time.time() - t)
                    t = time.time()
                    bytes_since_last_second = 0
                    self._com_queue_out.put((self.HIGH_PRIORITY_MSG,
                                             {'type': 'dl_speed_update_high_priority', 'worker_id': worker_id,
                                              'dl_speed': speed}))

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
                    self._com_queue_out.put((byte_offset,
                                             {'type': 'data_received_low_priority', 'worker_id': worker_id,
                                              'byte_offset': byte_offset, 'data': block}))
                    byte_offset = byte_offset + self._blocksize
                    bytes_received = bytes_received + self._blocksize

                    # stop of EOF
                    if not chunk:
                        break

            # set the thread to be idle
            self._com_queue_out.put((self.HIGH_PRIORITY_MSG,
                                     {'type': 'thread_finished_high_priority', 'worker_id': worker_id}))

    # --------------------------------------------------
    # Properties
    # --------------------------------------------------
    @property
    def concurrent_connections(self):
        """ return the number of concurrent download connections """
        return self._concurrent_connections

    @property
    def kill_speed(self):
        """ return the speed which if the connection is under will be killed """
        return self._kill_speed

    @property
    def total_dl_speed(self):
        """ return the total download speed of all the workers """
        dl_speed = 0
        for worker_id in self.worker_dl_speeds:
            dl_speed = dl_speed + (sum(self.worker_dl_speeds[worker_id]) /
                                   len(self.worker_dl_speeds[worker_id]))
        return dl_speed

    @property
    def worker_dl_speeds(self):
        """ return an estimate of the current worker download speeds """
        retval = {}
        for k in self._download_threads.keys():
            retval[k] = self._download_threads[k].private_dl_speed_fifo
        return retval

    # --------------------------------------------------
    # Methods
    # --------------------------------------------------
    def abort_download(self, worker_id=None):
        """ abort the current download, notify all of the threads to kill themselves

            Args:
                worker_id - worker_id of the download thread to kill, or kill all threads if None
        """
        for k in self._download_threads.keys():
            if worker_id is None or worker_id == k:
                if self._download_threads[k].private_thread_state == self.ACTIVE:
                    self._download_threads[k].private_thread_state = self.ABORTING
                    self._com_queue_in.put({'worker_id': k, 'type': 'kill'})

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

    def download(self, remote_path, local_path):
        """ download a directory or a file from the ftp server

            Args:
                remote_path - path to the remote file
                local_path - file location to save the remote file to
        """
        # open an ftp connection to the server, have a special check if the server does not support TLS
        try:
            ftp = self._ftp_connection()
        except (error_temp, error_perm), e:
            # show a pretty message if this server does not support AUTH_TLS
            if e.message.startswith('500') and 'TLS' in e.message:
                raise IOError(e.message + '\nThis server probably does not support TLS, ' +
                              'try adding the --disable_tls flag')

        with closing(ftp):
            # check if this is a directory
            listing = None
            try:
                ftp.cwd(remote_path)
                listing = ftp.nlst(remote_path)
            except (error_temp, error_perm), _:
                pass

        # download the file if this is not a directory
        if listing is None:
            # this is a file so just download the file
            self.download_file(remote_path, local_path)
            return

        # this is a directory, create it if it does not exist
        if not os.path.exists(local_path):
            os.mkdir(local_path)

        # loop through each item in the directory and download it
        for f in listing:
            self.download(os.path.join(remote_path, f), os.path.join(local_path, f))

    def download_file(self, remote_path, local_path):
        """ downloads a file from a remote ftp server

            Args:
                remote_path - path to the remote file
                local_path - file location to save the remote file to
        """
        # sanity check for local_path if it is a directory
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, os.path.basename(remote_path))

        # clean the file if needed
        if self._clean:
            self.clean_local_file(remote_path, local_path)

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

        # loop until file is downloaded and fully saved to disk
        while not blockmap.is_blockmap_complete():
            self._manage_download_threads(blockmap, remote_path)

            # process all of the high priority messages
            self._process_high_priority_messages(blockmap)

            # process low priority messages (just process one of them)
            self._process_low_priority_messages(blockmap, local_path)

            # call the refresh display callback
            self.on_refresh_display(self, blockmap, remote_path)

            # sleep
            time.sleep(0.001)

        # clean up the block map
        blockmap.delete_blockmap()
