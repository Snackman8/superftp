""" blockmap module """

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import math
import os


# --------------------------------------------------
#    Classes
# --------------------------------------------------
class BlockmapException(Exception):
    """ Blockmap Exception Class """


class Blockmap:
    """ class used to keep track of which blocks in the file have been downloaded, which are currently pending download
        by which connections, and which blocks are available.

        The blockmap is always read from disk and never kept in memory
    """
    # --------------------------------------------------
    # Constants
    # --------------------------------------------------
    DOWNLOADED = '*'                # block has been saved to the disk
    AVAILABLE = '.'                 # block is available to be allocated to a worker thread for downloading
    SAVING = '_'                    # data for block has been received and is in the queue waiting to be written to disk
    PENDING = '0123456789ABCDEF'    # block has been allocated to one of the worker threads (16 possible)

    # --------------------------------------------------
    # Init
    # --------------------------------------------------
    def __init__(self, remote_path, local_path, file_size_func, min_blocks_per_segment=8, max_blocks_per_segment=512,
                 initial_blocksize=1048576):
        """ initialize the blockmap

            Check if a local blockmap exists in the local_path location, if it does not exist, try to contact the FTP
            server to get the filesize and then create a new blockmap

            The min_blocks_per_segment, max_blocks_per_segment, and blocksize can be tuned  for optimal performance
            given the situation.  If the time to establish a connection is long relative to download speed, the
            min_blocks_per_segment should be increased so that less time is spent waiting for a connection instead of
            using a previously established connection.

            If the download speeds are highly variable per connection, the max_blocks_per_segment can be lowered to
            force more connection turnover in the hopes of achieving a better download speed.

            Args:
                remote_path - path on ftp server of file to download
                local_path - local path on disk where downloaded file will be saved
                file_size_func - a function that can be called to get the file size of the file on the FTP server, the
                                 prototype for this function is file_size_func(remote_path) returns an int
                min_blocks_per_segment - minimum number of blocks per download segment, default is 8
                max_blocks_per_segment - maximum number of blocks per download segment, default is 512
                initial_blocksize - size of each block in bytes if creating a new blockmap, default is 1MB
        """
        self._initial_blocksize = initial_blocksize
        self._remote_path = remote_path
        self._local_path = local_path
        self._file_size_func = file_size_func
        self._min_blocks_per_segment = min_blocks_per_segment
        self._max_blocks_per_segment = max_blocks_per_segment

        # generate the blockmap_path
        if os.path.isdir(local_path):
            raise BlockmapException('Error! local path "%s" is a directory, must be a file' % local_path)
        self._blockmap_path = self._local_path + '.blockmap'

    def __repr__(self):
        """ string representation of the blockmap """
        return self.__str__()

    def __str__(self):
        """ string representation of the blockmap """
        s = ''
        if self.is_blockmap_already_exists():
            _blocksize, blockmap = self._read_blockmap()
            s = str(blockmap)
        return s

    # --------------------------------------------------
    # Private Functions
    # --------------------------------------------------
    def _read_blockmap(self):
        """ load the blockmap from the local copy on the disk

            skip the first line, which is the blocksize

            Returns:
                string representation of the blockmap
        """
        with open(self._blockmap_path, 'r') as f:
            s = f.read()
            blocksize = int(s.split('\n')[0])
            s = s[s.find('\n') + 1:]
            return blocksize, s

    def _persist_blockmap(self, blocksize, blockstr):
        """ save the blockmap to disk

            Args:
                blockstr - string representation of the blockmap to persist
        """
        with open(self._blockmap_path, 'w') as f:
            f.write(str(blocksize) + '\n' + blockstr)

    def allocate_segments(self, worker_ids):
        """ allocate an available segment in the blockmap to the specified worker_id

            Args:
                worker_id - list of worker ids to allocate the segment to

            Returns:
                (starting_block, blocks)
        """
        # exit if no worker_ids
        if not worker_ids:
            return {}

        # find the largest available free block
        retval = {}
        blocksize, blockmap = self._read_blockmap()
        for segment_size in range(len(blockmap), 0, -1):
            start_block = blockmap.find('.' * segment_size)
            if start_block >= 0:
                # calculate the optimal_segment_size
                optimal_segment_size = int(math.ceil(float(segment_size) / len(worker_ids)))
                optimal_segment_size = min(optimal_segment_size, self._max_blocks_per_segment)
                optimal_segment_size = max(optimal_segment_size, self._min_blocks_per_segment)

                # allocate to the worker_ids
                x = start_block
                for k in worker_ids:
                    blocks = min(segment_size, optimal_segment_size)
                    retval[k] = {'byte_offset': x * blocksize, 'blocks': blocks}
                    x = x + blocks
                    segment_size = segment_size - blocks
                    assert segment_size >= 0
                    self.change_block_range_status(retval[k]['byte_offset'], retval[k]['blocks'], k)
                    if segment_size == 0:
                        break
                break

        # return the results
        return retval

    def change_status(self, old_status, new_status):
        """ change all of the blocks that have the old_status to the new_status

            Args:
                old_status - status to search for
                new_status - status to replace with
        """
        if old_status not in (self.AVAILABLE, self.DOWNLOADED, self.SAVING) and old_status not in self.PENDING:
            raise BlockmapException('status of "%s" is not a valid status' % old_status)
        if new_status not in (self.AVAILABLE, self.DOWNLOADED, self.SAVING) and new_status not in self.PENDING:
            raise BlockmapException('status of "%s" is not a valid status' % new_status)

        blocksize, blockmap = self._read_blockmap()
        blockmap = blockmap.replace(old_status, new_status)
        self._persist_blockmap(blocksize, blockmap)

    def change_block_range_status(self, byte_offset, blocks, status):
        """ change the status of a block range

            Args:
                byte_offset - starting byte offset of the block.  NOTE: This is not a block number but a byte offset!
                blocks - number of blocks to change status of.  NOTE: This is number of blocks, not bytes!
                status - new status
        """
        # read the blockmap
        blocksize, blockmap = self._read_blockmap()

        # make sure byte_offset is a multiple of block size
        if byte_offset % blocksize != 0:
            raise BlockmapException('byte_offset %d is not a multiple of block size %d' % (byte_offset, blocksize))

        # make sure status is valid
        if status not in (self.AVAILABLE, self.DOWNLOADED, self.SAVING) and status not in self.PENDING:
            raise BlockmapException('status of "%s" is not a valid status' % status)

        # set the status of the block
        starting_block = byte_offset // blocksize
        for i in range(0, blocks):
            blockmap = blockmap[:starting_block + i] + status + blockmap[starting_block + i + 1:]
        self._persist_blockmap(blocksize, blockmap)

    def delete_blockmap(self):
        """ delete the blockmap """
        os.remove(self._blockmap_path)

    def get_statistics(self, dl_speed=0):
        """ return statistics about the blockmap

            returns a tuple of (non_downloaded_blocks, available blocks, number of blocks, blocklsize, eta)
        """
        # read the blockmap
        blocksize, blockmap = self._read_blockmap()

        # calculate non saved blocks
        non_downloaded_blocks = len(blockmap) - blockmap.count('*')

        # calcualte available blocks
        available_blocks = blockmap.count('.')

        # calculate ETA
        if dl_speed == 0:
            if non_downloaded_blocks == 0:
                eta = 'done'
            else:
                eta = 'infinite'
        else:
            eta = (non_downloaded_blocks * blocksize) / dl_speed
            if eta < 120:
                eta = '%d seconds' % eta
            else:
                eta = '%0.1f minutes' % (eta / 60)

        return (non_downloaded_blocks, available_blocks, len(blockmap), blocksize, eta)

    def init_blockmap(self):
        """ initialize the blockmap if it does not exist, or clean it if it does exist """
        # create a new blockmap if it does not exist saved to disk
        if not os.path.exists(self._blockmap_path):
            # create a new blockmap and save it
            filesize = self._file_size_func(self._remote_path)
            blockmap = self.AVAILABLE * int(math.ceil(filesize / float(self._initial_blocksize)))
            blocksize = self._initial_blocksize
        else:
            # clean the blockmap
            blocksize, blockmap = self._read_blockmap()
            chars = list(set(blockmap))
            for c in chars:
                if c != '*':
                    blockmap = blockmap.replace(c, self.AVAILABLE)
        self._persist_blockmap(blocksize, blockmap)

    def is_blockmap_complete(self):
        """ returns true if the blockmap shows that the entire file has been downloaded """
        _blocksize, blockmap = self._read_blockmap()
        return set(blockmap) == set('*')

    def is_blockmap_already_exists(self):
        """ return true if a blockmap exists on the local disk """
        return os.path.exists(self._blockmap_path)
