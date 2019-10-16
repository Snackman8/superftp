import math
import os

class Blockmap():
    """ class used to keep track of which blocks in the file have been downloaded, which are currently pending download
        by which connections, and which blocks are available.
        
        The blockmap is always read from disk and never kept in memory
    """
    DOWNLOADED = '*'
    AVAILABLE = '.'
    SAVING = '_'
    PENDING = '0123456789ABCDEF'
    
    def __init__(self, remote_path, local_path, max_blocks_per_segment, file_size_func):
        """ initialize the blockmap
        
            Check if a local blockmap exists in the local_path location, if it does not exist, try to contact the FTP
            server to get the filesize and then create a new blockmap
        
            Args:
                remote_path - path on ftp server of file to download
                local_path - local path on disk where downloaded file will be saved
                max_blocks_per_segment - maximum number of blocks per download segment
                file_size_func - a function that can be called to get the file size of the file on the FTP server, the
                                 prototype for this function is file_size_func(remote_path) returns an int
        """     
        self._blocksize = 1024 * 1024
        self._remote_path = remote_path
        self._local_path = local_path
        self._max_blocks_per_segment = max_blocks_per_segment
        self._file_size_func = file_size_func

        # generate the blockmap_path
        if os.path.isdir(local_path):
            raise Exception('Error! local path "%s" is a directory, must be a file' % local_path)        
        self._blockmap_path = self._local_path + '.blockmap'

    def _read_blockmap(self):
        """ load the blockmap from the local copy """
        with open(self._blockmap_path, 'r') as f:
            return f.read()
            
    def _persist_blockmap(self, blockmap):
        """ save the blockmap to disk """
        with open(self._blockmap_path, 'w') as f:
            f.write(blockmap)

    def set_pending_to_saving(self, worker_id):
        blockmap = self._read_blockmap()
        blockmap = blockmap.replace(worker_id, self.SAVING)
        self._persist_blockmap(blockmap)        

    def allocate_segment(self, worker_id):
        """ allocate an available segment in the blockmap to the specified worker_id
        
            Args:
                worker_id - a worker id to allocate the segment to
            
            Returns:
                (starting_block, blocks)
        """
        # clean the blockmap of any pending blocks with this worker_id
#        self.clean_blockmap(worker_id)
        
        # read the blockmap and look for the longest contiguous chain of available blocks
        blockmap = self._read_blockmap()
        for i in range(self._max_blocks_per_segment, 0, -1):
            blocks = i
            start_block = blockmap.find('.' * i)
            if start_block >= 0:
                break

        # try to allocate the segment
        byte_offset = start_block * self._blocksize
        self.change_block_range_status(byte_offset, blocks, worker_id)
        
        # return the allocated segment
        return byte_offset, blocks

    def change_block_range_status(self, byte_offset, blocks, status):
        """ change the status of a block range
        
            Args:
                byte_offset - starting byte offset of the block.  NOTE: This is not a block number but a byte offset!
                blocks - number of blocks to change status of.  NOTE: This is number of blocks, not bytes!
                status - new status
        """
        # make sure byte_offset is a multiple of block size
        if byte_offset % self._blocksize != 0:
            raise Exception('byte_offset %d is not a multiple of block size %d' % (byte_offset, self._blocksize))

        # make sure status is valid
        if status not in (self.AVAILABLE, self.DOWNLOADED) and status not in self.PENDING:
            raise Exception('status of "%s" is not a valid status' % status) 
            
        # set the status of the block
        blockmap = self._read_blockmap()
        og_blockmap = blockmap
        starting_block = byte_offset / self._blocksize
        for i in range(0, blocks):
            blockmap = blockmap[:starting_block + i] + status + blockmap[starting_block + i + 1:]
        self._persist_blockmap(blockmap)
        print og_blockmap, '->', blockmap

#     def clean_blockmap(self, worker_id):
#         """ clean any pending blocks from the blockmap """
#         # check that worker id is a 
#         if worker_id not in self.PENDING:
#             raise Exception('"%s" is not a valid worker_id' % worker_id)
#  
#         blockmap = self._read_blockmap()
#         blockmap = blockmap.replace(worker_id, self.AVAILABLE)
#         self._persist_blockmap(blockmap)

    def delete_blockmap(self):
        """ delete the blockmap """
        os.remove(self._blockmap_path)

    def has_available_blocks(self):
        blockmap = self._read_blockmap()
        return '.' in blockmap

    def init_blockmap(self):
        """ initialize the blockmap if it does not exist """
        # create a new blockmap if it does not exist saved to disk
        if not os.path.exists(self._blockmap_path):
            # create a new blockmap and save it
            filesize = self._file_size_func(self._remote_path)
            blockmap = self.AVAILABLE * int(math.ceil(filesize / float(self._blocksize)))
            self._persist_blockmap(blockmap)
        
    def is_blockmap_complete(self):
        """ returns true if the blockmap shows that the entire file has been downloaded """
        blockmap = self._read_blockmap()
        return set(blockmap) == set('*')

    def is_blockmap_already_exists(self):
        return os.path.exists(self._blockmap_path)
        