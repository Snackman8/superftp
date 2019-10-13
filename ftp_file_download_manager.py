import math
import os
from ftplib import FTP

class FtpFileDownloader:    
    """ Performs downloading of a single file using FTP """
    def __init__(self, server_url, username, password):
        """
            Args:
                server_url - url to the ftp server
                username - username to login to ftp server with
                password - password to login to ftp server with
        """
        # init
        self._server_url = server_url
        self._username = username
        self._password = password
        self._blocksize = 1024 * 1024

    def _download_segment(self):
        pass
 
    def _allocate_segment(self, block_map):
        pass
 
    def _activate_download_thread(self, thread_id, segment):
        pass

    def _get_blockmap_for_file(self, remote_path, local_path):
        """ Retrieves the blockmap for a file.  The local_path is first checked for an existing blockmap.
            If there is an existing blockmap, it is assumed that this is a continuation of a previously aborted
            download.  If no blockmap exists at the local_path, a new blockmap is created and written to the 
            local_path location.
            
            The remote server must be contacted to retrieve the size of the remote file in
            order to create a new blockmap.
            
            Args:
                remote_path - path to the remote file
                local_path - file location to save the remote file to
            
            Returns:
                blockmap as a string
        """
        # calculate the blockmap location based on the local_path
        blockmap_path = local_path + '.blockmap'
        
        # read the existing blockmap if it exists already
        if os.path.exists(blockmap_path):
            with open(blockmap_path, 'r') as f: 
                return f.read()
        
        # calculate a new blockmap, query the filesize from the remote ftp server
        ftp = FTP(self._server_url)
        ftp.login(self._username, self._password)
#        ftp.prot_p()
        print ftp.retrlines('LIST')
        ftp.sendcmd("TYPE i")    # Switch to Binary mode
        size = ftp.size(remote_path)   # Get size of file
        print size        
        ftp.sendcmd("TYPE A")    # Switch to Binary mode
        blockmap = '.' * int(math.ceil(size / float(self._blocksize)))
        
        # write the new blocmap to disk
        with open(blockmap_path, 'w') as f:
            f.write(blockmap)
        
        # return the blockmap
        return blockmap
    
    def _get_download_thread_state(self, thread_id):
        pass

    def _is_download_finished(self, blockmap):
        """ return True if the blockmap shows that all blocks have been downloaded
        
            Args:
                blockmap - blockmap string
            
            Returns:
                True if the blockmap shows all blocks have been downloaded
        """
        return set(blockmap) == set('*')
    
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

        # load an existing block map or generate a block map for the file
        block_map = self._get_blockmap_for_file(remote_path, local_path)

        # loop until file is downloaded
        while not self._finished(block_map):
            # look for an idle download thread
            for i in range(0, self._max_download_threads):
                if self._get_download_thread_state(i) == 'IDLE':
                    segment = self._allocate_segment(block_map)
#                     self._activate_download_thread(i, segment)
#             
            # sleep
            time.sleep(1)
        
        # clean up the block map
#        self._delete_block_map_for_file()
