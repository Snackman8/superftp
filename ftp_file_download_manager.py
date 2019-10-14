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

    def _get_download_thread_state(self, thread_id):
        pass

    def tw_ftp_download_segment(self, com_queue_in, com_queue_out, remote_path, byte_offset, blocks, blocksize,
                                worker_id):
        """ ??? """
        # open an ftp connection to the server
        with closing(ftplib.FTP(self._server_url)) as ftp:
            # connect and login
            ftp.connect(self._port)
            ftp.login(self._username, self._password)

            # switch to FTP binary mode, then initiate a transfer starting at the byte_offset
            ftp.voidcmd('TYPE I')
            conn = ftp.transfercmd('retr %s' % remote_path, byte_offset)
            conn.settimeout(30)            
    
            # loop until the correct amount of data has been transferred
            bytes_received = 0
            data = ''
            while bytes_received < (blocks * blocksize):
                # check for a message on in the incoming communication queue
                while not com_queue_in.empty():
                    msg = com_queue_in.get_nowait()
                    if msg['type'] == 'kill':
                        return
                    else:
                        raise Exception('Unhandled incoming message type of "%s"' % msg['type'])
                
                # receive the data
                chunk = conn.recv(blocksize)
                
                # EOF if chunk is empty
                if not chunk:
                    # EOF, so break out of the loop and notify about the final partial block of data
                    break
            
                # save the data
                data = data + chunk
                
                # send data if greater than blocksize
                if len(data) > blocksize:
                    block = data[:blocksize]
                    data = data[blocksize:]
                    com_queue_out.put({'type': 'data_received', 'worker_id': worker_id,
                                       'byte_offset': byte_offset, 'data': block})
                    byte_offset = bytes_offset + blocksize        
            
            # send the final data
            if data:
                com_queue_out.put({'type': 'data_received', 'byte_offset': byte_offset, 'data': data})
                    
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
        blockmap = BlockMap(remote_path, local_path, ftp_get_filesize)
        
        # loop until file is downloaded
        while not blockmap.is_blockmap_complete():
            # look for an idle download thread
            for i in range(0, self._max_download_threads):
                if self._get_download_thread_state(i) == 'IDLE':
                    worker_id = str(i)
                    byte_offset, blocks = blockmap.allocate_segment(worker_id)

            # check the incoming com queue for messages from the download threads
            

            # sleep
            time.sleep(1)
        
        # clean up the block map
        blockmap.delete_blockmap()





#     def _download_segment(self):
#         pass
#  
#     def _allocate_segment(self, block_map):
#         pass
#  
#     def _activate_download_thread(self, thread_id, segment):
#         pass
# 
# #     def _get_blockmap_for_file(self, remote_path, local_path):
# #         """ Retrieves the blockmap for a file.  The local_path is first checked for an existing blockmap.
# #             If there is an existing blockmap, it is assumed that this is a continuation of a previously aborted
# #             download.  If no blockmap exists at the local_path, a new blockmap is created and written to the 
# #             local_path location.
# #             
# #             The remote server must be contacted to retrieve the size of the remote file in
# #             order to create a new blockmap.
# #             
# #             Args:
# #                 remote_path - path to the remote file
# #                 local_path - file location to save the remote file to
# #             
# #             Returns:
# #                 blockmap as a string
# #         """
# #         # calculate the blockmap location based on the local_path
# #         blockmap_path = local_path + '.blockmap'
# #         
# #         # read the existing blockmap if it exists already
# #         if os.path.exists(blockmap_path):
# #             with open(blockmap_path, 'r') as f: 
# #                 return f.read()
# #         
# #         # calculate a new blockmap, query the filesize from the remote ftp server
# #         ftp = FTP(self._server_url)
# #         ftp.login(self._username, self._password)
# # #        ftp.prot_p()
# #         print ftp.retrlines('LIST')
# #         ftp.sendcmd("TYPE i")    # Switch to Binary mode
# #         size = ftp.size(remote_path)   # Get size of file
# #         print size        
# #         ftp.sendcmd("TYPE A")    # Switch to Binary mode
# #         blockmap = '.' * int(math.ceil(size / float(self._blocksize)))
# #         
# #         # write the new blocmap to disk
# #         with open(blockmap_path, 'w') as f:
# #             f.write(blockmap)
# #         
# #         # return the blockmap
# #         return blockmap


#     def _is_download_finished(self, blockmap):
#         """ return True if the blockmap shows that all blocks have been downloaded
#         
#             Args:
#                 blockmap - blockmap string
#             
#             Returns:
#                 True if the blockmap shows all blocks have been downloaded
#         """
#         return set(blockmap) == set('*')
    
