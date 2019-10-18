# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
from ftp_file_download_manager import FtpFileDownloader


# --------------------------------------------------
#    Main
# --------------------------------------------------
def run(args):
    ftp_downloader = FtpFileDownloader(args['server'], args['username'], args['password'], args['connections'],
                                       args['port'], args['min_blocks_per_segment'], args['max_blocks_per_segment'])
    ftp_downloader.download_file(args['remote_path'], args['local_path'])


def main():
    parser = argparse.ArgumentParser(description=("Multi-segmented FTP downloader\n\nDownloads and FTP file using " +
                                                  "multiple threads concurrently."))
    parser.add_argument("--server", help="ftp server to connect to", required=True)
    parser.add_argument("--port", help="port number to use", type=int, default=21)
    parser.add_argument("--username", help="username to login with", default='anonymous')
    parser.add_argument("--password", help="password to login with", default='password')
    parser.add_argument("--remote_path", help="location of file or directory on ftp server to download", required=True)
    parser.add_argument("--local_path", help="local location to save file to, can be a directory name", default='.')
    parser.add_argument("--connections", help="number of concurrent connections to use", type=int, default=4)
    parser.add_argument("--min_blocks_per_segment",
                        help="minimum number of contigous 1MB blocks allocated per connection", type=int, default=8)
    parser.add_argument("--max_blocks_per_segment",
                        help="maximum number of contiguous 1MB blocks per connection", type=int, default=128)
    parser.add_argument("--quiet", help="quiet mode, no console output", action="store_true")
    args = parser.parse_args()
    run(vars(args))


if __name__ == '__main__':
    main()
