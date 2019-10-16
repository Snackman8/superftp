import argparse
from ftp_file_download_manager import FtpFileDownloader

def run(args):
    ftp_downloader = FtpFileDownloader(args['server'], args['username'], args['password'], args['connections'])
    ftp_downloader.download_file(args['remote_path'], args['local_path'])            
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", help="ftp server to connect to", required=True)
    parser.add_argument("--port", help="port number to use", type=int, default=21)
    parser.add_argument("--username", help="username to login with", default='anonymous')
    parser.add_argument("--password", help="password to login with", default='password')
    parser.add_argument("--remote_path", help="location of file or directory on ftp server to download", required=True)
    parser.add_argument("--local_path", help="local location to save file to, can be a directory name", default='.')
    parser.add_argument("--connections", help="number of concurrent connections to use", default=4)
    args = parser.parse_args()
    run(vars(args))

if __name__ == '__main__':
    main()