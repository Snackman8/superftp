""" superftp - multi-segmend ftp downloader"""

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
import os
import sys
from functools import partial
from ftp_file_download_manager import FtpFileDownloader


# --------------------------------------------------
#    Functions
# --------------------------------------------------
def calculate_download_statistics(ftp_download_manager, blockmap):
    """ calculate the download statistics, this is shared by all of the display functions

        Args:
            ftp_download_manager - the ftp_download_manager
            blockmap - the blockmap

        Returns
            eta, percent_complete, download_speed
    """
    # calculate some common parameters
    (available_blocks, total_blocks, blocksize) = blockmap.get_statistics()

    # calculate download_speed
    download_speed = 0
    for i in range(0, ftp_download_manager.concurrent_connections):
        worker_id = format(i, 'x')
        download_speed = (download_speed +
                          sum(ftp_download_manager.worker_dl_speeds[worker_id]) /
                          len(ftp_download_manager.worker_dl_speeds[worker_id]))

    # calculate % complete
    percent_complete = float(total_blocks - available_blocks) / total_blocks * 100

    # calculate ETA
    if download_speed == 0:
        if percent_complete == 100:
            eta = 'done'
        else:
            eta = 'infinite'
    else:
        eta = (available_blocks * blocksize) / download_speed
        if eta < 120:
            eta = '%d seconds' % eta
        else:
            eta = '%0.1f minutes' % (eta / 60)

    # success
    return (eta, percent_complete, download_speed)


def display_compact(ftp_download_manager, blockmap, remote_filepath):
    """ display compact screen update on one line """
    eta, percent_complete, download_speed = calculate_download_statistics(ftp_download_manager, blockmap)

    s = 'ETA:%-13s %0.1f%%  %0.3fMB/sec  ' % (eta, percent_complete, download_speed / 1024 / 1024)
    s = s + remote_filepath[max(0, len(remote_filepath) - (79 - len(s))):]
    sys.stdout.write('\r%-79s' % s)
    sys.stdout.flush()


def display_full(_no_ascii, ftp_download_manager, blockmap, remote_filepath):
    """ display full screen update """
    rows, columns = os.popen('stty size', 'r').read().split()
    rows = int(rows)
    columns = int(columns)

    eta, percent_complete, download_speed = calculate_download_statistics(ftp_download_manager, blockmap)

    # move to top of screen and emit the summary line
    sys.stdout.write('\033[0;0H')
    s = 'ETA:%-13s %0.1f%%  %0.3fMB/sec  ' % (eta, percent_complete, download_speed / 1024 / 1024)
    s = s + remote_filepath[max(0, len(remote_filepath) - (columns - len(s))):]
    sys.stdout.write(('%%-%ds' % columns) % s)
    # clear the remainder of the line
    sys.stdout.write('\033[K')

    # blank line
    sys.stdout.write('\033[2;0H\033[K')

    # show the speed fifos
    fifo_depth = 0
    dlspeeds = ftp_download_manager.worker_dl_speeds
    for i in range(0, ftp_download_manager.concurrent_connections):
        worker_id = format(i, 'x')
        fifo_depth = len(dlspeeds[worker_id])
        for j in range(0, fifo_depth):
            # move to cursor position
            sys.stdout.write('\033[%d;%dH' % (j + 3, i * 10))
            # print the download speed
            sys.stdout.write('[%0.3f]' % (dlspeeds[worker_id][-(j + 1)] / 1024 / 1024))
            # clear the remainder of the line
            sys.stdout.write('\033[K')

    # blank line
    y = fifo_depth + 3
    sys.stdout.write('\033[%d;0H\033[K' % y)

    # show the blockmap
    y = fifo_depth + 4
    s = str(blockmap)
    cells = (rows - y) * columns
    if len(s) > cells:
        blockmap_str = s
        ratio = float(len(blockmap_str)) / cells
        s = ''
        for i in range(0, cells):
            s = s + blockmap_str[int(i * ratio)]

    for i in range(0, rows - y):
        sys.stdout.write('\033[%d;%dH' % (y + i, 0))
        sys.stdout.write(s[i * columns: (i + 1) * columns] + '\033[K')
        if (i + 1) * columns > len(s):
            break

    # clear the rest of the screen
    sys.stdout.write('\033[J')
    sys.stdout.flush()


# --------------------------------------------------
#    Event Handlers
# --------------------------------------------------
def on_block_downloaded(display_mode, noascii, *_args, **kwargs):
    """ event handler for when a block has been downloaded, used to refresh the screen

        Args:
            display_mode - mode to display on the screen
            noascii - if true, do not use ANSI ascii formatting characters
            args - None
            kwargs - ftp_download_mananger, blockmap, remote_filepath
    """
    if display_mode == 'quiet':
        # no display when quiet
        return
    elif display_mode == 'compact':
        display_compact(kwargs['ftp_download_manager'], kwargs['blockmap'], kwargs['remote_filepath'])
    elif display_mode == 'full':
        display_full(noascii, kwargs['ftp_download_manager'], kwargs['blockmap'], kwargs['remote_filepath'])
    else:
        raise Exception('Unknown display mode of %s' % display_mode)


# --------------------------------------------------
#    Main
# --------------------------------------------------
def run(args):
    """ run function of the script

        Args:
            args - dictionary of arguments passed in from the command line
    """
    # clean if needed
    if args['clean']:
        FtpFileDownloader.clean_local_file(args['remote_path'], args['local_path'])

    # create a new ftp downloader
    ftp_downloader = FtpFileDownloader(args['server'], args['username'], args['password'], args['connections'],
                                       args['port'], args['min_blocks_per_segment'], args['max_blocks_per_segment'],
                                       args['blocksize'])

    # download
    ftp_downloader.on_block_downloaded = partial(on_block_downloaded, args['display_mode'], args['noascii'])
    try:
        ftp_downloader.download_file(args['remote_path'], args['local_path'])
    except KeyboardInterrupt:
        ftp_downloader.abort_download()
    print


def main():
    """ main function, handles parsing of arguments """
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
    parser.add_argument("--display_mode", help="quiet, compact, full",
                        default='full')
    parser.add_argument("--noascii", help="do not use ascii terminal", action="store_true")
    parser.add_argument("--clean", help="clean any existing downloaded files", action="store_true")
    parser.add_argument("--blocksize", help="size in bytes of each block to download", type=int, default=1024 * 1024)
    args = parser.parse_args()
    run(vars(args))


if __name__ == '__main__':
    main()
