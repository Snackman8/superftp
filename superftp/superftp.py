""" superftp - multi-segmend ftp downloader"""

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
import ftplib
import os
import sys
import traceback
from functools import partial
# disable pylint for relative-import below, no way to make it work with sphinx and nosetests and comply with pylint
if sys.version_info >= (3, 0):
    from .ftp_file_download_manager import FtpFileDownloader     # pylint: disable=W0403
else:
    from ftp_file_download_manager import FtpFileDownloader     # pylint: disable=W0403


# --------------------------------------------------
#    Constants
# --------------------------------------------------
ANSI_WHITE = '\033[37m'
ANSI_RED = '\033[91m'
ANSI_GREEN = '\033[92m'
ANSI_YELLOW = '\033[93m'

ANSI_CLEAR_REST_OF_LINE = '\033[K'
ANSI_CLEAR_REST_OF_SCREEN = '\033[J'

ANSI_MOVE = '\033[%d;%dH'


# --------------------------------------------------
#    Functions
# --------------------------------------------------
def _pretty_blockmap(blockmap, rows, cols):
    """ create a string which contains a pretty ANSI representation of the blockmap fitted to a window with the
        specified size of rols by cols

        Args:
            blockmap - blockmap instance to represent
            rows - rows to show
            cols - columns to show

        Returns:
            a pretty ANSI string
    """
    # calculate a scaling factor to make a large blockmap fit the window
    bstr = str(blockmap)
    scale = min(1.0, float(rows * cols) / float(len(bstr)))

    # loop across the rows
    s = ''
    last_ansi_color = None
    for y in range(0, rows):
        # loop across the cols
        for x in range(0, cols):
            # break if no more characters
            if (y * cols + x) >= len(bstr):
                break

            # get the character from the scaled blockmap
            c = bstr[int((y * cols + x) / scale)]

            # map to a color
            if c in blockmap.PENDING + blockmap.SAVING:
                if last_ansi_color != ANSI_YELLOW:
                    s = s + ANSI_YELLOW
                    last_ansi_color = ANSI_YELLOW

            elif c in blockmap.DOWNLOADED:
                if last_ansi_color != ANSI_GREEN:
                    s = s + ANSI_GREEN
                    last_ansi_color = ANSI_GREEN
            else:
                if last_ansi_color != ANSI_WHITE:
                    s = s + ANSI_WHITE
                    last_ansi_color = ANSI_WHITE

            # add the character
            s = s + c

        # newline
        s = s + ANSI_CLEAR_REST_OF_LINE + '\r\n'

    # return the pretty blockmap representation
    return s


def _pretty_summary_line(ftp_download_manager, blockmap, remote_filepath):
    """ construct a single line (no ansi) representation of the download state

        Args:
            ftp_download_manager - ftp_download_manager instance to show summary for
            blockmap - blockmap instance of the download to show summary for
            remote_file_path - the file path on the remote server of the file being downloaded

        Returns:
            string containing a single line summary of the download state
     """
    # calculate some numbers
    dl_speed = ftp_download_manager.total_dl_speed
    non_downloaded_blocks, _, total_blocks, _blocksize, eta = blockmap.get_statistics(dl_speed)
    percent_complete = (1.0 - (float(non_downloaded_blocks) / total_blocks)) * 100

    # display
    s = 'ETA:%-13s %5.1f%%  %0.3fMB/sec  ' % (eta, percent_complete, dl_speed / 1024 / 1024)
    s = s + remote_filepath[max(0, len(remote_filepath) - (79 - len(s))):]

    # return the pretty representation
    return s


def _pretty_dl_speed_fifo(ftp_download_manager, kill_speed):
    """ construct a pretty ansi representation of the download fifo speeds

        Args:
            ftp_download_manager - ftp_download_manager instance to show summary for
            kill_speed - shwo in red if under this speed in MB/sec

        Returns:
            string containing a pretty ansi representation of the download fifo speeds
    """
    # loop over each depth
    s = ''
    last_ansi_color = None
    for y in range(0, ftp_download_manager.SPEED_FIFO_SIZE):
        # loop across the workers
        for k in ftp_download_manager.worker_dl_speeds.keys():
            # get the speed, we want to show the oldest speed at the top so we traverse in reverse order
            speed = float(ftp_download_manager.worker_dl_speeds[k][::-1][y]) / 1024 / 1024

            # emit the color
            if speed == 0:
                if last_ansi_color != ANSI_WHITE:
                    s = s + ANSI_WHITE
                    last_ansi_color = ANSI_WHITE
            elif speed < kill_speed:
                if last_ansi_color != ANSI_RED:
                    s = s + ANSI_RED
                    last_ansi_color = ANSI_RED
            else:
                if last_ansi_color != ANSI_GREEN:
                    s = s + ANSI_GREEN
                    last_ansi_color = ANSI_GREEN

            # emit the speed
            s = s + '[%0.3f]' % speed

        # clear ther emainder of the line
        s = s + ANSI_CLEAR_REST_OF_LINE + '\n'

    # return the pretty representation
    return s


def _display_compact(ftp_download_manager, blockmap, remote_filepath):
    """ writes a one line compact status display of the current download to the screen.  Does not use ANSI
        characters

        Args:
            ftp_download_manager - ftp_download_manager instance to show summary for
            blockmap - blockmap instance of the download to show summary for
            remote_file_path - the file path on the remote server of the file being downloaded
    """
    s = _pretty_summary_line(ftp_download_manager, blockmap, remote_filepath)
    sys.stdout.write('\r%-79s' % s)
    sys.stdout.flush()


def _display_full(ftp_download_manager, blockmap, remote_filepath, force_window_size=None):
    """ writes a full screen detailed update of the current download to the screen.  Uses ANSI characters for color
        and cursor positioning

        Args:
            ftp_download_manager - ftp_download_manager instance to show summary for
            blockmap - blockmap instance of the download to show summary for
            remote_file_path - the file path on the remote server of the file being downloaded
    """
    # get the window size
    if force_window_size:
        rows, columns = force_window_size
    else:
        try:
            rows, columns = [int(x) for x in os.popen('stty size', 'r').read().split()]
        except ValueError as _:
            rows, columns = (24, 80)
    y = 1

    # show the pretty summary line
    s = ''
    s = s + ANSI_MOVE % (y, 0) + ANSI_WHITE
    s = s + _pretty_summary_line(ftp_download_manager, blockmap, remote_filepath) + ANSI_CLEAR_REST_OF_LINE
    s = s + ANSI_MOVE % (y + 1, 0) + ANSI_CLEAR_REST_OF_LINE

    # show the pretty download speed fifos
    y = 3
    s = s + ANSI_MOVE % (y, 0)
    s = s + _pretty_dl_speed_fifo(ftp_download_manager, ftp_download_manager.kill_speed)
    s = s + ANSI_MOVE % (y + ftp_download_manager.SPEED_FIFO_SIZE, 0) + ANSI_CLEAR_REST_OF_LINE

    # show the blockmap
    y = y + ftp_download_manager.SPEED_FIFO_SIZE + 1
    s = s + ANSI_MOVE % (y, 0)
    s = s + _pretty_blockmap(blockmap, rows - y - 1, columns)

    # clear the rest of the screen
    s = s + ANSI_CLEAR_REST_OF_SCREEN

    # write to screen
    sys.stdout.write(s)
    sys.stdout.flush()


# --------------------------------------------------
#    Event Handlers
# --------------------------------------------------
def _on_refresh_display(display_mode, ftp_download_manager, blockmap, remote_filepath):
    """ event handler for when the ftp handler would like to refresh the display

        Args:
            display_mode - mode to display on the screen, can be quiet, compact, or full
            ftp_download_manager - ftp_download_manager instance to show summary for
            blockmap - blockmap instance of the download to show summary for
            remote_file_path - the file path on the remote server of the file being downloaded
    """
    # update the display
    if display_mode == 'quiet':
        # no display when quiet
        return
    if display_mode == 'compact':
        _display_compact(ftp_download_manager, blockmap, remote_filepath)
    elif display_mode == 'full':
        _display_full(ftp_download_manager, blockmap, remote_filepath)
    else:
        raise Exception('Unknown display mode of %s' % display_mode)


# --------------------------------------------------
#    Main
# --------------------------------------------------
def _run(args):
    """ run function of the script

        Args:
            args - dictionary of arguments passed in from the command line
    """
    # create a new ftp downloader
    ftp_downloader = FtpFileDownloader(server_url=args['server'], username=args['username'], password=args['password'],
                                       port=args['port'], concurrent_connections=args['connections'],
                                       min_blocks_per_segment=args['min_blocks_per_segment'],
                                       max_blocks_per_segment=args['max_blocks_per_segment'],
                                       initial_blocksize=args['blocksize'],
                                       kill_speed=args['kill_speed'],
                                       clean=args['clean'],
                                       enable_tls=args['enable_tls'])

    # download
    ftp_downloader.on_refresh_display = partial(_on_refresh_display, args['display_mode'])
    try:
        ftp_downloader.download(args['remote_path'], args['local_path'])
    except KeyboardInterrupt:
        ftp_downloader.abort_download()
    except ftplib.error_perm as e:
        sys.stderr.write('\n' + 'FTP ERROR: ' + str(e) + '\n')
    except IOError as e:
        sys.stderr.write('\n' + str(e) + '\n')
        if args['debug']:
            sys.stderr.write(traceback.format_exc())

    sys.stdout.write(ANSI_WHITE + '\n')
    sys.stdout.flush()


def main():
    """ main function, handles parsing of arguments """
    parser = argparse.ArgumentParser(description=("Multi-segmented FTP downloader\n\nDownloads and FTP file using " +
                                                  "multiple threads concurrently."))
    parser.add_argument("-s", "--server", help="ftp server to connect to", required=True)
    parser.add_argument("-u", "--username", help="username to login with", default='anonymous')
    parser.add_argument("-p", "--password", help="password to login with", default='password')
    parser.add_argument("-rp", "--remote_path", help="location of file or directory on ftp server to download",
                        required=True)
    parser.add_argument("-lp", "--local_path", help="local location to save file to, can be a directory name",
                        default='.')

    parser.add_argument("--port", help="port number to use", type=int, default=21)
    parser.add_argument("--connections", help="number of concurrent connections to use", type=int, default=4)
    parser.add_argument("--min_blocks_per_segment",
                        help="minimum number of contigous 1MB blocks allocated per connection", type=int, default=8)
    parser.add_argument("--max_blocks_per_segment",
                        help="maximum number of contiguous 1MB blocks per connection", type=int, default=128)
    parser.add_argument("--display_mode", help="quiet, compact, full",
                        default='full')
    parser.add_argument("--clean", help="clean any existing downloaded files", action="store_true")
    parser.add_argument("--blocksize", help="size in bytes of each block to download", type=int, default=1024 * 1024)
    parser.add_argument("--kill_speed", help=("minimum speed in MB/sec a download thread must average or else it is" +
                                              " killed"),
                        type=float, default=1.0)
    parser.add_argument("--enable_tls", help="enable FTP TLS encryption", action="store_true")
    parser.add_argument("--debug", help="enable debug mode", action="store_true")

    args = parser.parse_args()
    _run(vars(args))


if __name__ == '__main__':  # pragma: no cover
    main()
