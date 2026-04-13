# superftp
Fast multi-segment FTP client

Superftp maximizes download speed for large FTP transfers over long geographic distances. It splits a remote file into
segments, downloads those segments in parallel, and keeps track of what has already been saved so interrupted downloads
can resume safely.

Because each connection may take a different network path, segment speeds can vary widely. Superftp monitors those
connections, reallocates work as segments complete, and can restart slow segments to avoid getting stuck on poor routes.

In sum:

* Large files are segmented into small pieces.
* Each segment is downloaded in parallel.
* Superftp monitors the download rate on each segment.
* Each segment may route differently from the source.
* Superftp restarts segments which have been routed through slow connections.
* As segments complete, Superftp reassigns parallel downloads to
  remaining segments.
* Aborted, failed, or killed downloads can be resumed

### Installation

Superftp is Python 3 only.

Install from PyPI:

`pip3 install superftp`

Install the latest version directly from git without cloning first:

`pip3 install git+https://github.com/Snackman8/superftp.git`

After installation, the `superftp` command should be available in your environment:

`superftp --help`

### Quickstart

Download `/example.txt` from `ftpserver.example` into the current directory:

    superftp --server ftpserver.example --username anonymous --password password \
    --remote_path /example.txt

The argument specifiers also have short versions:

    superftp -s ftpserver.example -u anonymous -p password -rp /example.txt

Download into a specific local directory:

    superftp -s ftpserver.example -u anonymous -p password -rp /example.txt -lp ./downloads

Download a remote directory recursively:

    superftp -s ftpserver.example -u anonymous -p password -rp /pub/files -lp ./downloads

If a download is interrupted, rerun the same command to resume it:

    superftp -s ftpserver.example -u anonymous -p password -rp /bigfile.iso -lp ./downloads

To enable TLS encryption, add the `--enable_tls` flag:

    superftp -s ftpserver.example -u anonymous -p password -rp /example.txt --enable_tls

To use a compact single-line status display:

    superftp -s ftpserver.example -u anonymous -p password -rp /example.txt --display_mode compact

To start fresh and discard any existing local file and resume state:

    superftp -s ftpserver.example -u anonymous -p password -rp /example.txt --clean

Run the command with `-h` to see the full help.

### How Resume Works

Superftp stores resume state in a sidecar file named `<local file>.blockmap`.

* If a download is interrupted, rerunning the same command resumes from the saved blockmap.
* `--clean` removes the local file and its blockmap before downloading again.
* A partial local file without a matching blockmap is treated as a fresh download.

### Tuning

The defaults are intended to work well for general use, but a few options are worth knowing:

* `--connections` controls how many FTP connections download in parallel.
* `--blocksize` controls how much data each tracked block represents.
* `--kill_speed` sets the minimum average speed in MB/sec before a connection is restarted.
* `--display_mode` can be `quiet`, `compact`, or `full`.

For most users, the defaults are the right place to start. `--kill_speed` is mainly useful for long-distance transfers
where some connections occasionally route poorly.

### Dependencies

Superftp runtime does not require any third-party packages beyond the Python 3 standard library.

To run the unit tests:

`pyftpdlib==1.5.5`

### Release Notes ###
v1.0.4
* Added README instructions for direct `pip` installation from git.
* Documented Python 3-only support and deprecated Python 2.
* Clarified resume behavior, `--clean`, and common command examples.
* Improved download safety for worker failures and truncated transfers.
* Added validation for unsupported connection counts.

v1.0.3
* First official release
