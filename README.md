# superftp
Fast multi-segment FTP client

This FTP client maximizes download speed for large files over 
long geographic distances.  The program splits the file into
segments. It then launches several download process, one for each segment.
The program monitors what parts of which segments have been downloaded.
Superftp monitors how fast each segment is downloading.  

Note that over the Internet, each download routes differently from the source to
destination differently, 
and so the download speeds will vary - 
especially as the geographic distance between 
the  server and client increases.
Superftp monitors the download speeds and kills slow downloads that
have been routed inefficiently, and then restarts them.  It keeps track
of what segments have been downloaded and does not redownload any
segments.

In sum:

* Large files are segmented into small pieces. 
* Each segment is downloaded in parallel.  
* Superftp monitors the download rate on each segment.
* Each segment routes differently from the source
* Superftp restarts segments which have been routed through slow connections.
* As segments complete, Superftp reassigns parallel downloads to
  remaining segments.
* Aborted, failed, or killed downloads can be resumed

### Installation

The easiest way to install is using pip

To install for python3 (preferred method)

`pip3 install superftp`

To install for python2

`pip2 install superftp`


### Quickstart

Download /example.txt from ftp server with address ftpserver.example, username of Anonymous, and password of password to the current directory.

    superftp --server ftpserver.example --username Anonymous --password password \
    --remote_path /example.txt

The argument specifiers also have short versions of -s, -u, -p, -rp

    superftp -s ftpserver.example -u Anonymous -p password -rp /example.txt

To enable TLS encryption add the --enable_tls flag

    superftp -s ftpserver.example -u Anonymous -p password -rp /example.txt --enable_tls

Run the superftp command with the -h option to see the help



### Dependencies
The superftp application and module does not require any additional dependencies outside the standard  libraries.
In order to run the unit tests, `pyftpdlib==1.5.5` is required



### Build superftp on a development machine

1. Clone the git repository
2. run the `build.sh` script in the root of the project, the build.sh script will do the following
    * clean the project
    * run pycodestyle on the project to check that best practice coding standards are followed
    * run pylint on the project to check that best practice coding standards are followed
    * run the unit tests for the project
    * generate documentation for the project (the generated documentation is available at `docs/_build/html/index.html`)
    * package the project into a redistributable, the redistributable is available in the `dist` directory in the root of the project



### Release Notes ###
v1.0.3
* First official release