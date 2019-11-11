# superftp
Fast multi-segment FTP client

#### Key Features
* Designed to download large files by breaking the file into multiple parts (segments) and downloading the parts in parallel
* Designed to recover from aborted, failed, or killed downloads by resuming previous downloads
* Kill speed feature will kill and restart slow connections to optimize faster internet packet routes
 
#### Quickstart

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
