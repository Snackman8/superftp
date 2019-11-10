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
