pycodestyle --max-line-length=120 .
find . -iname "*.py" | xargs pylint --max-line-length=120 --good-names=a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,y,v,w,x,y,z --disable=R0902,R0913,R0914
nosetests --with-coverage --cover-inclusive --cover-package=blockmap,superftp,ftp_file_download_manager --cover-html

