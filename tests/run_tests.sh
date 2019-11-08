pycodestyle --max-line-length=120 .
(cd superftp && find . -iname "*.py" | grep -v ./docs/conf.py | xargs pylint --max-line-length=120 --good-names=a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,y,v,w,x,y,z --disable=R0902,R0913,R0914)
nosetests --with-coverage --cover-inclusive --cover-package=superftp --cover-html
(cd docs && make clean html)
python setup.py sdist
