""" setup for superftp """
import setuptools

f = open('README.md')
long_description = f.read()
f.close()

setuptools.setup(
    name="superftp",
    version="1.0.3",
    description="Multisegment FTP Client",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/Snackman8/superftp",
    license="License :: OSI Approved :: Apache Software License 2.0 (Apache-2.0)",
    packages=setuptools.find_packages(),
    python_requires='>=2.7',
    entry_points={
        'console_scripts': ['superftp=superftp.superftp:main'],
    }
)
