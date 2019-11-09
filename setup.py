""" setup for superftp """
import setuptools

setuptools.setup(
    name="superftp",
    version="1.0.0",
    description="Multisegment FTP Client",
    url="https://github.com/Snackman8/superftp",
    license="License :: OSI Approved :: Apache Software License 2.0 (Apache-2.0)",
    packages=setuptools.find_packages(),
    python_requires='>=2.7',
    entry_points={
        'console_scripts': ['superftp=superftp.superftp:main'],
    }
)
