#from distutils.core import setup
from setuptools import setup

setup(name='webhdfs',
      version='0.0.1',
      author='Diwanshu Shekhar',
      author_email='diwanshu@gmail.com',
      url='https://github.com/DiwanshuShekhar/webhdfs',
      description ='Python package to perform file operations on Kerberized HDFS',
      long_description='NA',
      requires=['webhdfs', 'webhdfs.requests_kerberos'],
      packages=['webhdfs', 'webhdfs.requests_kerberos'],
      install_requires=[
        'requests>=1.1.0',
        'cryptography>=1.3;python_version!="3.3"',
        'cryptography>=1.3,<2;python_version=="3.3"'
      ],
      extras_require={
        ':sys_platform=="win32"': ['winkerberos>=0.5.0'],
        ':sys_platform!="win32"': ['pykerberos>=1.1.8,<2.0.0'],
      },
)
