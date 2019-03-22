from distutils.core import setup

setup(name='webhdfs',
      version='0.0.1',
      author='Diwanshu Shekhar',
      author_email='diwanshu@gmail.com',
      url='https://github.com/DiwanshuShekhar/webhdfs',
      description ='Python package to perform file operations on Kerberized HDFS',
      long_description='NA',
      requires=['webhdfs', 'webhdfs.requests_kerberos'],
      packages=['webhdfs', 'webhdfs.requests_kerberos']
      )
