# About webhdfs

Uses Apache WebHDFS protocol to perform  file operations between unix and HDFS.
This package also supports kerberos authentication the implementation of which is a fork of 
https://github.com/requests/requests-kerberos

The library uses requests library to make http calls to HDFS

## How to install the package

Download the package and cd to the installation directory and run:
```
python setup.py install
```

## How to use the package

```python
  from webhdfs.hdfs import WebHDFSClient

  client = WebHDFSClient('http://node.example.com:14000/webhdfs/v1', 
                          auth='use_kerberos')

  client.get_file('hdfs_file_path', 'unix_file_path')

  client.put_file('unix_file_path', 'hdfs_file_path')
```
