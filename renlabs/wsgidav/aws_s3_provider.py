# (c) 2020 Steve Work; redistribution granted per MIT License
# (http://github.com/swork/wsgidav/LICENSE)

# Derived from wsgidav, which carries this notice:
# (c) 2009-2020 Martin Wendt and contributors;
# see WsgiDAV https://github.com/mar10/wsgidav
# Original PyFileServer (c) 2005 Ho Chun Wei.
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license.php

"""Implementation of a wsgidav DAVProvider that serves uses AWS resources as a
backing store (S3 buckets). Supports only a low bar for performance, as the
underlying AWS S3 operations often don't line up well with assumptions one
might make about a filesystem implementation (and can be slow themselves).

The ability to deploy in AWS Lambda is a primary design goal, implying zero state
maintained between requests. Performance necessarily suffers.

Files in an S3 bucket are "objects" with "keys" instead of "paths". There is no
concept of folders/directories in S3; instead there is a convention that uses
slashes in keys to clue UI consoles into presenting a hierarchical structure of
keys. The S3 operation that best corresponds to enumerating a WebDAV folder is
GetObjectNames with a prefix that ends with '/'. The results include *all*
objects matching that prefix - including objects with keys whose names contain
further slashes, which in a hierarchical filesystem would be children of a
subfolder. Our enumeration must retrieve these subfolder contents and then
filter them out. Performance necessarily suffers.

Similarly, using zero-length objects as proxies for directories (and containers
for metadata) means extra checks for object key existence - and non-existence,
since a directory proxy must not be accompanied by an object keyed similarly
but without a trailing slash. The AWS Lambda requirement prevents us from
maintaining state between requests, so performance necessarily suffers.

Object read is done by means of a get_object call, the return from which is an
object that sort-of implements the Python iterator protocol for file reading.
This stream isn't seekable, so Range requests are supported by streaming in and
discarding leading bytes outside the range. Performance necessarily suffers.

Object WRITE (and PATCH when implemented) is fully memory-buffered (so far):
the entire new object comes into RAM, and is handed to the S3 API as a bytes
sequence. It may be possible to implement a chunk-buffering class to wrap an
incoming file-like stream object in a way that avoids the buffering, but this
work is TODO. Intermediate ground would be file-buffering the incoming stream.
Range writes (unimplemented as I write) will involve retrieving the entire
object, patching and then sending it back as bytes. Performance necessarily
suffers.

1. The root folder always exists, represented by an object with the key '/'.

2. Files in a directory are distinguished from subdirectories:

   - File object keys never end in slash. Directory object keys always end in
     slash.
   - Both file and directory object keys always begin with a slash.
   - Two slashes never appear in succession.
   - For every non-terminal slash in every object key (file or directory) there
     exists a corresponding directory object, keyed with the same sequence of
     bytes up to and including that slash.
   - Directory objects are always of length zero.
   - No directory object is allowed to end with two slashes ('//').
   - No file object is allowed to exist whose key is the same as a directory
     object key save the trailing slash.

3. Metadata TODO

4. Locking specialization (required?) TODO

TODO :class:`~wsgidav.aws_dav_provider.AWSS3Provider` implements a DAV resource
provider that publishes an S3 bucket WITH CONTENTS CONFORMING TO THIS
SPECIFICATION. ()It does NOT provide general access to an existing S3 bucket.)

If ``readonly=True`` is passed, write attempts will raise HTTP_FORBIDDEN.

"""

import os
import shutil
import stat
import sys
import io

import boto3, botocore

from wsgidav import compat, util
from wsgidav.dav_error import (
    DAVError, HTTP_FORBIDDEN, HTTP_NOT_FOUND, HTTP_METHOD_NOT_ALLOWED)
from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider

__docformat__ = "reStructuredText"

_logger = util.get_module_logger(__name__)

BUFFER_SIZE = 8192

class FileContentGatherer(io.BytesIO):
    """Override io.BytesIO.close(), deferring to __del__

    BytesIO instances toss their contents on .close(). wsgidav asks us to
    supply a writable file-like to receive a request body but calls .close() on
    it before notifying us it's done, so the value is lost before we can read
    it. This wrapper lets us retrieve the value before it is lost.
    """
    def close(self):
        pass
    def __del__(self):
        super(FileContentGatherer, self).close()


class StreamingBodyWrapper:
    """Mix a forward-only .seek() into a readable stream."""
    def __init__(self, sb):
        assert isinstance(sb, botocore.response.StreamingBody)
        self.sb = sb
        self.pointer = 0

    def __del__(self):
        self.sb.close()

    def read(self, size=-1):
        if size <= 0:
            r = self.sb.read()
        else:
            r = self.sb.read(amt=size)
        self.pointer += len(r)
        return r

    def __getattr__(self, name):
        _logger.warning(f'ga({name!r}) was called unexpectedly')
        raise AttributeError(name)

    def seek(self, position):
        assert position >= self.pointer
        r = self.sb.read(amt=position)
        position -= len(r)
        while position:
            r = self.sb.read(amt=position)
            position -= len(r)

    def close(self):
        return self.sb.close()


class FileObjectResource(DAVNonCollection):
    """Represents a single existing DAV resource instance: an object in an S3
    bucket.

    See also _DAVResource, DAVNonCollection, and FilesystemProvider.
    """
    _s3Client = None

    @classmethod
    def setUpClass(cls, s3Client):
        assert cls._s3Client is None
        cls._s3Client = s3Client

    @property
    def s3Client(self):
        if self._s3Client is None:
            self._s3Client = boto3.client('s3')
        return self._s3Client

    def __init__(self, environ, root_prefix, listing):
        self.listing_item = listing['Contents'][0]
        key = self.listing_item['Key']
        rplen = len(root_prefix)
        discardRoot, self.davPath = (key[:rplen], key[rplen-1:])
        assert discardRoot == root_prefix
        assert self.davPath[0] == '/'
        super(FileObjectResource, self).__init__(self.davPath, environ)
        self._content_source = None  # botocore StreamingBody instance
        self._content_sink = None  # FileContentGatherer instance
        self._content_sink_type = None

    # Getter methods for standard live properties
    def get_content_length(self):
        return int(self.listing_item['Size'])

    def get_content_type(self):
        return self.listing_item.get('ContentType',
                                     util.guess_mime_type(self.davPath))

    def get_creation_date(self):
        _logger.warning('Substituting LastModified for creation_date')
        return self.get_last_modified()

    def get_display_name(self):
        return os.path.split(self.davPath)[1]

    def get_etag(self):
        return self.listing_item['ETag']

    def get_last_modified(self):
        return self.listing_item['LastModified'].utcnow().timestamp()

    def support_etag(self):
        return True

    def support_ranges(self):
        return True

    def get_content(self):
        """Open content as a stream for reading.

        See DAVResource.get_content()
        """
        assert not self.is_collection
        # GC issue 28, 57: if we open in text mode, \r\n is converted to one byte.
        # So the file size reported by Windows differs from len(..), thus
        # content-length will be wrong.
        if self._content_sink:
            raise RuntimeError("get_content while writing?")
        response = self.s3Client.get_object(
            Bucket=self.provider.bucket,
            Key=self.listing_item['Key'])
        return StreamingBodyWrapper(response['Body'])

    def begin_write(self, content_type=None):
        """Open content as a stream for writing.

        See DAVResource.begin_write()

        This implementation is purposefully awful: buffers everything then
        writes. Meets the current lazy requirements, doesn't require anything
        special of S3. FileContentsGatherer is required since caller closes the
        returned file-like before notifying us through .end_write(), losing the
        content.
        """
        assert not self.is_collection
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        if self._content_source:
            raise RuntimeError("begin_write while reading?")
        _logger.debug(f"begin_write: {self.davPath} type {content_type!r}")
        self._content_sink_type = content_type
        self._content_sink = FileContentGatherer()
        return self._content_sink

    def end_write(self, hasErrors):
        if self._content_source:
            raise RuntimeError("end_write while reading?")
        if not self._content_sink:
            raise RuntimeError("end_write while not writing?")
        _logger.info(f"end_write(hasErrors:{hasErrors}): {self.davPath}")
        if hasErrors:
            self._content_sink = None  # toss body
            self._content_sink_type = None
            return
        # else
        content = self._content_sink.getvalue()
        response = self.s3Client.put_object(
            Bucket=self.provider.bucket,
            Key=self.provider.root_prefix + self.davPath[1:],
            Body=self._content_sink.getvalue(),
            ContentType=self._content_sink_type
        )
        self._content_sink = None
        self._content_sink_type = None
        _logger.info(f'end_write wrote {len(content)} to {self.provider.bucket}:{self.davPath}')

    def delete(self):
        """Remove this resource or collection (recursive).

        See DAVResource.delete()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        response = self.s3Client.delete_object(
            Bucket=self.provider.bucket,
            Key=self.provider.root_prefix + self.davPath[1:])
        _logger.info(f'delete:{self.davPath!r} response:{response!r}')

    def copy_move_single(self, dest_davPath, is_move):
        """See DAVResource.copy_move_single() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        assert dest_davPath[0] == '/'
        assert not dest_davPath[-1] != '/'
        assert not util.is_equal_or_child_uri(self.davPath, dest_davPath)
        self.s3Client.copy_object(
            Bucket=self.provider.bucket,
            CopySource=self.provider.root_prefix + self.davPath[1:],
            Key=self.provider.root_prefix + dest_davPath[1:])
        if is_move:
            self.delete()
        # # Copy file (overwrite, if exists)
        # shutil.copy2(self._file_path, fpDest)
        # # (Live properties are copied by copy2 or copystat)
        # # Copy dead properties
        # propMan = self.provider.prop_manager
        # if propMan:
        #     destRes = self.provider.get_resource_inst(dest_path, self.environ)
        #     if is_move:
        #         propMan.move_properties(
        #             self.get_ref_url(),
        #             destRes.get_ref_url(),
        #             with_children=False,
        #             environ=self.environ,
        #         )
        #     else:
        #         propMan.copy_properties(
        #             self.get_ref_url(), destRes.get_ref_url(), self.environ
        #         )

    def support_recursive_move(self, dest_path):
        """Return True, if move_recursive() is available (see comments there)."""
        return False

    # def move_recursive(self, dest_path):
    #     """See DAVResource.move_recursive() """
    #     if self.provider.readonly:
    #         raise DAVError(HTTP_FORBIDDEN)
    #     fpDest = self.provider._loc_to_file_path(dest_path, self.environ)
    #     assert not util.is_equal_or_child_uri(self.path, dest_path)
    #     assert not os.path.exists(fpDest)
    #     _logger.debug("move_recursive({}, {})".format(self._file_path, fpDest))
    #     shutil.move(self._file_path, fpDest)
    #     # (Live properties are copied by copy2 or copystat)
    #     # Move dead properties
    #     if self.provider.prop_manager:
    #         destRes = self.provider.get_resource_inst(dest_path, self.environ)
    #         self.provider.prop_manager.move_properties(
    #             self.get_ref_url(),
    #             destRes.get_ref_url(),
    #             with_children=True,
    #             environ=self.environ,
    #         )

    def set_last_modified(self, dest_path, time_stamp, dry_run):
        """Set last modified time for destPath to timeStamp on epoch-format"""
        # Translate time from RFC 1123 to seconds since epoch format
        _logger.warning(f"Ignorning set_last_modified for {dest_path}")
        return True
        # secs = util.parse_time_string(time_stamp)
        # if not dry_run:
        #     os.utime(self._file_path, (secs, secs))
        # return True


class DirObjectResource(DAVCollection):
    """Represents a single existing file system folder DAV resource.

    See also _DAVResource, DAVCollection, and FilesystemProvider.
    """
    _s3Client = None

    @classmethod
    def setUpClass(cls, s3Client):
        assert cls._s3Client is None
        cls._s3Client = s3Client

    @property
    def s3Client(self):
        if self._s3Client is None:
            self._s3Client = boto3.client('s3')
        return self._s3Client

    def __init__(self, environ, root_prefix, listing):
        self.environ = environ
        self.listing = listing
        assert len(listing['Contents']) == 1
        key = listing['Contents'][0]['Key']
        rplen = len(root_prefix)
        discardRoot, self.davPath = (key[:rplen], key[rplen-1:])
        assert discardRoot == root_prefix
        assert self.davPath[0] == '/'
        assert self.davPath[-1] == '/'
        super(DirObjectResource, self).__init__(self.davPath, environ)
        _logger.debug(f'{self!r}')

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.provider.bucket}:{self.davPath}>'

    def childNameToKey(self, child):
        if '/' in child[:-1]:
            raise RuntimeError(f'multi-level child? {child!r}')
        return self.provider.root_prefix + self.davPath[1:] + child

    # Getter methods for standard live properties
    def get_creation_date(self):
        return 0

    def get_display_name(self):
        return self.davPath

    def get_directory_info(self):
        return None

    def get_etag(self):
        return None

    def get_last_modified(self):
        return self.listing['Contents'][0]['LastModified'].timestamp()

    def get_member_names(self):
        """Return list of direct collection member names (utf-8 encoded).

        See DAVCollection.get_member_names()
        """
        nameList = []
        assert self.davPath[0] == '/'
        start = self.provider.root_prefix + self.davPath[1:]
        startLen = len(start)
        rootLen = len(self.provider.root_prefix)
        response = self.s3Client.list_objects_v2(
            Bucket=self.provider.bucket,
            StartAfter=start,
            Prefix=start)
        _logger.debug(f'get_member_names in {self.davPath} start:{start} response:{response!r}')
        nameList.extend(
            filter(lambda x: x.count('/') <= 1,
                   map(lambda x: x['Key'][startLen:],
                       response.get('Contents', []))))
        while response.get('IsTruncated'):
            response = self.s3Client.list_objects_v2(
                ContinuationToken=response['NextContinuationToken'])
            nameList.extend(
                filter(lambda x: x.count('/') <= 1,
                       map(lambda x: x['Key'][startLen:],
                           response.get('Contents', []))))
        return nameList

    def get_member(self, baseName):
        """Return direct collection member (DAVResource or derived).

        See DAVCollection.get_member()
        """
        _logger.debug(f'{self.__class__.__name__}.get_member(baseName:{baseName!r}) in {self.davPath}')
        assert '/' not in baseName[:-1]
        return self.provider.get_resource_inst(
            self.davPath + baseName,
            self.environ)

    def create_empty_resource(self, name):
        """Create an empty (length-0) resource.

        See DAVResource.create_empty_resource()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        assert "/" not in name
        key = self.provider.root_prefix + self.davPath[1:] + name
        self.s3Client.put_object(
            Bucket=self.provider.bucket,
            Key=key,
            Body=b''
        )
        return self.provider.get_resource_inst(
            self.davPath + name,
            self.environ)

    def create_collection(self, baseName):
        """Create a new collection as member of self.

        See DAVResource.create_collection()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        assert '/' not in baseName
        _logger.debug(f'create_collection baseName:{baseName!r}')

        plainKey = self.davPath + baseName
        plain = self.provider.get_resource_inst(plainKey, self.environ)
        _logger.debug(f'create_collection   plainKey:{plainKey} found:{plain!r}')
        if plain:
            raise DAVError(HTTP_METHOD_NOT_ALLOWED)

        key = self.provider.root_prefix + self.davPath[1:] + baseName + '/'
        self.s3Client.put_object(
            Bucket=self.provider.bucket,
            Key=key,
            Body=b'')
        return self.provider.get_resource_inst(
            self.davPath + baseName + '/',
            self.environ)

    def handle_delete(self):
        _logger.debug(f'handle_delete...')
        self.delete()
        return True

    def delete(self):
        """Remove this resource or collection (recursive).

        See DAVResource.delete()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        _logger.debug(f'{self.__class__.__name__}.delete {self.davPath!r}')
        k = self.provider.root_prefix + self.davPath[1:]
        try:
            response = self.s3Client.list_objects_v2(
                Bucket=self.provider.bucket,
                Prefix=k)
            for delkey in sorted(
                    map(lambda x: x['Key'],
                        response.get('Contents', []))):
                _logger.debug(f'{self.__class__.__name__}.delete   subobj key:{delkey!r}')
                self.s3Client.delete_object(
                    Bucket=self.provider.bucket,
                    Key=delkey)
        except Exception as e:
            _logger.exception(f'delete in {self.davPath!r} suffered an exception')
            raise

    def support_recursive_delete(self):
        return True

    def copy_move_single(self, dest_path, is_move):
        """See DAVResource.copy_move_single() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        raise DAVError(HTTP_FORBIDDEN)

    def support_recursive_move(self, dest_path):
        return False

    def move_recursive(self, dest_path):
        """See DAVResource.move_recursive() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        raise DAVError(HTTP_FORBIDDEN)

    def set_last_modified(self, dest_path, time_stamp, dry_run):
        """Set last modified time for destPath to timeStamp on epoch-format"""
        # Translate time from RFC 1123 to seconds since epoch format
        _logger.warn(f"Ignoring set_last_modified for {dest_path}")
        return True


class AWSS3Provider(DAVProvider):
    """Implement wsgidav provider over a configured S3 bucket

    Suitable for AWS Lambda deployment
    """
    S3CLIENT = None
    ROOT_PREFIX = None  # flags retrieveRoot() on first instantiation
    BUCKET = None
    ROOT_LISTING = None

    @classmethod
    def retrieveRoot(cls, bucket, root_prefix):
        """Cache the root dir node"""
        assert cls.S3CLIENT is None
        assert cls.BUCKET is None
        assert cls.ROOT_PREFIX is None
        assert cls.ROOT_LISTING is None
        cls.S3CLIENT = boto3.client('s3')
        if root_prefix[-1] != '/':
            raise RuntimeError(f"config item root_prefix:{root_prefix!r} must end in a slash")
        response = cls.S3CLIENT.list_objects_v2(Bucket=bucket,
                                                Prefix=root_prefix,
                                                MaxKeys=1)
        _logger.info(f'AWSS3Provider instance over {bucket}:{root_prefix}'
                     + f' response:{response!r}')
        if len(response.get('Contents', [])) == 0:
            response = cls.setUpRoot(bucket, root_prefix)
        assert response['Contents'][0]['Key'] == root_prefix
        cls.BUCKET = bucket
        cls.ROOT_LISTING = response
        cls.ROOT_PREFIX = root_prefix
        FileObjectResource.setUpClass(s3Client=cls.S3CLIENT)
        DirObjectResource.setUpClass(s3Client=cls.S3CLIENT)

    @classmethod
    def setUpRoot(cls, bucket, root_prefix):
        """Create root dir object on first-time use of this bucket:prefix"""
        response = cls.S3CLIENT.put_object(
            Bucket=bucket,
            Key=root_prefix,
            Body=b'')  # metadata: permissions? Create-date?
        _logger.warn(f'{cls.__name__}.setUpRoot first-time use of bucket:{bucket!r} root:{root_prefix!r}')
        response = cls.S3CLIENT.list_objects_v2(
            Bucket=bucket,
            Prefix=root_prefix,
            MaxKeys=1)
        return response

    @property
    def bucket(self):
        assert self.BUCKET is not None
        assert self.ROOT_PREFIX is not None
        assert self.ROOT_LISTING is not None
        return self.BUCKET

    @property
    def root_listing(self):
        assert self.BUCKET is not None
        assert self.ROOT_PREFIX is not None
        assert self.ROOT_LISTING is not None
        return self.ROOT_LISTING

    @property
    def root_prefix(self):
        assert self.BUCKET is not None
        assert self.ROOT_PREFIX is not None
        assert self.ROOT_LISTING is not None
        return self.ROOT_PREFIX

    def __init__(self, bucket, root_prefix='', readonly=False):
        if self.ROOT_PREFIX is None:
            self.retrieveRoot(bucket, root_prefix)
        else:
            if root_prefix != self.root_prefix:
                _logger.error(f'root_prefix parameter to {self.__class__.__name__}'
                              + f' must not change'
                              + f'({root_prefix!r} vs {self._root_prefix!r})')
        self.readonly = readonly
        super(AWSS3Provider, self).__init__()

    def __repr__(self):
        rw = "Read-Write"
        if self.readonly:
            rw = "Read-Only"
        n = self.__class__.__name__
        return f"<{n} {self.BUCKET}:{self.ROOT_PREFIX} {rw}>"

    def is_readonly(self):
        return self.readonly

    def get_resource_inst(self, davPath, environ):
        """Return ...Resource obj for davPath.
        See DAVProvider.get_resource_inst()
        """
        self._count_get_resource_inst += 1

        if len(davPath) == 0:
            raise DAVError(HTTP_NOT_FOUND)
        if davPath[0] != '/':
            return None
        if '//' in davPath:
            return None

        if davPath == '/':
            listing = self.ROOT_LISTING
        else:
            key = self.ROOT_PREFIX + davPath[1:]
            listing = self.S3CLIENT.list_objects_v2(
                Bucket=self.BUCKET,
                Prefix=key,
                MaxKeys=1)
            if not 'Contents' in listing:
                return None
            if listing['Contents'][0]['Key'] != key:
                return None
        _logger.debug(f'{self.__class__.__name__}:get_resource_inst({davPath!r}) listing:{listing!r}')

        if davPath[-1] == '/':
            return DirObjectResource(environ, self.ROOT_PREFIX, listing)
        # else
        return FileObjectResource(environ, self.ROOT_PREFIX, listing)
