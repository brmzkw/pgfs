import argparse
import errno
import logging
import os
import stat

import psycopg2

from fuse import FUSE, FuseOSError, LoggingMixIn, Operations


class PostgresFS(LoggingMixIn, Operations):
    def __init__(self, dsn):
        self.fds = []
        self.dsn = dsn

    def _execute(self, query, params=None):
        try:
            self.db = psycopg2.connect(self.dsn)
            self.cursor = self.db.cursor()
            self.cursor.execute(query, params)
            self.db.commit()
            self.cursor.close()
            self.db.close()
            return self.cursor
        except: # reconnect, ugly
            self.db = psycopg2.connect(self.dsn)
            self.cursor = self.db.cursor()
            self.cursor.execute(query, params)
            self.db.commit()
            self.cursor.close()
            self.db.close()
            return self.cursor

    def getattr(self, path, fh=None):
        if path == '/':
            return super().getattr(path, fh=fh)

        self._execute("SELECT is_dir, size, ctime, mtime FROM fs WHERE path = %s", (path,))
        result = self.cursor.fetchone()
        if result is None:
            raise FuseOSError(errno.ENOENT)

        is_dir, size, ctime, mtime = result
        mode = stat.S_IFDIR | 0o755 if is_dir else stat.S_IFREG | 0o644
        st = dict(st_mode=mode, st_nlink=2, st_size=size, st_ctime=ctime.timestamp(), st_mtime=mtime.timestamp(), st_atime=mtime.timestamp())
        return st

    def create(self, path, mode, fi=None):
        self._execute("INSERT INTO fs (path, is_dir, data, size) VALUES (%s, FALSE, %s, 0)", (path, psycopg2.Binary(b'')))
        return 0

    def readdir(self, path, fh):
        self._execute("SELECT path FROM fs WHERE path LIKE %s", (f'{path.rstrip("/")}/%',))
        dirents = ['.', '..']
        dirents.extend([os.path.basename(row[0]) for row in self.cursor.fetchall()])
        return dirents

    def read(self, path, size, offset, fh):
        self._execute("SELECT data FROM fs WHERE path = %s", (path,))
        result = self.cursor.fetchone()
        if result is None:
            raise FuseOSError(errno.ENOENT)
        data = result[0].tobytes()
        return data[offset:offset + size]

    def write(self, path, data, offset, fh):
        current_data = self.read(path, 1<<32, 0, fh)  # Large size to read all
        if isinstance(current_data, memoryview):
            current_data = current_data.tobytes()
        new_data = current_data[:offset] + data + current_data[offset+len(data):]
        self._execute("UPDATE fs SET data = %s, size = %s WHERE path = %s", (new_data, len(new_data), path))
        return len(data)

    def truncate(self, path, length, fh=None):
        self._execute("SELECT data FROM fs WHERE path = %s", (path,))
        result = self.cursor.fetchone()
        if result is None:
            raise FuseOSError(errno.ENOENT)
        data = result[0][:length]
        self._execute("UPDATE fs SET data = %s, size = %s WHERE path = %s", (data, len(data), path))
        return 0

    def unlink(self, path):
        self._execute("SELECT is_dir, size, ctime, mtime FROM fs WHERE path = %s", (path,))
        result = self.cursor.fetchone()
        if result is None:
            raise FuseOSError(errno.ENOENT)

        self._execute("DELETE FROM fs WHERE path = %s", path)
        return 0


    def getxattr(self, path, name, position=0):
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()
    dsn = os.environ.get('POSTGRES_URL')
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(PostgresFS(dsn=dsn), args.mount, foreground=True, allow_other=True)