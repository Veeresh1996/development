
import os
from urllib.parse import unquote_plus, urlparse, urlunparse, splituser, splithost, splitport, splitpasswd, parse_qsl


class Path(object):
    def __init__(self, path: str):
        super().__init__()
        self.original_path = unquote_plus(path)
        self.url = urlparse(self.original_path)

    def get(self) -> str:
        return self.original_path

    def get_with_suffix(self, suffix: str) -> str:
        return urlunparse(self.url._replace(path=os.path.join(self.url.path, suffix)))

    # local fs funcs
    def is_local(self) -> bool:
        return self.url.scheme == "" or self.url.scheme == "file"

    def local_mkdirs(self):
        if self.is_local():
            os.makedirs(self.url.path, exist_ok=True)

    def get_path(self) -> str:
        return self.url.path

    def get_path_with_base_path_removed(self, base_path: str) -> str:
        return self.url.path.replace(base_path, "")

    # s3 funcs
    def is_s3(self) -> bool:
        return self.url.scheme == "s3"

    def get_s3_bucket(self) -> str:
        return self.url.netloc

    def get_s3_key(self) -> str:
        return self.url.path[1:]

    # mysql funcs
    def is_mysql(self) -> bool:
        return self.url.scheme == "mysql"

    def get_mysql_cfg(self) -> dict:
        cfg = dict(parse_qsl(self.url.query))
        hostpart, _ = splithost(urlunparse(self.url._replace(scheme="")))
        userpart, hostpart = splituser(hostpart)
        host, port = splitport(hostpart)
        user, passwd = splitpasswd(userpart)
        cfg['user'] = user
        if passwd:
            cfg['password'] = passwd
        cfg['host'] = host
        if port:
            cfg['port'] = port
        cfg['database'] = self.url.path[1:]
        return cfg
