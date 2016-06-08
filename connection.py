from contextlib import contextmanager
import psycopg2
import pyodbc
import sys


def iswindows():
    return sys.platform.startswith("win")


class BaseCnHandler(object):
    """
    Abstract interface for database connection handler.  Not to be instantiated.

    Parameters
    ----------
    cnfcn: function implementing pyodbc.connect
    """

    def __init__(self, cnfcn):
        self.cnfcn = cnfcn

    @property
    def cn_str(self):
        raise NotImplementedError()

    def open_persistent_connection(self):
        return self.cnfcn(self.cn_str)

    @contextmanager
    def open_cursor(self):
        cn = self.open_persistent_connection()
        curs = cn.cursor()
        try:
            yield curs
        except Exception as err:
            raise err
        else:
            curs.execute("COMMIT")
        finally:
            cn.close()

    @contextmanager
    def open_connection(self):
        cn = self.open_persistent_connection()
        try:
            yield cn
        except Exception as err:
            print err
            raise err
        finally:
            cn.close()


class PgCnHandler(BaseCnHandler):
    """
    Connect to Postgres.
    """
    def __init__(self, dbname, username, host="localhost"):
        super(PgCnHandler, self).__init__(cnfcn=psycopg2.connect)
        self.username = username
        self.host = host
        self.dbname = dbname

    @property
    def cn_str(self):
        return "dbname=%s host='%s' user=%s" % (self.dbname, self.host, self.username)


class SqlCnHandler(BaseCnHandler):

    def __init__(self, server, dbname, username="", password=""):
        super(SqlCnHandler, self).__init__(cnfcn=pyodbc.connect)
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password

    @property
    def _linux_cn_str(self):
        return "DSN=%s; DATABASE=%s; UID=%s; PWD=%s" % \
               (self.server, self.dbname, self.username, self.password)

    @property
    def _windows_cn_str(self):
        res = "DRIVER={SQL Server}; SERVER=%s; DATABASE=%s" % (self.server, self.dbname)
        if self.username:
            res += "; UID=%s; PWD=%s" % (self.username, self.password)
        else:
            res += "; Trusted_Connection=yes;"
        return res

    @property
    def cn_str(self):
        if iswindows():
            return self._windows_cn_str
        else:
            return self._linux_cn_str
