from __future__ import absolute_import
import database.connection as dbcn
from contextlib import contextmanager
import functools as ft
import multiprocessing as mp
import pandas as pd
import pandas.io.sql as pdsql
from tqdm import tqdm


def _chunks(xs, n):
    """
    Yield successive n-sized chunks from xs.
    """
    for i in xrange(0, len(xs), n):
        yield xs[i:i + n]


def _list2insertvalues(data):
    """
    Converts a nested list to a comma seprated values string.  Helper
    function for _list2insertstatements.

    Parameters
    ----------
    data: [[]]

    Returns
    -------
    str
    """
    nrow, ncol = len(data), len(data[0])
    res = nrow*[None]
    processed_row = ncol*[None]
    for i, row in enumerate(data):
        for j, cell in enumerate(row):
            processed_row[j] = _cellval2str(cell)
        res[i] = _list2csv(processed_row)

    return ",".join(res)


def _list2insertstatements(tablename, colnames, data):
    """
    Converts a nested list, where each row represents a row of db data,
    and a list of column names to a list of SQL insert statements.

    Parameters
    ----------
    tablename: str
    colnames: [str]
    data: [[]]

    Returns
    -------
    [INSERT INTO ..., INSERT INTO ...]
    """
    sql_prefix = "INSERT INTO %s %s VALUES" % (tablename, _list2csv(colnames))
    return "%s %s" % (sql_prefix, _list2insertvalues(data))


def _exec_query(curs, queries):
    """
    Execute a query or a list of queries.

    Parameters
    ----------
    cn: CnHandler
    queries: [str]
    """
    if not isinstance(queries, list):
        queries = [queries]

    for query in queries:
        curs.execute(query)

    return None


def _exec_insert_sql(cnhandler, sql_insert):
    """
    Parameters
    ----------
    cnhandler: CnHandler
    sql_insert: str
    """
    with cnhandler.open_cursor() as curs:
        _exec_query(curs, sql_insert)


def _insert_chunk(cnhandler, tablename, colnames, data):
    insert_query = _list2insertstatements(tablename, colnames, data)
    _exec_insert_sql(cnhandler, insert_query)
    return 0


def _insert_list(cnhandler, tablename, colnames, data, njobs, max_rows_per_insert):
    """
    Parameters
    ----------
    cnhandler: connection.CnHandler
    tablename: str
    colnames: [str]
    data: [[]]
    njobs: int
    max_rows_per_insert: int

    Returns
    -------
    None
    """
    chunksize = max_rows_per_insert*njobs
    for job_chunk in tqdm(_chunks(data, chunksize), total=len(data)/chunksize):
        pool = mp.Pool(processes=njobs)
        res = [pool.apply_async(_insert_chunk, args=(cnhandler, tablename, colnames, insert_chunk))
               for insert_chunk in _chunks(job_chunk, max_rows_per_insert)]
        res = [p.get() for p in res]
        pool.close()
        pool.join()


class QueryRunner(object):
    """
    Class for executing sql queries.
    """

    def __init__(self, cnhandler):
        self.cnhandler = cnhandler

    @staticmethod
    def _unicode2str(fr):
        if fr.shape[0]:
            for colname in fr.columns:
                if isinstance(fr[colname].iat[0], unicode):
                    fr[colname] = fr[colname].astype("str").str.strip()
        return fr

    def _sql_select_chunked(self, ssql, chunksize):
        cn = self.cnhandler.open_persistent_connection()
        try:
            for subtable in pdsql.read_sql(ssql, cn, chunksize=chunksize):
                yield self._unicode2str(subtable)
        except Exception as err:
            print err
            yield err
        finally:
            cn.close()

    def _sql_select_unchunked(self, ssql):
        with self.cnhandler.open_connection() as cn:
            table = pdsql.read_sql(ssql, cn)
        return self._unicode2str(table)

    def sql_select(self, ssql, chunksize=0):
        """
        Parameters
        ----------
        ssql: string
            sql SELECT statement or stored proc call

        Examples
        --------
        >>> cn = DbCnPxm0nedbProd()
        >>> # Standard select
        >>> ph = cn.sql_select("SELECT * FROM ProductHeader")
        >>> # Stored proc
        >>> sp_results = cn.sql_select("exec usp_do_something 'foo', 'bar'")
        """
        if chunksize:
            return self._sql_select_chunked(ssql, chunksize)
        else:
            return self._sql_select_unchunked(ssql)

    def _insert_list(self, tablename, colnames, data, njobs, max_rows_per_insert):
        """
        Parameters
        ----------
        tablename: str
        colnames: list
        data: list
        closecn: bool
        """
        _insert_list(cnhandler=self.cnhandler, tablename=tablename,
                     colnames=colnames, data=data, njobs=njobs, 
                     max_rows_per_insert=max_rows_per_insert)

    def sql_insert(self, tablename, data, colnames=None, njobs=None, max_rows_per_insert=None):
        """
        Insert a nested list or a DataFrame into a table.
        If data is a list, user must also provide a list of column names.
        If data is a DataFrame, the DataFrame's columns must be the same as the 
        columns in the table.

        For either type of data, rows represent rows of a table.

        By default, this function will break data into evenly sized chunks and insert the chunks
        in parallel.  If the function ends up using too much memory, lower max_rows_per_insert
        to a more reasonable size.

        Parameters
        ----------
        tablename: str
        data: list or DataFrame
        colnames: [str]
        njobs: int
            Number of cores to use.
        max_rows_per_insert: int
            Maximum number of rows inserted per core.
        """
        njobs = njobs or mp.cpu_count()

        if isinstance(data, list):
            assert(colnames is not None)
        elif isinstance(data, pd.DataFrame):
            colnames = data.columns
            data = data.values.tolist()
        else:
            assert Exception("Data of type %s not allowed." % type(data))

        if not max_rows_per_insert:
            nrow = len(data)
            # Break jobs into evenly sized chunks.
            if njobs < nrow:
                max_rows_per_insert = len(data)/njobs
            # Handle edge case where there are fewer rows than cores.
            else:
                max_rows_per_insert = nrow

        self._insert_list(tablename=tablename, colnames=colnames,
                          data=data, njobs=njobs, max_rows_per_insert=max_rows_per_insert)

    @staticmethod
    def _delete_where(tablename, cols):
        """
        Creates sql statement to delete a row from a table.  Ends up making a string like

        DELETE FROM tablename WHERE
            cols.keys[0] = cols.values[0] AND ... AND cols.keys[n] = cols.values[n]

        Parameters
        ----------
        tablename: str
        cols: {colname: colval}

        Returns
        -------
        str
        """
        wherecond = " AND ".join(["%s = %s" % (colname, _cellval2str(colval, True))
                                  for colname, colval in cols.iteritems()])
        return "DELETE FROM %s WHERE %s" % (tablename, wherecond)

    def sql_upsert(self, tablename, data, keycols, njobs=0):
        """
        Parameters
        ----------
        tablename: str
        data: DataFrame
        keycols: list
        """
        delete_queries = []
        for _, row in data.iterrows():
            cols = {colname: row.loc[colname] for colname in keycols}
            delete_queries.append(self._delete_where(tablename, cols))
        self.exec_query(delete_queries)
        self.sql_insert(tablename, data, njobs=njobs)

    def exec_query(self, queries):
        """
        Execute a query or a list of queries.  Never returns anything.

        Parameters
        ----------
        queries: list
        """
        with self.cnhandler.open_cursor() as curs:
            _exec_query(curs=curs, queries=queries)


def tutorial_connection():
    handler = dbcn.PgCnHandler(dbname="tutorial", username="hahdawg")
    return QueryRunner(cnhandler=handler)


if __name__ == "__main__":
	cn = tutorial_connection()
	print cn.sql_select("SELECT * FROM kid LIMIT 5")
