#####
#
# y47 Software Library Resource
# Copyright (C) 2010-2012 Year47. All Rights Reserved.
# 
# Author: Glenn T Norton
# Contact: glenn@year47.com
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
# 
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
# 
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
#
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
#
# 3. This notice may not be removed or altered from any source distribution.
#
#####

class Connection(object):
    def _connect(self): raise NotImplementedError

# END: Connection


class SQLiteConnection(Connection):
    """example: 
        from y47.db.connection import SQLiteConnection
        connection = SQLiteConnection(database=r'sqlite.db').connect
        print connection
        <sqlite3.Connection object at 0x7f9dda3f5a28>
        
        # or, if you like typing

        from y47.db.connection import SQLiteConnection
        connection = SQLiteConnection()
        connection.database = r'sqlite.db'
        db = connection.connect
        print db
        <sqlite3.Connection object at 0x7fcad01f0a28>
        """
    def __init__(self, database=None, autocommit=None):
        Connection.__init__(self)
        self._database = database
        self._autocommit = autocommit

        self._autocommit_levels = [None, 'DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']


    # database
    def _getDatabase(self):
        return self._database

    def _setDatabase(self, database):
        self._database = database

    database = property(_getDatabase, _setDatabase, None, 'database property')


    def _getAutoCommit(self):
        return self._autocommit

    def _setAutocommit(self, autocommit=None):
        if autocommit not in self._autocommit_levels:
            raise ValueError, 'autocommit type not in %s' % \
                    self._autocommit_levels

        self._autocommit = autocommit

    autocommit = property(_getAutoCommit, _setAutocommit, None, 
                        'autocommit property')


    # connect
    def _connect(self):
        if not self._getDatabase():
            raise ValueError, "database not set i.e. 'filename' or ':memory:'"

        self._connection = None
        try:
            try:
                import sqlite
            except ImportError:
                try:
                    import sqlite3 as sqlite
                except ImportError:
                    print 'Cannot find SQLite module'

            self._connection = sqlite.connect(self.database, 
                                        isolation_level=self._getAutoCommit())

            self._connection.text_factory = str
            return self._connection

        except(StandardError, sqlite.Error), err:
            print err       

    connect = property(_connect, None, None, 'connect property')


# END: SQLiteConnection


class MySQLConnection(Connection):
    """example:
    from y47.db.connection import MySQLConnection
    connection = MySQLConnection(host='localhost', user='test', passwd='test',
                                database='test').connect
    print connection
    <_mysql.connection open to 'localhost' at 20a3f50>

    # or, if you like typing

    from y47.db.connection import MySQLConnection
    connection = MySQLConnection()
    connection.host = 'localhost'
    connection.user = 'test'
    connection.passwd = 'test'
    connection.database = 'test'
    db = connection.connect
    print db
    <_mysql.connection open to 'localhost' at 1b94cf0>
    """
    def __init__(self, host=None, user=None, passwd=None, database=None, 
                autocommit=1):
        Connection.__init__(self)
        self._host = host
        self._user = user
        self._passwd = passwd
        self._database = database
        self._autocommit = autocommit

        self._mysql = None


    def _getHost(self):
        return self._host

    def _setHost(self, host):
        self._host = host

    host = property(_getHost, _setHost, None, 'host')


    def _getUser(self):
        return self._user

    def _setUser(self, user):
        self._user = user

    user = property(_getUser, _setUser, None, 'user')


    def _getPasswd(self):
        return self._passwd

    def _setPasswd(self, passwd):
        self._passwd = passwd

    passwd = property(_getPasswd, _setPasswd, None, 'passwd')


    def _getDatabase(self):
        return self._database

    def _setDatabase(self, database):
        self._database = database

    database = property(_getDatabase, _setDatabase, None, 'Db')


    def _getAutoCommit(self):
        return self._autocommit

    def _setAutocommit(self, autocommit=1):
        self._autocommit = autocommit

    autocommit = property(_getAutoCommit, _setAutocommit, None, 'autocommit')


    def _connect(self):
        if not self._getHost(): raise ValueError, 'host not set'
        if not self._getUser(): raise ValueError, 'user not set'
        if not self._getPasswd(): raise ValueError, 'passwd not set'
        if not self._getDatabase(): raise ValueError, 'database not set'

        self._mysql = None
        try:
            import MySQLdb
        except ImportError:
            print 'Cannot find MySQLdb module'

        try:
            self._mysql = MySQLdb.connect( 
                host = self._getHost(),
                user = self._getUser(),
                passwd = self._getPasswd(),
                db = self._getDatabase()
            )

            self._mysql._transactional = self._getAutoCommit()
            return self._mysql

        except(StandardError, MySQLdb.Error), err:
            print err

    connect = property(_connect, None, None, 'connect')


# END: MySQLConnection


class OracleConnection(Connection):
    """ example:
        from y47.db.connection import OracleConnection
        connection = OracleConnection(host='127.0.0.1', user='test', 
                                        passwd='test', sid='ora').connect
        print connection
        <cx_Oracle.Connection to y47test@127.0.0.1/XE>

        # or, if you really like typing

        from y47.db.connection import OracleConnection
        connection = OracleConnection()
        connection.host = '127.0.0.1'
        connection.user = 'test'
        connection.passwd = 'test'
        connection.sid = 'ora'
        db = connection.connect
        print db
        <cx_Oracle.Connection to y47test@127.0.0.1/XE>
    """
    def __init__(self, host=None, user=None, passwd=None, sid=None, 
                autocommit=1):

        Connection.__init__(self)
        self._host = host
        self._user = user
        self._passwd = passwd
        self._sid = sid
        self._autocommit = autocommit

    
    def _getHost(self):
        return self._host

    def _setHost(self, host):
        self._host = host

    host = property(_getHost, _setHost, None, 'host')


    def _getUser(self):
        return self._user

    def _setUser(self, user):
        self._user = user

    user = property(_getUser, _setUser, None, 'user')


    def _getPasswd(self):
        return self._passwd

    def _setPasswd(self, passwd):
        self._passwd = passwd

    passwd = property(_getPasswd, _setPasswd, None, 'passwd')


    def _getSid(self):
        return self._sid

    def _setSid(self, sid):
        self._sid = sid

    sid = property(_getSid, _setSid, None, 'sid')


    def _getAutoCommit(self):
        return self._autocommit

    def _setAutocommit(self, autocommit=1):
        self._autocommit = autocommit

    autocommit = property(_getAutoCommit, _setAutocommit, None, 'autocommit')


    def _connect(self):
        if not self._getHost(): raise ValueError, 'host not set'
        if not self._getUser(): raise ValueError, 'user not set'
        if not self._getPasswd(): raise ValueError, 'passwd not set'
        if not self._getSid(): raise ValueError, 'sid not set'

        self._oracle = None
        try:
            import cx_Oracle
        except ImportError:
            print 'Cannot find cx_Oracle module'

        try:
            self._connection_string = "%s/%s@%s/%s" % (
                self._getUser(),
                self._getPasswd(),
                self._getHost(),
                self._getSid()
            )
            self._oracle = cx_Oracle.connect(self._connection_string)
            self._oracle.autocommit = self._getAutoCommit()
            return self._oracle

        except(StandardError, cx_Oracle.Error), err:
            raise cx_Oracle.Error(err)

    connect = property(_connect, None, None, 'connect')


# END: OracleConnection

