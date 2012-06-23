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
    """Connection(object): Connection is an abstract class. 
    
    Objects inheriting from Connection MUST override the
    connect(), getExceptionHandler() and getQuoteHandler() methods or a
    NotImplementedError will be raised.
    """

    def connect(self): 
        raise NotImplementedError

    def getExceptionHandler(self):
        raise NotImplementedError

    def getQuoteHandler(self):
        raise NotImplementedError



class Factory(object):
    """Factory(object): Factory is an abstract class. 
    
    Objects inheriting from Factory MUST override the getConnection() method 
    or a NotImplementedError will be raised."""

    def getConnection(self, name): 
        raise NotImplementedError



class ConnectionFactory(Factory):
    """ConnectionFactory(Factory): - inherits from Factory.
    
    Objects inheriting from Factory MUST override the getConnection() method 
    or a NotImplementedError will be raised."""
    def __init__(self):
        pass


    def getConnection(self, name):
        """getConnection - currently accepts 'mysql, 'oracle', 'sqlite'"""
        if not name:
            raise ValueError, "name required"

        self._connection = None

        try:
            if 'mysql' in name.lower():
                self._connection = MySQLConnection()

            elif 'oracle' in name.lower():
                self._connection = OracleConnection()

            elif 'sqlite' in name.lower():
                self._connection = SQLiteConnection()

            else:
                raise ValueError, "No factory call available for %s" % name

            return self._connection

        except(StandardError), err:
            print err



class MySQLConnection(Connection):
    """MySQLConnection(Connection.Connection) - inherits from Connection. 
    
    Objects inheriting from Connection MUST override the
    connect(), getExceptionHandler() and getQuoteHandler() methods or a
    NotImplementedError will be raised."""
    
    def __init__(self, host=None, user=None, passwd=None, db=None, 
            autocommit=1):
        """initiate a MySQLConnection.

        host, user, passwd, db & autocommit attributes may be assigned here or
        through appropriate set* methods.

        """
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.autocommit = autocommit


    def setHost(self, host):
        """assign mysql hostname"""
        self.host = host


    def setUser(self, user):
        """assign MySQL username"""
        self.user = user


    def setPasswd(self, passwd):
        """assign MySQL password"""
        self.passwd = passwd


    def setDb(self, db):
        """assign MySQL database"""
        self.db = db


    def setAutocommit(self, autocommit=1):
        """assign autocommit to bypass commit requirement. default is 1"""
        self.autocommit = autocommit



    def connect(self):
        """connect to a MySQL database"""
        import MySQLdb
        try:
            self._mysql_dbh = MySQLdb.connect( 
                host = self.host,
                user = self.user,
                passwd = self.passwd,
                db = self.db
            )

            self._mysql_dbh._transactional = self.autocommit
            return self._mysql_dbh

        except(StandardError, MySQLdb.Error), err:
            print err



    def getExceptionHandler(self):
        """returns the top level error handler"""
        return self._mysql_dbh.Error


    def getQuoteHandler(self):
        """returns the default quote handler"""
        return self._mysql_dbh.escape




class OracleConnection(Connection):
    """OracleConnection(Connection.Connection) - inherits from Connection. 
    
    Objects inheriting from Connection MUST override the
    connect(), getExceptionHandler() and getQuoteHandler() methods or a
    NotImplementedError will be raised.""" 

    def __init__(self, username=None, passwd=None, ip_address='127.0.0.1', 
                sid=None, autocommit=1):
        """initiate an OracleConnection.

        username, passwd, ip_address, sid & autocommit attributes may be 
        assigned here or through appropriate set* methods.

        """ 
        self.username = username
        self.passwd = passwd
        self.ip_address = ip_address
        self.sid = sid
        self.autocommit = autocommit

    
    def setUsername(self, u):
        """assign Oracle username"""
        self.username = u

    
    def setPasswd(self, p):
        """assign Oracle password"""
        self.passwd = p


    def setIPAddress(self, ip_address='127.0.0.1'):
        """assign server domain or IP address"""
        self.ip_address = ip_address


    def setSID(self, s):
        """assign Oracle system identifier"""
        self.sid = s


    def setAutocommit(self, autocommit=1):
        """assign Oracle transaction autocommit. default is 1"""
        self.autocommit = autocommit


    def connect(self):
        """connect to a Oracle database"""
        import cx_Oracle
        try:
            self.connection_string = "%s/%s@%s/%s" % (
                self.username,
                self.passwd,
                self.ip_address,
                self.sid
            )
            self._oracle_dbh = cx_Oracle.connect(self.connection_string)
            self._oracle_dbh.autocommit = self.autocommit
            return self._oracle_dbh

        except(StandardError, cx_Oracle.Error), err:
            raise cx_Oracle.Error(err)



    def getExceptionHandler(self):
        """returns the top level error handler"""
        import cx_Oracle
        return cx_Oracle.Error


    def getQuoteHandler(self):
        """returns the default quote handler"""
        return self._quoteHandler


    def _quoteHandler(self, s):
        """should not be called directly. Use getQuoteHandler() instead.
        
        handles single & double qoutes
        
        **FIXME**
        consider removal, should be using parameters.
        i.e. (\"\"\"SELECT * FROM test WHERE name = ?\"\"\", ('Glenn',))
        originally created to handle single quotes in strings i.e. "O''Reilly"
        """
        new_s = ""
        tmp = s.split("'")
        for t in tmp[0:-1]:
            new_s += t
            new_s += "''"
        new_s += tmp[-1]

        tmp = new_s.split('"')
        new_s = ""
        for t in tmp[0:-1]:
            new_s += t
            new_s += '""'
        new_s += tmp[-1]

        return new_s



class SQLiteConnection(Connection):
    """SQLiteConnection(Connection.Connection) - inherits from Connection.
    
    Connection is an abstract class. Objects inheriting from Connection MUST override the
    connect(), getExceptionHandler() and getQuoteHandler() methods or a
    NotImplementedError will be raised."""

    def __init__(self, database=None):
        """initiate a SQLiteConnection"""
        self.database = database


    def setDatabase(self, database):
        """set the database i.e. 'filename' or ':memory'"""
        self.database = database


    def connect(self):
        """connect to a SQLite database"""
        # import pysqlite or sqlite3
        try:
            import sqlite
        except ImportError:
            import sqlite3 as sqlite

        if not self.database:
            raise ValueError, "Database not set i.e. 'filename' or ':memory'"

        try:
            self._sqlite_dbh = sqlite.connect(self.database, isolation_level=None)
            self._sqlite_dbh.text_factory = str
            return self._sqlite_dbh
        except(StandardError, sqlite.Error), err:
            print err       

    def getDictionaryRow(self):
        """pass this to your SQLCursor.setDictionaryRow() method if you prefer
        key/value fetch* results."""
        # import pysqlite or sqlite3
        try:
            import sqlite
        except ImportError:
            import sqlite3 as sqlite
        return sqlite.Row


    def getExceptionHandler(self):
        """returns the top level error handler"""
        return self._sqlite_dbh.DatabaseError


    def getQuoteHandler(self):
        """returns the default quote handler"""
        return self._quoteHandler


    def _quoteHandler(self, s):
        """should not be called directly. Use getQuoteHandler() instead.
        
        handles single & double qoutes
        
        **FIXME**
        consider removal, should be using parameters.
        i.e. (\"\"\"SELECT * FROM test WHERE name = ?\"\"\", ('Glenn',))
        originally created to handle single quotes in strings i.e. "O''Reilly"
        """
        new_s = ""
        tmp = s.split("'")
        for t in tmp[0:-1]:
            new_s += t
            new_s += "''"
        new_s += tmp[-1]

        tmp = new_s.split('"')
        new_s = ""
        for t in tmp[0:-1]:
            new_s += t
            new_s += '""'
        new_s += tmp[-1]

        return new_s


# END: connection.py
# vim: set ai tw=79 sw=4 sts=4 set ft=python # 
