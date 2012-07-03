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

import types
from y47.db import InvalidRowType


def _dictionaryFactory(c):
    if c:
        keys = [ d[0] for d in c.description ]
        return [ dict( zip(keys, row) ) for row in c ]

    return None

# ============================================================================

class Cursor(object):
    def _execute(self, sql, args): raise NotImplementedError


# ============================================================================


class SQLiteCursor(Cursor):
    """examples:

        TupleType row example (default)

        FILENAME = r'test.db'
        from y47.db.connection import SQLiteConnection
        from y47.db.cursor import SQLiteCursor
        connection = SQLiteConnection(database=FILENAME).connect
        cursor = SQLiteCursor(connection=connection)
        results = cursor.execute("SELECT * FROM test WHERE name=?", ('Glenn',))
        for result in results:
	        print result[0][1]


        DictionaryType row example 
        ( or you could use SQLiteDictionaryCursor :) )

        import types
        FILENAME = r'test.db'
        from y47.db.connection import SQLiteConnection
        from y47.db.cursor import SQLiteCursor
        connection = SQLiteConnection(database=FILENAME).connect
        cursor = SQLiteCursor(connection=connection, row_type=types.DictionaryType)
        results = cursor.execute("SELECT * FROM test WHERE name=?", ('Glenn',))
        for result in results:
	        print result['name']
    """
    def __init__(self, connection=None, row_type=types.TupleType): 
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = row_type
        self._row_types = [types.DictType, types.DictionaryType, 
                            types.TupleType]

        self._cursor = None


    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')


    def _getRowType(self):
        return self._row_type

    def _setRowType(self, row_type=types.TupleType):
        self._row_type = row_type

    row_type = property(_getRowType, _setRowType, None, 'row_type property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'

        # setup the cursor
        if self._getRowType() in [types.DictType, types.DictionaryType]:
            try:
                import sqlite
            except ImportError:
                try:
                    import sqlite3 as sqlite
                except ImportError:
                    print 'Cannot find SQLite module'

            self._connection.row_factory = sqlite.Row


        self._cursor = self._connection.cursor()

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        return self._cursor.fetchall()


    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: SQLiteCursor
# ============================================================================


class SQLiteDictionaryCursor(Cursor):
    """example:

        FILENAME = r'test.db'
        from y47.db.connection import SQLiteConnection
        from y47.db.cursor import SQLiteCursor
        connection = SQLiteConnection(database=FILENAME).connect
        cursor = SQLiteCursor(connection=connection)
        results = cursor.execute("SELECT * FROM test WHERE name=?", ('Glenn',))
        for result in results:
	        print result['name']
    """
    def __init__(self, connection=None): 
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = types.DictionaryType
        self._row_types = [types.DictType, types.DictionaryType]

        self._cursor = None


    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'

        # setup the cursor
        if self._row_type not in self._row_types:
            raise InvalidRowType(self._row_type, self._row_types, 
                                'Invalid Rowtype')
            
        try:
            import sqlite
        except ImportError:
            try:
                import sqlite3 as sqlite
            except ImportError:
                print 'Cannot find SQLite module'

        self._connection.row_factory = sqlite.Row
        self._cursor = self._connection.cursor()

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        return self._cursor.fetchall()


    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: SQLiteDictionaryCursor
# ============================================================================


class MySQLCursor(Cursor):
    """examples:

        TupleType row example (default)

        from y47.db.connection import MySQLConnection
        from y47.db.cursor import MySQLCursor
        connection = MySQLConnection(host='localhost', user='y47test', 
                                passwd='y47test', database='y47test').connect
        cursor = MySQLCursor(connection=connection)
        cursor.execute("SELECT * FROM test")
        ((1L, 'Glenn'),)

        DictionaryType row example 
        ( or you could use MySQLDictionaryCursor :) )

        import types
        cursor = MySQLCursor(connection=connection, row_type=types.DictionaryType)
        cursor.execute("SELECT * FROM test")
        ({'id': 1L, 'name': 'Glenn'},)
    """
    def __init__(self, connection=None, row_type=types.TupleType):
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = row_type
        self._row_types = [types.DictType, types.DictionaryType, 
                            types.TupleType]

        self._cursor = None

    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')

    def _getRowType(self):
        return self._row_type

    def _setRowType(self, row_type=types.TupleType):
        self._row_type = row_type

    row_type = property(_getRowType, _setRowType, None, 'row_type property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'


        if self._getRowType() in [types.DictType, types.DictionaryType]:
            try:
                import MySQLdb.cursors
                self._cursor = \
                        self._connection.cursor(MySQLdb.cursors.DictCursor)
            except ImportError:
                print 'could not find MySQLdb cursors'
        else:
            self._cursor = self._connection.cursor()
        

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        return self._cursor.fetchall()


    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: MySQLCursor
# ============================================================================


class MySQLDictionaryCursor(Cursor):
    """example:

        from y47.db.connection import MySQLConnection
        from y47.db.cursor import MySQLDictionaryCursor
        connection = MySQLConnection(host='localhost', user='y47test', 
                                    passwd='y47test', database='y47test').connect
        cursor = MySQLDictionaryCursor(connection=connection)
        cursor.execute("SELECT * FROM test")
        ({'id': 1L, 'name': 'Glenn'},)
    """
    def __init__(self, connection=None): 
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = types.DictionaryType
        self._row_types = [types.DictType, types.DictionaryType]

        self._cursor = None


    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'

        # setup the cursor
        if self._row_type not in self._row_types:
            raise InvalidRowType(self._row_type, self._row_types, 
                                'Invalid Rowtype')
            
        try:
            import MySQLdb.cursors
            self._cursor = \
                    self._connection.cursor(MySQLdb.cursors.DictCursor)
        except ImportError:
            print 'could not find MySQLdb cursors'

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        return self._cursor.fetchall()


    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: MySQLDictionaryCursor
# ============================================================================


class OracleCursor(Cursor):
    """examples:

        TupleType row example (default)
    
        from y47.db.connection import OracleConnection
        from y47.db.cursor import OracleCursor
        connection = OracleConnection(host='127.0.0.1', user='y47test', 
                                        passwd='y47test', sid='XE').connect
        cursor = OracleCursor(connection=connection)
        results = cursor.execute("SELECT * FROM TEST")
        for result in results:
	        print result[0][0]
	
        Glenn

        DictionaryType row example 
        ( or you could use OracleDictionaryCursor :) )
        
        import types
        from y47.db.connection import OracleConnection
        from y47.db.cursor import OracleCursor
        connection = OracleConnection(host='127.0.0.1', user='y47test', 
                                        passwd='y47test', sid='XE').connect
        cursor = OracleCursor(connection=connection, 
                                row_type=types.DictionaryType)
        results = cursor.execute("SELECT * FROM TEST")
        for result in results:
	        print result['NAME']
	
        Glenn
    """
    def __init__(self, connection=None, row_type=types.TupleType):
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = row_type
        self._row_types = [types.DictType, types.DictionaryType, 
                            types.TupleType]

        self._cursor = None

    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')

    def _getRowType(self):
        return self._row_type

    def _setRowType(self, row_type=types.TupleType):
        self._row_type = row_type

    row_type = property(_getRowType, _setRowType, None, 'row_type property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'


        self._cursor = self._connection.cursor()

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        if self._getRowType() in [types.DictType, types.DictionaryType]:
            return _dictionaryFactory(self._cursor)
        else:
            return self._cursor.fetchall()



    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: OracleCursor
# ============================================================================


class OracleDictionaryCursor(Cursor):
    """example:
    
        from y47.db.connection import OracleConnection
        from y47.db.cursor import OracleDictionaryCursor
        connection = OracleConnection(host='127.0.0.1', user='test',
                                        passwd='test', sid='ora').connect

        cursor = OracleDictionaryCursor(connection=connection)
        cursor.execute("SELECT * FROM TEST")
        
        [{'NAME': 'Glenn'}]
    """
    def __init__(self, connection=None): 
        Cursor.__init__(self)
        self._connection = connection
        self._row_type = types.DictionaryType
        self._row_types = [types.DictType, types.DictionaryType]

        self._cursor = None


    def _getConnection(self):
        return self._connection

    def _setConnection(self, connection):
        self._connection = connection

    connection = property(_getConnection, _setConnection, None, 
                        'connection property')


    def _execute(self, sql, args=None):
        if not self._getConnection():
            raise ValueError, 'connection not set'

        # setup the cursor
        if self._row_type not in self._row_types:
            raise InvalidRowType(self._row_type, self._row_types, 
                                'Invalid Rowtype')

        self._cursor = self._connection.cursor()

        if args:
            self._cursor.execute(sql, args)
        else:
            self._cursor.execute(sql)

        return _dictionaryFactory(self._cursor)


    def execute(self, sql, args=None):
        return self._execute(sql, args)


# END: OracleDictionaryCursor
# ============================================================================



