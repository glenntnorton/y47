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

from y47.db.connection import Connection, SQLiteConnection
from y47.db.connection import MySQLConnection, OracleConnection

from y47.db.cursor import Cursor, SQLiteCursor, SQLiteDictionaryCursor
from y47.db.cursor import OracleCursor, OracleDictionaryCursor
from y47.db.cursor import MySQLCursor, MySQLDictionaryCursor

import unittest
import types


# path to SQLite3 database file
FILENAME = r'test.db'

ALLOWED_TYPES = [types.DictType, types.DictionaryType, types.TupleType]


# Query (1)
class TestCursor(unittest.TestCase):
    def setUp(self):
        self.connection = SQLiteConnection(database=FILENAME).connect

    def testQueryRaisesNotImplemented(self):
        self.cursor = Cursor()
        with self.assertRaises(NotImplementedError):
            self.cursor._execute("SELECT * FROM test WHERE name=?", ('Glenn',))


# ============================================================================


# SQLiteCursor (10)
class TestSQLiteCursor(unittest.TestCase):
    def setUp(self):
        self.connection = SQLiteConnection(database=FILENAME).connect

    # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = SQLiteCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        self.assertTrue('sqlite3.Connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # row_type from __init__
    def testRowTypeDefault(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeInAllowedTypes(self):
        self.cursor = SQLiteCursor(connection=self.connection,
                                    row_type=types.DictionaryType)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeNotInAllowedTypes(self):
        self.cursor = SQLiteCursor(connection=self.connection,
                                    row_type=types.ListType)
        self.assertTrue(self.cursor._getRowType() not in ALLOWED_TYPES)


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = SQLiteCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")

    def testConnectId(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=?",
                                    ('Glenn',))
        self.assertEqual(results[0][0], 1)

    def testConnectName(self):
        self.cursor = SQLiteCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=?",
                                    ('Glenn',))
        self.assertEqual(results[0][1], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


# SQLiteDictionaryCursor (7)
class TestSQLiteDictionaryCursor(unittest.TestCase):
    def setUp(self):
        self.connection = SQLiteConnection(database=FILENAME).connect

    # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = SQLiteDictionaryCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = SQLiteDictionaryCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = SQLiteDictionaryCursor(connection=self.connection)
        self.assertTrue('sqlite3.Connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = SQLiteDictionaryCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = SQLiteDictionaryCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")

    def testConnectId(self):
        self.cursor = SQLiteDictionaryCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=?",
                                    ('Glenn',))
        self.assertEqual(results[0]['id'], 1)

    def testConnectName(self):
        self.cursor = SQLiteDictionaryCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=?",
                                    ('Glenn',))
        self.assertEqual(results[0]['name'], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


# MySQLCursor (10)
class TestMySQLCursor(unittest.TestCase):
    def setUp(self):
        self.connection = MySQLConnection(host='localhost', user='y47test', 
                                passwd='y47test', database='y47test').connect

    # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = MySQLCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = MySQLCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = MySQLCursor(connection=self.connection)
        self.assertTrue('_mysql.connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = MySQLCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # row_type from __init__
    def testRowTypeDefault(self):
        self.cursor = MySQLCursor(connection=self.connection)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeInAllowedTypes(self):
        self.cursor = MySQLCursor(connection=self.connection,
                                    row_type=types.DictionaryType)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeNotInAllowedTypes(self):
        self.cursor = MySQLCursor(connection=self.connection,
                                    row_type=types.ListType)
        self.assertTrue(self.cursor._getRowType() not in ALLOWED_TYPES)


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = MySQLCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")

    def testConnectId(self):
        self.cursor = MySQLCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test")
        self.assertEqual(results[0][0], 1)

    def testConnectName(self):
        self.cursor = MySQLCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test")
        self.assertEqual(results[0][1], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


# MySQLDictionaryCursor (7)
class TestMySQLDictionaryCursor(unittest.TestCase):
    def setUp(self):
        self.connection = MySQLConnection(host='localhost', user='y47test', 
                                passwd='y47test', database='y47test').connect

    # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = MySQLDictionaryCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = MySQLDictionaryCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = MySQLDictionaryCursor(connection=self.connection)
        self.assertTrue('_mysql.connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = MySQLDictionaryCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = MySQLDictionaryCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")

    def testConnectId(self):
        self.cursor = MySQLDictionaryCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=%s",
                                    ('Glenn',))
        self.assertEqual(results[0]['id'], 1)

    def testConnectName(self):
        self.cursor = MySQLDictionaryCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test WHERE name=%s",
                                    ('Glenn',))
        self.assertEqual(results[0]['name'], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


# OracleCursor (9)
class TestOracleCursor(unittest.TestCase):
    def setUp(self):
        self.connection = OracleConnection(host='127.0.0.1', user='y47test', 
                                        passwd='y47test', sid='XE').connect

    # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = OracleCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = OracleCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = OracleCursor(connection=self.connection)
        self.assertTrue('cx_Oracle.Connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = OracleCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # row_type from __init__
    def testRowTypeDefault(self):
        self.cursor = OracleCursor(connection=self.connection)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeInAllowedTypes(self):
        self.cursor = OracleCursor(connection=self.connection,
                                    row_type=types.DictionaryType)
        self.assertTrue(self.cursor._getRowType() in ALLOWED_TYPES)

    def testRowTypeNotInAllowedTypes(self):
        self.cursor = OracleCursor(connection=self.connection,
                                    row_type=types.ListType)
        self.assertTrue(self.cursor._getRowType() not in ALLOWED_TYPES)


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = OracleCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")


    def testConnectName(self):
        self.cursor = OracleCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test")
        self.assertEqual(results[0][0], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


# OracleDictionaryCursor (6)
class TestOracleDictionaryCursor(unittest.TestCase):
    def setUp(self):
        self.connection = OracleConnection(host='127.0.0.1', user='y47test', 
                                        passwd='y47test', sid='XE').connect

   # connection from __init__
    def testInitConnectionIsNone(self):
        self.cursor = OracleDictionaryCursor(connection=None)
        self.assertEqual(self.cursor.connection, None)

    def testInitConnectionIsNotNone(self):
        self.cursor = OracleDictionaryCursor(connection=self.connection)
        self.assertTrue(self.cursor.connection is not None)

    def testInitConnectionIsCorrectType(self):
        self.cursor = OracleDictionaryCursor(connection=self.connection)
        self.assertTrue('cx_Oracle.Connection' in repr(self.cursor.connection))

    def testInitConnectionIsIncorrectType(self):
        self.cursor = OracleDictionaryCursor(connection=self.connection)
        self.assertTrue('foo' not in repr(self.cursor.connection))


    # execute
    def testExecuteConnectionNotSet(self):
        self.cursor = OracleDictionaryCursor()
        with self.assertRaises(ValueError):
            self.cursor.execute("SELECT * FROM test")


    def testConnectName(self):
        self.cursor = OracleDictionaryCursor(connection=self.connection)
        results = self.cursor.execute("SELECT * FROM test")
        self.assertEqual(results[0]["NAME"], 'Glenn')


    def tearDown(self):
        self.connection = None
        del self.connection


# ============================================================================


if __name__ == '__main__':
    print 'Running cursor tests...'
    unittest.main()


# ============================================================================
