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
import unittest


# path to SQLite3 database file
FILENAME = r'/home/gnorton/Lib/_old/tests/com/year47/database/connection/test.db'


# Connection (1)
class TestConnection(unittest.TestCase):
    def testConnectRaisesNotImplemented(self):
        self.connection = Connection()
        with self.assertRaises(NotImplementedError):
            self.connection._connect()


# SQLiteConnection (12)
class TestSQLiteConnection(unittest.TestCase):

    # database from __init__
    def testDatabaseIsNone(self):
        self.sqlite = SQLiteConnection(database=None)
        self.assertEqual(self.sqlite.database, None)

    def testDatabaseIsMemory(self):
        self.sqlite = SQLiteConnection(database=':memory:')
        self.assertEqual(self.sqlite.database, ':memory:')

    def testDatabaseFileExists(self):
        import os.path
        self.sqlite = SQLiteConnection(database=FILENAME)
        self.assertEqual(os.path.isfile(self.sqlite.database), True)

    def testDatabaseFileDoesNotExist(self):
        import os.path
        f = 'foo.db'
        self.sqlite = SQLiteConnection(database=f)
        self.assertEqual(os.path.isfile(self.sqlite.database), False)

    # autocommit in __init__
    def testAutoCommitInTypes(self):
        self.sqlite = SQLiteConnection(autocommit=None)
        self.assertTrue(self.sqlite.autocommit in
                self.sqlite._autocommit_levels)


    # database as property
    def testDBPropertyIsNone(self):
        self.sqlite = SQLiteConnection()
        self.sqlite.database = None
        self.assertEqual(self.sqlite.database, None)

    def testDBPropertyIsMemory(self):
        self.sqlite = SQLiteConnection()
        self.sqlite.database = ':memory:'
        self.assertEqual(self.sqlite.database, ':memory:')

    def testDBPropertyFileExists(self):
        import os.path
        self.sqlite = SQLiteConnection()
        self.sqlite.database = FILENAME
        self.assertEqual(os.path.isfile(self.sqlite.database), True)

    def testDBPropertyFileDoesNotExist(self):
        import os.path
        f = 'foo.db'

        self.sqlite = SQLiteConnection()
        self.sqlite.database = f
        self.assertEqual(os.path.isfile(self.sqlite.database), False)


    # autocommit
    def testAutoCommitNotInTypes(self):
        with self.assertRaises(ValueError):
            self.sqlite = SQLiteConnection()
            self.sqlite.autocommit = 'FOO'

    
    # connect
    def testConnectDatabaseNotSet(self):
        self.sqlite = SQLiteConnection()
        with self.assertRaises(ValueError):
            self.sqlite.connect()

    def testConnect(self):
        self.sqlite = SQLiteConnection()
        self.sqlite.database = FILENAME
        db = self.sqlite.connect
        self.assertTrue('sqlite3.Connection' in repr(db))
        db.close()


# ============================================================================

# MySQLConnection (25)
class TestMySQLConnection(unittest.TestCase):

    # host from __init__
    def testInitHostIsNone(self):
        self.connection = MySQLConnection(host=None)
        self.assertEqual(self.connection.host, None)

    def testInitHostIsNotNone(self):
        self.connection = MySQLConnection(host='localhost')
        self.assertEqual(self.connection.host, 'localhost')


    # user from __init__
    def testInitUserIsNone(self):
        self.connection = MySQLConnection(user=None)
        self.assertEqual(self.connection.user, None)

    def testInitUserIsNotNone(self):
        self.connection = MySQLConnection(user='y47test')
        self.assertEqual(self.connection.user, 'y47test')


    # passwd from __init__
    def testInitPasswdIsNone(self):
        self.connection = MySQLConnection(passwd=None)
        self.assertEqual(self.connection.passwd, None)

    def testInitPasswdIsNotNone(self):
        self.connection = MySQLConnection(passwd='y47test')
        self.assertEqual(self.connection.passwd, 'y47test')


    # database from __init__
    def testInitDbIsNone(self):
        self.connection = MySQLConnection(database=None)
        self.assertEqual(self.connection.database, None)

    def testInitDbIsNotNone(self):
        self.connection = MySQLConnection(database='y47test')
        self.assertEqual(self.connection.database, 'y47test')


    # autocommit from __init__
    def testInitAutoCommitIsOneByDefault(self):
        self.connection = MySQLConnection()
        self.assertEqual(self.connection.autocommit, 1)

    def testInitAutoCommitIsEnabled(self):
        self.connection = MySQLConnection(autocommit=1)
        self.assertEqual(self.connection.autocommit, 1)

    def testInitAutoCommitIsDisabled(self):
        self.connection = MySQLConnection(autocommit=0)
        self.assertEqual(self.connection.autocommit, 0)


    # host property
    def testHostIsNone(self):
        self.connection = MySQLConnection()
        self.connection.host = None
        self.assertEqual(self.connection.host, None)

    def testHostIsNotNone(self):
        self.connection = MySQLConnection()
        self.connection.host = 'localhost'
        self.assertEqual(self.connection.host, 'localhost')


    # user property
    def testUserIsNone(self):
        self.connection = MySQLConnection()
        self.connection.user = None
        self.assertEqual(self.connection.user, None)

    def testUserIsNotNone(self):
        self.connection = MySQLConnection()
        self.connection.user = 'y47test'
        self.assertEqual(self.connection.user, 'y47test')


    # passwd property
    def testPasswdIsNone(self):
        self.connection = MySQLConnection()
        self.connection.passwd = None
        self.assertEqual(self.connection.passwd, None)

    def testPasswdIsNotNone(self):
        self.connection = MySQLConnection()
        self.connection.passwd = 'y47test'
        self.assertEqual(self.connection.passwd, 'y47test')


    # database property
    def testDbIsNone(self):
        self.connection = MySQLConnection()
        self.connection.database = None
        self.assertEqual(self.connection.database, None)

    def testDbIsNotNone(self):
        self.connection = MySQLConnection()
        self.connection.database = 'y47test'
        self.assertEqual(self.connection.database, 'y47test')


    # autocommit property
    def testAutoCommitIsEnabled(self):
        self.connection = MySQLConnection()
        self.connection.autocommit = 1
        self.assertEqual(self.connection.autocommit, 1)

    def testAutoCommitIsDisabled(self):
        self.connection = MySQLConnection()
        self.connection.autocommit = 0
        self.assertEqual(self.connection.autocommit, 0)


    # connect
    def testConnectHostNotSet(self):
        self.connection = MySQLConnection(user='y47test', passwd='y47test',
                                            database='y47test')
        with self.assertRaises(ValueError):
            self.connection.connect()

    def testConnectUserNotSet(self):
        self.connection = MySQLConnection(host='localhost', passwd='y47test',
                                            database='y47test')
        with self.assertRaises(ValueError):
            self.connection.connect()

    def testConnectPasswdNotSet(self):
        self.connection = MySQLConnection(host='localhost', user='y47test',
                                            database='y47test')
        with self.assertRaises(ValueError):
            self.connection.connect()

    def testConnect(self):
        self.connection = MySQLConnection()
        self.connection.host = 'localhost'
        self.connection.user = 'y47test'
        self.connection.passwd = 'y47test'
        self.connection.database = 'y47test'
        db = self.connection.connect

        self.assertTrue('_mysql.connection' in repr(db))
        db.close()


# ============================================================================

# OracleConnection (26)
class TestOracleConnection(unittest.TestCase):
    
    # host from __init__
    def testInitHostIsNone(self):
        self.connection = OracleConnection(host=None)
        self.assertEqual(self.connection.host, None)

    def testInitHostIsNotNone(self):
        self.connection = OracleConnection(host='localhost')
        self.assertEqual(self.connection.host, 'localhost')


    # user from __init__
    def testInitUserIsNone(self):
        self.connection = OracleConnection(user=None)
        self.assertEqual(self.connection.user, None)

    def testInitUserIsNotNone(self):
        self.connection = OracleConnection(user='y47test')
        self.assertEqual(self.connection.user, 'y47test')


    # passwd from __init__
    def testInitPasswdIsNone(self):
        self.connection = OracleConnection(passwd=None)
        self.assertEqual(self.connection.passwd, None)

    def testInitPasswdIsNotNone(self):
        self.connection = OracleConnection(passwd='y47test')
        self.assertEqual(self.connection.passwd, 'y47test')


    # sid from __init__
    def testInitSidIsNone(self):
        self.connection = OracleConnection(sid=None)
        self.assertEqual(self.connection.sid, None)

    def testInitSidIsNotNone(self):
        self.connection = OracleConnection(sid='XE')
        self.assertEqual(self.connection.sid, 'XE')


    # autocommit from __init__
    def testInitAutoCommitIsOneByDefault(self):
        self.connection = OracleConnection()
        self.assertEqual(self.connection.autocommit, 1)

    def testInitAutoCommitIsEnabled(self):
        self.connection = OracleConnection(autocommit=1)
        self.assertEqual(self.connection.autocommit, 1)

    def testInitAutoCommitIsDisabled(self):
        self.connection = OracleConnection(autocommit=0)
        self.assertEqual(self.connection.autocommit, 0)


    # host property
    def testHostIsNone(self):
        self.connection = OracleConnection()
        self.connection.host = None
        self.assertEqual(self.connection.host, None)

    def testHostIsNotNone(self):
        self.connection = OracleConnection()
        self.connection.host = 'localhost'
        self.assertEqual(self.connection.host, 'localhost')


    # user property
    def testUserIsNone(self):
        self.connection = OracleConnection()
        self.connection.user = None
        self.assertEqual(self.connection.user, None)

    def testUserIsNotNone(self):
        self.connection = OracleConnection()
        self.connection.user = 'y47test'
        self.assertEqual(self.connection.user, 'y47test')


    # passwd property
    def testPasswdIsNone(self):
        self.connection = OracleConnection()
        self.connection.passwd = None
        self.assertEqual(self.connection.passwd, None)

    def testPasswdIsNotNone(self):
        self.connection = OracleConnection()
        self.connection.passwd = 'y47test'
        self.assertEqual(self.connection.passwd, 'y47test')


    # sid property
    def testSidIsNone(self):
        self.connection = OracleConnection()
        self.connection.sid = None
        self.assertEqual(self.connection.sid, None)

    def testSidIsNotNone(self):
        self.connection = OracleConnection()
        self.connection.sid = 'XE'
        self.assertEqual(self.connection.sid, 'XE')


    # autocommit property
    def testAutoCommitIsEnabled(self):
        self.connection = OracleConnection()
        self.connection.autocommit = 1
        self.assertEqual(self.connection.autocommit, 1)

    def testAutoCommitIsDisabled(self):
        self.connection = OracleConnection()
        self.connection.autocommit = 0
        self.assertEqual(self.connection.autocommit, 0)


    # connect
    def testConnectHostNotSet(self):
        self.connection = OracleConnection(user='y47test', passwd='y47test',
                                            sid='XE')
        with self.assertRaises(ValueError):
            self.connection.connect()


    def testConnectUserNotSet(self):
        self.connection = OracleConnection(host='localhost', passwd='y47test',
                                            sid='XE')
        with self.assertRaises(ValueError):
            self.connection.connect()


    def testConnectPasswdNotSet(self):
        self.connection = OracleConnection(host='localhost', user='y47test',
                                            sid='XE')
        with self.assertRaises(ValueError):
            self.connection.connect()


    def testConnectSidNotSet(self):
        self.connection = OracleConnection(host='localhost', user='y47test',
                                            passwd='y47test')
        with self.assertRaises(ValueError):
            self.connection.connect()


    def testConnect(self):
        self.connection = OracleConnection()
        self.connection.host = 'localhost'
        self.connection.user = 'y47test'
        self.connection.passwd = 'y47test'
        self.connection.sid = 'XE'
        db = self.connection.connect
        self.assertTrue('cx_Oracle.Connection' in repr(db))
        db.close()

# ============================================================================

if __name__ == '__main__':
    print 'Running connection tests...'
    unittest.main()

# ============================================================================
