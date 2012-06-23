#####
#
# Year47 Software Library Resource
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

import unittest
from connection import *

class TestConnection(unittest.TestCase):
    
    def setUp(self):
        self.connection = Connection.Connection()


    def test_connect(self):
        with self.assertRaises(NotImplementedError):
            self.connection.connect()
        
    
    def test_getExceptionHandler(self):
        with self.assertRaises(NotImplementedError):
            tmp = self.connection.getExceptionHandler()


    def test_getQuoteHandler(self):
        with self.assertRaises(NotImplementedError):
            self.connection.getQuoteHandler()


    def tearDown(self):
        del self.connection


# -------------------------------------------------------------------------- #


class TestConnectionFactory(unittest.TestCase):

    def setUp(self):
        self.factory = ConnectionFactory()


    def test_getFactoryMySQL(self):
        self.mysql = self.factory.getConnection('mysql')
        self.assertTrue('MySQLConnection' in repr(self.mysql))


    def test_getFactoryOracle(self):
        self.oracle = self.factory.getConnection('oracle')
        self.assertTrue('OracleConnection' in repr(self.oracle))


    def test_getFactorySQLite(self):
        self.sqlite = self.factory.getConnection('sqlite')
        self.assertTrue('SQLiteConnection' in repr(self.sqlite))


    # This will spit out 'No factory call available for foo' to STDOUT
    # Remove if you like
    def test_getFactoryFailure(self):
        self.foo = self.factory.getConnection('foo')
        self.assertTrue(self.foo is None)


    def tearDown(self):
        del self.factory        


# -------------------------------------------------------------------------- #


class TestConnection(unittest.TestCase):
    
    def setUp(self):
        self.factory = Factory()


    def test_getConnection(self):
        with self.assertRaises(NotImplementedError):
            self.factory.getConnection('foo')
        
    
    def tearDown(self):
        del self.factory


# -------------------------------------------------------------------------- #


class TestMySQLConnection(unittest.TestCase):

    def setUp(self):
        self.mysql_connection = MySQLConnection()



    def test_setHost(self):
        self.mysql_connection.setHost('localhost')
        self.assertEqual(self.mysql_connection.host, 'localhost')


    def test_setUser(self):
        self.mysql_connection.setUser('y47test')
        self.assertEqual(self.mysql_connection.user, 'y47test')


    def test_setPasswd(self):
        self.mysql_connection.setPasswd('y47test')
        self.assertEqual(self.mysql_connection.passwd, 'y47test')


    def test_setDb(self):
        self.mysql_connection.setDb('y47test')
        self.assertEqual(self.mysql_connection.db, 'y47test')


    def test_setAutoCommit(self):
        self.mysql_connection.setAutocommit(1)
        self.assertEqual(self.mysql_connection.autocommit, 1)


    def test_connect(self):
        self.mysql_connection.setHost('localhost')
        self.mysql_connection.setUser('y47test')
        self.mysql_connection.setPasswd('y47test')
        self.mysql_connection.setDb('y47test')
        self.mysql_connection.setAutocommit(1)

        self._mysql_dbh = self.mysql_connection.connect()

        self.assertTrue('_mysql.connection' in repr(self._mysql_dbh))
        self.assertEqual(self._mysql_dbh._transactional, 1)
        self.assertEqual(self._mysql_dbh.get_host_info(), 'Localhost via UNIX socket')
    

    def tearDown(self):
        del self.mysql_connection


# -------------------------------------------------------------------------- #


class TestOracleConnection(unittest.TestCase):

    def setUp(self):
        self.oracle_connection = OracleConnection()


    def test_setUsername(self):
        self.oracle_connection.setUsername('y47test')
        self.assertEqual(self.oracle_connection.username, 'y47test')


    def test_setPasswd(self):
        self.oracle_connection.setPasswd('y47test')
        self.assertEqual(self.oracle_connection.passwd, 'y47test')


    def test_setIPAddress(self):
        self.oracle_connection.setIPAddress('127.0.0.1')
        self.assertEqual(self.oracle_connection.ip_address, '127.0.0.1')


    def test_setSID(self):
        self.oracle_connection.setSID('xe')
        self.assertEqual(self.oracle_connection.sid, 'xe')


    def test_setAutoCommit(self):
        self.oracle_connection.setAutocommit(1)
        self.assertEqual(self.oracle_connection.autocommit, 1)


    def test_connect(self):
        self.oracle_connection.setUsername('y47test')
        self.oracle_connection.setPasswd('y47test')
        self.oracle_connection.setIPAddress('127.0.0.1')
        self.oracle_connection.setSID('xe')
        self.oracle_connection.setAutocommit(1)

        self.connection_string = "%s/%s@%s/%s" % (
            self.oracle_connection.username,
            self.oracle_connection.passwd,
            self.oracle_connection.ip_address,
            self.oracle_connection.sid
        )
        self._oracle_dbh = self.oracle_connection.connect()

        self.assertTrue('cx_Oracle.Connection' in repr(self._oracle_dbh))
        self.assertEqual(self._oracle_dbh.autocommit, 1)
        self.assertEqual(self._oracle_dbh.dsn, '127.0.0.1/xe')
    

    def tearDown(self):
        del self.oracle_connection


# -------------------------------------------------------------------------- #


class TestSQLiteConnection(unittest.TestCase):

    def setUp(self):
        self.sqlite_connection = SQLiteConnection()


    def test_connect_to_file(self):
        self.sqlite_connection.setDatabase('test.db')
        self.db = self.sqlite_connection.connect()
        self.assertTrue('Connection' in repr(self.db))


    def test_connect_to_memory(self):
        self.sqlite_connection.setDatabase(':memory')
        self.db = self.sqlite_connection.connect()
        self.assertTrue('Connection' in repr(self.db))


    def test_setDatabase(self):
        self.sqlite_connection.setDatabase('test.db')
        self.assertEqual('test.db', self.sqlite_connection.database)


    def test_getExceptionHandler(self):
        self.sqlite_connection.setDatabase('test.db')
        self.db = self.sqlite_connection.connect()
        self.handler = self.sqlite_connection.getExceptionHandler()
        self.assertTrue('DatabaseError' in repr(self.handler))


    def test_getQouteHandler(self):
        self.sqlite_connection.setDatabase('test.db')
        self.db = self.sqlite_connection.connect()
        self.handler = self.sqlite_connection.getQuoteHandler()
        self.assertTrue('_quoteHandler' in repr(self.handler))


    def tearDown(self):
        del self.sqlite_connection


# -------------------------------------------------------------------------- #


if __name__ == '__main__':
    unittest.main()


# -------------------------------------------------------------------------- #
# END: TestConnection.py
# vim: set ai tw=79 sw=4 sts=4 set ft=python # 

