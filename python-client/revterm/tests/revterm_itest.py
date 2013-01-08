# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import

import unittest
import revterm.connection as co

class ConnectionIntegrationTest(unittest.TestCase):
    """
    Lays the groundwork for later tests, making sure that they are even worth doing.
    """
    @classmethod
    def setUpClass(cls):
        client = None
        try:
            client = co.RevoriConnection()
        except Exception as e:
            print("Unable to connect, make sure that Revori is running on localhost, \
                port 8017")
            raise e
        finally:
            if client != None:
                client.close()
        
        
    def setUp(self):
        self.client = co.RevoriConnection() 
    
    def tearDown(self):
        self.client.send('drop database zoo')
        self.client.close()
    
    def test_help(self):
        self.client.send('help')
        
        val = self.client.recv()
        
        self.assertEqual(85, val.message.count('\n'))
    
    def test_db_creation(self):
        self.client.send('create database zoo')
        
        val = self.client.recv()
        
        self.assertEqual('created database zoo', val.message)
        
    def test_db_use(self):
        self.client.send('create database zoo')
        self.client.recv()
        
        self.client.send('use database zoo')
        
        val = self.client.recv()
        
        self.assertEqual('switched to database zoo', val.message)
        self.assertEqual('zoo', val.database)
        
    def test_error_code(self):
        self.client.send('aleph')
        val = self.client.recv()
        
        self.assertEqual(4, val.code)
        self.assertEqual("Sorry, I don't understand.", val.message)
        
    def test_no_database(self):
        self.client.send('select * from zoo')
        val = self.client.recv()
        
        self.assertEqual(4, val.code)
        self.assertEqual("no database specified", val.message)
        
    def test_transaction_begin_commit_messages(self):
        self.client.send('begin')
        self.client.recv()
        
        self.client.send('commit')
        commitval = self.client.recv()
        
        self.assertEqual(3, commitval.code)
        self.assertEqual("committed transaction", commitval.message)
        
    def test_transaction_begin_rollback_messages(self):
        self.client.send('begin')
        self.client.recv()
        
        self.client.send('rollback')
        rbval = self.client.recv()
        
        self.assertEqual(3, rbval.code)
        self.assertEqual("abandoned transaction", rbval.message)
    
    def test_transaction_begin_messages(self):
        self.client.send('begin')
        val = self.client.recv()
        
        self.assertEqual(3, val.code)
        self.assertEqual("pushed new transaction context", val.message)
        
    def test_transaction_rollback_error_messages(self):
        self.client.send('rollback')
        val = self.client.recv()
        
        self.assertEqual(4, val.code)
        self.assertEqual("no transaction in progress", val.message)
        
        
class TerminalIntegrationTest(unittest.TestCase):
    """
    Just a subset of the script from:
    
        https://github.com/ReadyTalk/revori/wiki/CLI-Revori-Client
    
    Future work would involve breaking these apart into individual tests and adding more
    of the (currently undocumented) features. 
    """
    @classmethod
    def setUpClass(cls):
        client = None
        try:
            client = co.RevoriConnection()
        except Exception as e:
            print("Unable to connect, make sure that Revori is running on localhost, \
                port 8017")
            raise e
        finally:
            if client != None:
                client.close()
                   
    def setUp(self):
        self.client = co.RevoriConnection()
        self.client.send('create database zoo')
        self.client.recv()
        self.client.send('use database zoo')
        self.client.recv()
    
    def tearDown(self):
        self.client.send('drop database zoo')
        self.client.close()
    
    def test_casting_exception(self):
        self.client.send('create table test (id int32, val int32, primary key (id))')
        self.client.recv()
        
        self.client.send("insert into test values (1, 'foo')")
        val = self.client.recv()
        
        self.assertEqual(4, val.code)
    
    def test_transaction_rollback(self):
        self.client.send('create table test (id int32, val int32, primary key (id))')
        self.client.recv()
        
        self.client.send('begin')
        self.client.recv()
        
        self.client.send('insert into test values (1, 1)')
        self.client.recv()
        
        self.client.send('rollback')
        self.client.recv()
        
        self.client.send('select * from test')
        val = self.client.recv()
        
        self.assertEqual(0, len(val.message))
    
    def test_copy(self):
        self.client.send('create table test (id int64, val int64, primary key (id))')
        self.client.recv() 
        
        self.client.send('copy test from stdin')
        val = self.client.recv()
        
        self.assertEqual('reading row data until "\\."', val.message)
        self.assertEqual(2, val.code) 
        self.assertTrue(val.copy_context)
        
        self.client.send('1,1')
        self.client.send('2,2')
        self.client.send('3,3')
        
        self.client.send('\\.')
        val = self.client.recv()
        
        self.assertEqual('inserted 3 row(s)', val.message)
        self.assertEqual(3, val.code)
        self.assertFalse(val.copy_context)
    
    def test_copy_no_table(self):
        self.client.send('copy test from stdin')
        val = self.client.recv()
        
        self.assertEqual('no such table: test', val.message)
        self.assertFalse(val.copy_context)

    def test_table_script(self):
        self.client.send('create table animals(name string, sound string, class string, \
            primary key(name))')
        
        val = self.client.recv()
        
        self.assertEqual('table animals defined', val.message)
        
        self.client.send("insert into animals values('Alligator', 'snap', 'reptilia')")
        val = self.client.recv()
        
        self.assertEqual("inserted 1 row", val.message)
        
        self.client.send("insert into animals values ('Rabbit', 'nibble', 'mammalia')")
        val = self.client.recv()
        
        self.client.send('tag original head')
        val = self.client.recv()
        
        self.assertEqual('tag original set to head', val.message)
        
        self.client.send('select * from animals')
        val = self.client.recv()
        
        self.assertEqual([('inserted', 'Alligator', 'snap', 'reptilia'), \
            ('inserted', 'Rabbit', 'nibble', 'mammalia')], val.message)
         
        self.client.send("select sound from animals where name='Rabbit'")
        val = self.client.recv()
        
        self.assertEqual([('inserted', 'nibble')], val.message)
        
        self.client.send('tag v1 head')
        val = self.client.recv()
        
        self.assertEqual('tag v1 set to head', val.message)
        
        self.client.send("insert into animals values('Blue Whale', \
            'yyyywwwwwaaaaauuuuuunnnnnn', 'mammalia')")
        val = self.client.recv()
        
        self.assertEqual('inserted 1 row', val.message)
        
        self.client.send('tag v2 head')
        val = self.client.recv()
        
        self.assertEqual('tag v2 set to head', val.message)
        
        self.client.send('diff v1 v2 select * from animals')
        val = self.client.recv()
        
        self.assertEqual([('inserted', 'Blue Whale', 'yyyywwwwwaaaaauuuuuunnnnnn', \
            'mammalia')], val.message)
        
        self.client.send('diff v2 v1 select * from animals')
        val = self.client.recv()
        
        self.assertEqual([('deleted', 'Blue Whale', 'yyyywwwwwaaaaauuuuuunnnnnn', \
            'mammalia')], val.message)
            
        self.client.send('tag head v1')
        val = self.client.recv()
        
        self.assertEqual('tag head set to v1', val.message)
        
        self.client.send('select * from animals')
        val = self.client.recv()
        
        self.assertEqual([('inserted', 'Alligator', 'snap', 'reptilia'), \
            ('inserted', 'Rabbit', 'nibble', 'mammalia')], val.message)
            
        self.client.send("insert into animals values('Germ', 'ick!', 'various')")
        val = self.client.recv()
        
        self.assertEqual('inserted 1 row', val.message)
        
        self.client.send('tag v3 head')
        
        val = self.client.recv()
        
        self.assertEqual('tag v3 set to head', val.message)
        
        self.client.send('diff v1 v3 select * from animals')
        val = self.client.recv()
        
        self.assertEqual([('inserted', 'Germ', 'ick!', \
            'various')], val.message)
        
        self.client.send('merge v1 v2 v3')
        val = self.client.recv()
        
        self.assertEqual('head set to result of merge (0 conflict(s))', val.message)
        
    

if __name__ == '__main__':
    unittest.main()
