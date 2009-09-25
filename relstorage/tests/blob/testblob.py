##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

from ZODB.blob import Blob
from ZODB.DB import DB
from zope.testing import doctest

import os
import random
import re
import struct
import sys
import time
import transaction
import unittest
import ZODB.blob
import ZODB.interfaces
from relstorage.tests.RecoveryStorage import IteratorDeepCompare
import ZODB.tests.StorageTestBase
import ZODB.tests.util
import zope.testing.renormalizing

def new_time():
    """Create a _new_ time stamp.

    This method also makes sure that after retrieving a timestamp that was
    *before* a transaction was committed, that at least one second passes so
    the packing time actually is before the commit time.

    """
    now = new_time = time.time()
    while new_time <= now:
        new_time = time.time()
    time.sleep(1)
    return new_time


class BlobTestBase(ZODB.tests.StorageTestBase.StorageTestBase):

    def setUp(self):
        ZODB.tests.StorageTestBase.StorageTestBase.setUp(self)
        self._storage = self.create_storage()


class BlobUndoTests(BlobTestBase):

    def testUndoWithoutPreviousVersion(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        # the blob footprint object should exist no longer
        self.assertRaises(KeyError, root.__getitem__, 'blob')
        database.close()
        
    def testUndo(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        blob = Blob()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        blob.open('w').write('this is state 2')
        transaction.commit()


        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()
        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()

    def testUndoAfterConsumption(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        open('consume1', 'w').write('this is state 1')
        blob = Blob()
        blob.consumeFile('consume1')
        root['blob'] = blob
        transaction.commit()
        
        transaction.begin()
        blob = root['blob']
        open('consume2', 'w').write('this is state 2')
        blob.consumeFile('consume2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()

    def testRedo(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        blob.open('w').write('this is state 2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 2')

        database.close()

    def testRedoOfCreation(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertRaises(KeyError, root.__getitem__, 'blob')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()


class RecoveryBlobStorage(BlobTestBase,
                          IteratorDeepCompare):

    def setUp(self):
        BlobTestBase.setUp(self)
        self._dst = self.create_storage('dest')

    def tearDown(self):
        self._dst.close()
        BlobTestBase.tearDown(self)

    # Requires a setUp() that creates a self._dst destination storage
    def testSimpleBlobRecovery(self):
        self.assert_(
            ZODB.interfaces.IBlobStorageRestoreable.providedBy(self._storage)
            )
        db = DB(self._storage)
        conn = db.open()
        conn.root()[1] = ZODB.blob.Blob()
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        conn.root()[2].open('w').write('some data')
        transaction.commit()
        conn.root()[3] = ZODB.blob.Blob()
        conn.root()[3].open('w').write(
            (''.join(struct.pack(">I", random.randint(0, (1<<32)-1))
                     for i in range(random.randint(10000,20000)))
             )[:-random.randint(1,4)]
            )
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        conn.root()[2].open('w').write('some other data')
        transaction.commit()
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)
    

def packing_with_uncommitted_data_non_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> root['blob'].open('w').write('test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()

    """

def packing_with_uncommitted_data_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> root['blob'].open('w').write('test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()
    """


def secure_blob_directory():
    """
    This is a test for secure creation and verification of secure settings of
    blob directories.

    >>> blob_storage = create_storage(blob_dir='blobs')

    Two directories are created:

    >>> os.path.isdir('blobs')
    True
    >>> tmp_dir = os.path.join('blobs', 'tmp')
    >>> os.path.isdir(tmp_dir)
    True

    They are only accessible by the owner:

    >>> oct(os.stat('blobs').st_mode)
    '040700'
    >>> oct(os.stat(tmp_dir).st_mode)
    '040700'

    These settings are recognized as secure:

    >>> blob_storage.fshelper.isSecure('blobs')
    True
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    True

    After making the permissions of tmp_dir more liberal, the directory is
    recognized as insecure:

    >>> os.chmod(tmp_dir, 040711)
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    False

    Clean up:

    >>> blob_storage.close()

    """

# On windows, we can't create secure blob directories, at least not
# with APIs in the standard library, so there's no point in testing
# this.
if sys.platform == 'win32':
    del secure_blob_directory

def loadblob_tmpstore():
    """
    This is a test for assuring that the TmpStore's loadBlob implementation
    falls back correctly to loadBlob on the backend.

    First, let's setup a regular database and store a blob:

    >>> blob_storage = create_storage()
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> root['blob'].open('w').write('test')
    >>> import transaction
    >>> transaction.commit()
    >>> blob_oid = root['blob']._p_oid
    >>> tid = connection._storage.lastTransaction()

    Now we open a database with a TmpStore in front:

    >>> database.close()

    >>> from ZODB.Connection import TmpStore
    >>> tmpstore = TmpStore(blob_storage)

    We can access the blob correctly:

    >>> tmpstore.loadBlob(blob_oid, tid) == blob_storage.loadBlob(blob_oid, tid)
    True

    Clean up:

    >>> tmpstore.close()
    >>> database.close()
    """

def is_blob_record():
    r"""
    >>> bs = create_storage()
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> transaction.commit()
    >>> ZODB.blob.is_blob_record(bs.load(ZODB.utils.p64(0), '')[0])
    False
    >>> ZODB.blob.is_blob_record(bs.load(ZODB.utils.p64(1), '')[0])
    True

    An invalid pickle yields a false value:

    >>> ZODB.blob.is_blob_record("Hello world!")
    False
    >>> ZODB.blob.is_blob_record('c__main__\nC\nq\x01.')
    False
    >>> ZODB.blob.is_blob_record('cWaaaa\nC\nq\x01.')
    False

    As does None, which may occur in delete records:

    >>> ZODB.blob.is_blob_record(None)
    False

    >>> db.close()
    """

def do_not_depend_on_cwd():
    """
    >>> bs = create_storage()
    >>> here = os.getcwd()
    >>> os.mkdir('evil')
    >>> os.chdir('evil')
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> conn.root()['blob'].open('w').write('data')
    >>> transaction.commit()
    >>> os.chdir(here)
    >>> conn.root()['blob'].open().read()
    'data'

    >>> bs.close()
    """

def savepoint_isolation():
    """Make sure savepoint data is distinct accross transactions

    >>> bs = create_storage()
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob('initial')
    >>> transaction.commit()
    >>> conn.root.b.open('w').write('1')
    >>> _ = transaction.savepoint()
    >>> tm = transaction.TransactionManager()
    >>> conn2 = db.open(transaction_manager=tm)
    >>> conn2.root.b.open('w').write('2')
    >>> _ = tm.savepoint()
    >>> conn.root.b.open().read()
    '1'
    >>> conn2.root.b.open().read()
    '2'
    >>> transaction.abort()
    >>> tm.commit()
    >>> conn.sync()
    >>> conn.root.b.open().read()
    '2'
    >>> db.close()
    """

def savepoint_cleanup():
    """Make sure savepoint data gets cleaned up.

    >>> bs = create_storage()
    >>> tdir = bs.temporaryDirectory()
    >>> os.listdir(tdir)
    []

    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob('initial')
    >>> _ = transaction.savepoint()
    >>> len(os.listdir(tdir))
    1
    >>> transaction.abort()
    >>> os.listdir(tdir)
    []
    >>> conn.root.b = ZODB.blob.Blob('initial')
    >>> transaction.commit()
    >>> conn.root.b.open('w').write('1')
    >>> _ = transaction.savepoint()
    >>> transaction.abort()
    >>> os.listdir(tdir)
    []

    >>> db.close()
    """


def setUp(test):
    ZODB.tests.util.setUp(test)

def tearDown(test):
    ZODB.tests.util.tearDown(test)

def storage_reusable_suite(prefix, factory,
                           test_blob_storage_recovery=False,
                           test_packing=False,
                           test_undo=True,
                           keep_history=True,
                           pack_test_name='blob_packing.txt',
                           ):
    """Return a test suite for a generic IBlobStorage.

    Pass a factory taking a name and a blob directory name.
    """

    def setup(test):
        setUp(test)
        def create_storage(name='data', blob_dir=None):
            if blob_dir is None:
                blob_dir = '%s.bobs' % name
            return factory(name, blob_dir)

        test.globs['create_storage'] = create_storage
    
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite(
        "blob_connection.txt", "blob_importexport.txt",
        "blob_transaction.txt",
        setUp=setup, tearDown=tearDown,
        optionflags=doctest.ELLIPSIS,
        ))
    if test_packing:
        suite.addTest(doctest.DocFileSuite(
            pack_test_name,
            setUp=setup, tearDown=tearDown,
            ))
    suite.addTest(doctest.DocTestSuite(
        setUp=setup, tearDown=tearDown,
        checker = zope.testing.renormalizing.RENormalizing([
            (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
            ]),
        ))

    def create_storage(self, name='data', blob_dir=None):
        if blob_dir is None:
            blob_dir = '%s.bobs' % name
        return factory(name, blob_dir)

    def add_test_based_on_test_class(class_):
        new_class = class_.__class__(
            prefix+class_.__name__, (class_, ),
            dict(create_storage=create_storage),
            )
        suite.addTest(unittest.makeSuite(new_class))

    if test_blob_storage_recovery:
        add_test_based_on_test_class(RecoveryBlobStorage)
    if test_undo:
        add_test_based_on_test_class(BlobUndoTests)

    suite.layer = ZODB.tests.util.MininalTestLayer(prefix+'BlobTests')

    return suite
