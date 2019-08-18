# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase

from relstorage.cache import interfaces

from relstorage.cache import mvcc


class TestMVCCDatabaseCorrdinator(TestCase):

    def _makeOne(self):
        return mvcc.MVCCDatabaseCoordinator()

    def test_implements(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStorageCacheMVCCDatabaseCoordinator))

    def test_register(self):
        c = self._makeOne()
        c.register(self)
        self.assertTrue(c.is_registered(self))
        c.unregister(self)
        self.assertFalse(c.is_registered(self))


class TestTransactionRangeObjectIndex(TestCase):

    def _makeOne(self, *args, **kwargs):
        return mvcc._TransactionRangeObjectIndex(*args, **kwargs)

    def test_bad_tid_in_ctor(self):
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=1, complete_since_tid=2, data=())

    def test_bad_data(self):
        # Too high
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          complete_since_tid=0,
                          data=[(1, 3)])

        # Too low
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          complete_since_tid=0,
                          data=[(1, 0)])

        # Just right
        c = self._makeOne(highest_visible_tid=2,
                          complete_since_tid=0,
                          data=[(1, 1)])

        self.assertEqual(2, c.highest_visible_tid)
        self.assertEqual(0, c.complete_since_tid)
