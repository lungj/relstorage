##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""Object load/store implementations.
"""

from base64 import decodestring
from base64 import encodestring
from ZODB.POSException import StorageError
import time

try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5


def compute_md5sum(data):
    if data is not None:
        return md5(data).hexdigest()
    else:
        # George Bailey object
        return None


class HistoryPreservingPostgreSQLLoadStore(object):

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        stmt = """
        SELECT tid
        FROM current_object
        WHERE zoid = %s
        """
        cursor.execute(stmt, (oid,))
        for (tid,) in cursor:
            return tid
        return None

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        cursor.execute("""
        SELECT encode(state, 'base64'), tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state64, tid = cursor.fetchone()
            if state64 is not None:
                state = decodestring(state64)
            else:
                # This object's creation has been undone
                state = None
            return state, tid
        else:
            return None, None

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        cursor.execute("""
        SELECT encode(state, 'base64')
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state64,) = cursor.fetchone()
            if state64 is not None:
                return decodestring(state64)
        return None

    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = "SELECT 1 FROM current_object WHERE zoid = %s"
        cursor.execute(stmt, (oid,))
        for row in cursor:
            return True
        return False

    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        cursor.execute("""
        SELECT encode(state, 'base64'), tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            state64, tid = cursor.fetchone()
            if state64 is not None:
                state = decodestring(state64)
            else:
                # The object's creation has been undone
                state = None
            return state, tid
        else:
            return None, None

    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT tid
        FROM object_state
        WHERE zoid = %s
            AND tid > %s
        ORDER BY tid
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()[0]
        else:
            return None

    def on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        stmt = """
        CREATE TEMPORARY TABLE temp_store (
            zoid        BIGINT NOT NULL,
            prev_tid    BIGINT NOT NULL,
            md5         CHAR(32),
            state       BYTEA
        ) ON COMMIT DROP;
        CREATE UNIQUE INDEX temp_store_zoid ON temp_store (zoid)
        """
        cursor.execute(stmt)

    def store_temp(self, cursor, oid, prev_tid, data):
        """Store an object in the temporary table."""
        md5sum = compute_md5sum(data)
        stmt = """
        DELETE FROM temp_store WHERE zoid = %s;
        INSERT INTO temp_store (zoid, prev_tid, md5, state)
        VALUES (%s, %s, %s, decode(%s, 'base64'))
        """
        cursor.execute(stmt, (oid, oid, prev_tid, md5sum, encodestring(data)))

    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table."""
        md5sum = compute_md5sum(data)
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = decode(%s, 'base64')
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, encodestring(data), oid))

    def restore(self, cursor, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = compute_md5sum(data)
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (%s, %s,
            COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
            %s, decode(%s, 'base64'))
        """
        if data is not None:
            data = encodestring(data)
        cursor.execute(stmt, (oid, tid, oid, md5sum, data))

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            encode(temp_store.state, 'base64')
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        LIMIT 1
        """
        cursor.execute(stmt)
        if cursor.rowcount:
            oid, prev_tid, attempted_prev_tid, data = cursor.fetchone()
            return oid, prev_tid, attempted_prev_tid, decodestring(data)
        return None

    def move_from_temp(self, cursor, tid):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, %s, prev_tid, md5, state
        FROM temp_store
        """
        cursor.execute(stmt, (tid,))

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in cursor]

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        cursor.execute("""
        -- Insert objects created in this transaction into current_object.
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %(tid)s
            AND prev_tid = 0;

        -- Change existing objects.  To avoid deadlocks,
        -- update in OID order.
        UPDATE current_object SET tid = %(tid)s
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = %(tid)s
                AND prev_tid != 0
            ORDER BY zoid
        )
        """, {'tid': tid})


class HistoryPreservingMySQLLoadStore(object):

    def __init__(self, Binary):
        self.Binary = Binary

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        stmt = """
        SELECT tid
        FROM current_object
        WHERE zoid = %s
        """
        cursor.execute(stmt, (oid,))
        for (tid,) in cursor:
            return tid
        return None

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        cursor.execute("""
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """, (oid,))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()
        else:
            return None, None

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        cursor.execute("""
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            (state,) = cursor.fetchone()
            return state
        return None

    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = "SELECT 1 FROM current_object WHERE zoid = %s"
        cursor.execute(stmt, (oid,))
        for row in cursor:
            return True
        return False

    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        cursor.execute("""
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()
        else:
            return None, None

    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT tid
        FROM object_state
        WHERE zoid = %s
            AND tid > %s
        ORDER BY tid
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        if cursor.rowcount:
            assert cursor.rowcount == 1
            return cursor.fetchone()[0]
        else:
            return None

    def on_store_opened(self, cursor, restart=False):
        """Create the temporary table for storing objects"""
        if restart:
            stmt = """
            DROP TEMPORARY TABLE IF EXISTS temp_store
            """
            cursor.execute(stmt)

        stmt = """
        CREATE TEMPORARY TABLE temp_store (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            prev_tid    BIGINT NOT NULL,
            md5         CHAR(32),
            state       LONGBLOB
        ) ENGINE MyISAM
        """
        cursor.execute(stmt)

    def store_temp(self, cursor, oid, prev_tid, data):
        """Store an object in the temporary table."""
        md5sum = compute_md5sum(data)
        stmt = """
        REPLACE INTO temp_store (zoid, prev_tid, md5, state)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(stmt, (oid, prev_tid, md5sum, self.Binary(data)))

    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table."""
        md5sum = compute_md5sum(data)
        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, self.Binary(data), oid))

    def restore(self, cursor, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = compute_md5sum(data)
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (%s, %s,
            COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
            %s, %s)
        """
        if data is not None:
            data = self.Binary(data)
        cursor.execute(stmt, (oid, tid, oid, md5sum, data))

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        # Lock in share mode to ensure the data being read is up to date.
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            temp_store.state
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        LIMIT 1
        LOCK IN SHARE MODE
        """
        cursor.execute(stmt)
        if cursor.rowcount:
            return cursor.fetchone()
        return None

    def move_from_temp(self, cursor, tid):
        """Moved the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, %s, prev_tid, md5, state
        FROM temp_store
        """
        cursor.execute(stmt, (tid,))

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in cursor]

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        cursor.execute("""
        REPLACE INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
        """, (tid,))


class HistoryPreservingOracleLoadStore(object):

    def __init__(self, runner, Binary, inputsize_BLOB, inputsize_BINARY):
        self.runner = runner
        self.Binary = Binary
        self.inputsize_BLOB = inputsize_BLOB
        self.inputsize_BINARY = inputsize_BINARY

    def get_current_tid(self, cursor, oid):
        """Returns the current integer tid for an object.

        oid is an integer.  Returns None if object does not exist.
        """
        stmt = """
        SELECT tid
        FROM current_object
        WHERE zoid = :1
        """
        cursor.execute(stmt, (oid,))
        for (tid,) in cursor:
            return tid
        return None

    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        stmt = """
        SELECT state, tid
        FROM current_object
            JOIN object_state USING(zoid, tid)
        WHERE zoid = :1
        """
        return self.runner.run_lob_stmt(
            cursor, stmt, (oid,), default=(None, None))

    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = """
        SELECT state
        FROM object_state
        WHERE zoid = :1
            AND tid = :2
        """
        (state,) = self.runner.run_lob_stmt(
            cursor, stmt, (oid, tid), default=(None,))
        return state

    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = "SELECT 1 FROM current_object WHERE zoid = :1"
        cursor.execute(stmt, (oid,))
        for row in cursor:
            return True
        return False

    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        stmt = """
        SELECT state, tid
        FROM object_state
        WHERE zoid = :oid
            AND tid = (
                SELECT MAX(tid)
                FROM object_state
                WHERE zoid = :oid
                    AND tid < :tid
            )
        """
        return self.runner.run_lob_stmt(
            cursor, stmt, {'oid': oid, 'tid': tid}, default=(None, None))

    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT MIN(tid)
        FROM object_state
        WHERE zoid = :1
            AND tid > :2
        """
        cursor.execute(stmt, (oid, tid))
        rows = cursor.fetchall()
        if rows:
            assert len(rows) == 1
            return rows[0][0]
        else:
            return None

    on_store_opened = None  # no store connection initialization needed

    def store_temp(self, cursor, oid, prev_tid, data):
        """Store an object in the temporary table."""
        md5sum = compute_md5sum(data)
        cursor.execute("DELETE FROM temp_store WHERE zoid = :oid", oid=oid)
        if len(data) <= 2000:
            # Send data inline for speed.  Oracle docs say maximum size
            # of a RAW is 2000 bytes.  inputsize_BINARY corresponds with RAW.
            cursor.setinputsizes(rawdata=self.inputsize_BINARY)
            stmt = """
            INSERT INTO temp_store (zoid, prev_tid, md5, state)
            VALUES (:oid, :prev_tid, :md5sum, :rawdata)
            """
            cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
                md5sum=md5sum, rawdata=data)
        else:
            # Send data as a BLOB
            cursor.setinputsizes(blobdata=self.inputsize_BLOB)
            stmt = """
            INSERT INTO temp_store (zoid, prev_tid, md5, state)
            VALUES (:oid, :prev_tid, :md5sum, :blobdata)
            """
            cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
                md5sum=md5sum, blobdata=data)

    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table."""
        md5sum = compute_md5sum(data)
        cursor.setinputsizes(data=self.inputsize_BLOB)
        stmt = """
        UPDATE temp_store SET
            prev_tid = :prev_tid,
            md5 = :md5sum,
            state = :data
        WHERE zoid = :oid
        """
        cursor.execute(stmt, oid=oid, prev_tid=prev_tid,
            md5sum=md5sum, data=self.Binary(data))

    def restore(self, cursor, oid, tid, data):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = compute_md5sum(data)
        cursor.setinputsizes(data=self.inputsize_BLOB)
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        VALUES (:oid, :tid,
            COALESCE((SELECT tid FROM current_object WHERE zoid = :oid), 0),
            :md5sum, :data)
        """
        if data is not None:
            data = self.Binary(data)
        cursor.execute(stmt, oid=oid, tid=tid, md5sum=md5sum, data=data)

    def detect_conflict(self, cursor):
        """Find one conflict in the data about to be committed.

        If there is a conflict, returns (oid, prev_tid, attempted_prev_tid,
        attempted_data).  If there is no conflict, returns None.
        """
        stmt = """
        SELECT temp_store.zoid, current_object.tid, temp_store.prev_tid,
            temp_store.state
        FROM temp_store
            JOIN current_object ON (temp_store.zoid = current_object.zoid)
        WHERE temp_store.prev_tid != current_object.tid
        """
        return self.runner.run_lob_stmt(cursor, stmt)

    def move_from_temp(self, cursor, tid):
        """Move the temporarily stored objects to permanent storage.

        Returns the list of oids stored.
        """
        stmt = """
        INSERT INTO object_state (zoid, tid, prev_tid, md5, state)
        SELECT zoid, :tid, prev_tid, md5, state
        FROM temp_store
        """
        cursor.execute(stmt, tid=tid)

        stmt = """
        SELECT zoid FROM temp_store
        """
        cursor.execute(stmt)
        return [oid for (oid,) in cursor]

    def update_current(self, cursor, tid):
        """Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        # Insert objects created in this transaction into current_object.
        stmt = """
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = :1
            AND prev_tid = 0
        """
        cursor.execute(stmt, (tid,))

        # Change existing objects.
        stmt = """
        UPDATE current_object SET tid = :1
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = :1
                AND prev_tid != 0
        )
        """
        cursor.execute(stmt, (tid,))

