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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint:disable=unused-argument
import os
import os.path
import random
import time
from collections import namedtuple

from pyperf import perf_counter

from relstorage.options import Options
from relstorage._util import get_memory_usage
from relstorage._util import byte_display

logger = __import__('logging').getLogger(__name__)

try:
    import sys
    import cProfile
    import pstats
    if '--profile' not in sys.argv:
        raise ImportError
except ImportError:
    class cProfile(object):
        class Profile(object):
            def enable(self):
                pass
            def disable(self):
                pass
    class pstats(object):
        class Stats(object):
            def __init__(self, *args):
                pass
            def sort_stats(self, *args):
                return self
            def print_stats(self, *args):
                pass


class MockOptions(Options):
    cache_module_name = ''
    cache_servers = ''
    cache_local_object_max = 16384
    cache_local_compression = 'none'
    cache_local_dir_count = 1

def profiled(func, name, runner):
    import vmprof # pylint:disable=import-error
    import tempfile


    def f(*args):
        prefix = name.replace(' ', '_') + '-'
        suffix = '.vmprof' + "%d%d" % (sys.version_info[:2])
        handle, _ = tempfile.mkstemp(suffix, prefix=prefix, dir=runner.args.temp)
        vmprof.enable(handle, lines=True)
        result = func(*args)
        vmprof.disable()
        os.close(handle)
        return result

    return f

def _combine_benchmark_results(options, group_name, benchmark_group, cache_options):
    # Do this in the master only, after running all the benchmarks
    # for a database.
    if options.worker:
        return

    if options.output:
        from pyperf import Benchmark
        from pyperf import BenchmarkSuite

        # Create a file for the entire suite, using names that can
        # be compared across different database configurations.
        dir_name = os.path.splitext(options.output)[0] + '.d'
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        # We're going to update the metadata, so we need to make
        # a copy.
        # Use the short name so that even runs across different object
        # counts are comparable.
        for name, benchmark in list(benchmark_group.items()):
            benchmark = Benchmark(benchmark.get_runs())
            benchmark.update_metadata({'name': name})
            benchmark_group[name] = benchmark
        suite = BenchmarkSuite(benchmark_group.values())

        fname = os.path.join(dir_name,
                             group_name + '_' + str(cache_options.cache_local_mb) + '.json')
        suite.dump(fname, replace=True)


def run_and_report_funcs(runner, named_funcs):
    profile = runner.args.profile
    benchmarks = {}
    for description in named_funcs:
        func = description[1]
        name = description[0]
        args = description[2:]
        if profile:
            func = profiled(func, name, runner)
        benchmark = runner.bench_time_func(name, func, *args)
        benchmarks[name] = benchmark
    return benchmarks


def local_benchmark(runner):
    # pylint:disable=too-many-statements,too-many-locals
    from relstorage.cache.local_client import LocalClient
    from relstorage.cache.lru_sqlite import SqlMapping
    options = MockOptions()
    options.cache_local_mb = 100
    options.cache_local_compression = 'none'


    KEY_GROUP_SIZE = 400
    DATA_SIZE = 1024

    # With 1000 in a key group, and 1024 bytes of data, we produce
    # 909100 keys, and 930918400 = 887MB of data, which will overflow
    # a cache of 500 MB.

    # A group size of 100 produces 9100 keys with 9318400 = 8.8MB of data.
    # Likewise, group of 200 produces 36380 keys with 35.5MB of data.

    # Group size of 400 produces 145480 keys with 142MB of data.

    # Most of our time is spent in compression, it seems.
    # In the 8.8mb case, populating all the data with default compression
    # takes about 2.5-2.8s. Using no compression, it takes 0.38 to 0.42s.
    # Reading is the same at about 0.2s.


    with open('/dev/urandom', 'rb') as f:
        random_data = f.read(DATA_SIZE)

    key_groups = []
    key_groups.append([(i, -1) for i in range(KEY_GROUP_SIZE)])
    for i in range(1, KEY_GROUP_SIZE):
        keys = [(i, j) for j in range(KEY_GROUP_SIZE)]
        assert len(set(keys)) == len(keys)
        key_groups.append(keys)


    # Recent PyPy and Python 3.6 preserves iteration order of a dict
    # to match insertion order. If we use a dict for ALL_DATA, this
    # gives slightly different results due to the key lengths being
    # different and so things being ejected at slightly different
    # times (on PyPy, 8 key groups have *no* matches in read() using a dict,
    # while that doesn't occur in cPython 2.7/3.4). To
    # make this all line up the same, we preserve order everywhere by using
    # a list of tuples (sure enough, that change makes 8 groups go missing)
    # Alternately, if we sort by the hash of the key, we get the iteration order that
    # CPython used for a dict, making all groups of keys be found in read(). This
    # keeps the benchmark consistent

    ALL_DATA = {}
    for group in key_groups:
        for key in group:
            ALL_DATA[key] = (random_data, key[1])
    ALL_DATA = list(ALL_DATA.items())
    ALL_DATA.sort(key=lambda x: hash(x[0]))
    #print("Entries", len(ALL_DATA),
    #      "Data size", byte_display(sum((len(v[1][0]) for v in ALL_DATA))))

    def makeOne(bucket_kind, populate=True):
        options.cache_local_storage = bucket_kind
        client = LocalClient(options, 'pfx')
        if populate:
            client._bucket0.bulk_update(ALL_DATA)
        return client

    def populate_equal(loops, bucket_kind):
        # Because we will populate when we make,
        # capture memory now to be able to include that.
        mem_before = get_memory_usage()
        client = makeOne(bucket_kind)

        begin = perf_counter()
        for _ in range(loops):
            for k, v in ALL_DATA:
                # install a copy that's equal;
                # this should mean no extra copies.
                state = v[0]
                state = state[:-1] + state[-1:]
                new_v = (state, v[1])
                assert new_v == v
                assert new_v is not v
                client[k] = new_v
        duration = perf_counter() - begin
        mem_used = get_memory_usage() - mem_before
        logger.info("Populated in %s; took %s mem; size: %d",
                    duration, byte_display(mem_used), len(client))
        return duration

    def populate_not_equal(loops, bucket_kind):
        # Because we will populate when we make,
        # capture memory now to be able to include that.
        mem_before = get_memory_usage()
        client = makeOne(bucket_kind)

        begin = perf_counter()
        for _ in range(loops):
            for k, v in ALL_DATA:
                # install a copy that's not quite equal.
                # This should require saving it.
                state = v[0]
                state = state + b'1'
                new_v = (state, v[1])
                client[k] = new_v
        duration = perf_counter() - begin
        mem_used = get_memory_usage() - mem_before
        logger.info("Populated in %s; took %s mem; size: %d",
                    duration, byte_display(mem_used), len(client))
        return duration

    def populate_empty(loops, bucket_kind):
        client = makeOne(bucket_kind, populate=False)
        mem_before = get_memory_usage()
        begin = perf_counter()
        for _ in range(loops):
            for k, v in ALL_DATA:
                client[k] = v
        duration = perf_counter() - begin
        mem_used = get_memory_usage() - mem_before
        logger.info("Populated in %s; took %s mem; size: %d",
                    duration, byte_display(mem_used), len(client))
        return duration

    def read(loops, bucket_kind):
        # This is basically the worst-case scenario for a basic
        # segmented LRU: A repeating sequential scan, where no new
        # keys are added and all existing keys fit in the two parts of the
        # cache. Thus, entries just keep bouncing back and forth between
        # probation and protected. It so happens that this is our slowest
        # case.
        client = makeOne(bucket_kind, populate=True)
        begin = perf_counter()
        for _ in range(loops):
            for keys in key_groups:
                for k in keys:
                    res = client[k]
                    #assert len(res) == len(keys)
                    if not res:
                        continue
                    assert res[0] == random_data

        print("Hit ratio: ", client.stats()['ratio'])
        return perf_counter() - begin
        # import pprint
        # pprint.pprint(client._bucket0.stats())
        # print("Probation promotes", client._bucket0._probation.promote_count)
        # print("Probation demotes", client._bucket0._probation.demote_count)
        # print("Probation removes", client._bucket0._probation.remove_count)

    def mixed(loops, bucket_kind):
        client = makeOne(bucket_kind, populate=True)
        hot_keys = key_groups[0]
        i = 0
        miss_count = 0
        begin = perf_counter()
        for _ in range(loops):
            for k, v in ALL_DATA:
                i += 1
                client[k] = v
                if i == len(hot_keys):
                    for hot_key in hot_keys:
                        res = client[hot_key]
                        if not res:
                            miss_count += 1
                    i = 0
        return perf_counter() - begin
    # def mixed_for_stats():
    #     # This is a trivial function that simulates the way
    #     # new keys can come in over time as we reset our checkpoints.
    #     # (Actually, it mostly shows our superiority over the plain LRU;
    #     # that one scored a 0.0 hit ratio, where our segmented LRU scores 1.0)
    #     client.reset_stats()
    #     hot_keys = key_groups[0]
    #     i = 0
    #     for _k, v in ALL_DATA:
    #         i += 1
    #         client._bucket0[str(i)] = v


    #     client.get_multi(hot_keys)

    #     print("Hit ratio", client.stats()['ratio'])

    groups = {}
    for name, bucket in (
            ('SQL', SqlMapping),
            ('CFFI', None),
    ):
        benchmarks = run_and_report_funcs(
            runner,
            ((name + ' pop_eq', populate_equal, bucket),
             (name + ' pop_ne', populate_not_equal, bucket),
             (name + ' epop', populate_empty, bucket),
             (name + ' read', read, bucket),
             (name + ' mix ', mixed, bucket),))
        group = {
            k[len(name) + 1:]: v for k, v in benchmarks.items()
        }
        groups[name] = group

    if not runner.args.worker:
        for name, group in groups.items():
            _combine_benchmark_results(runner.args, name, group, options)


StorageRecord = namedtuple('Record', ['asu', 'lba', 'size', 'opcode', 'ts'])

class StorageTraceSimulator(object):
    # Text trace files can be obtained from http://traces.cs.umass.edu/index.php/Storage/Storage
    # Binary trace files can be obtained from https://github.com/cache2k/cache2k-benchmark

    def _open_file(self, filename, mode='r'):
        if filename.endswith('.bz2'):
            import bz2
            f = bz2.BZ2File(filename, mode)
        else:
            f = open(filename, mode)
        return f

    def _read_binary_records(self, filename, num_clients=8, write_pct=.30,
                             mean_size=10000, stddev_size=512):
        # pylint:disable=too-many-locals
        import struct
        keys = []
        i = 0
        with self._open_file(filename, 'rb') as f:
            while True:
                key = f.read(4)
                if not key:
                    break
                key = struct.unpack(">I", key)[0]
                key = str(key)
                keys.append((i, key))
                i += 1

        random.seed("read_binary_records")
        write_keys = set(random.sample(keys, int(len(keys) * write_pct)))

        records = []
        for key in keys:
            size = int(random.normalvariate(mean_size, stddev_size))
            opcode = 'r'
            if key in write_keys:
                opcode = 'w'
            asu = 1 if num_clients == 1 else random.randrange(num_clients)

            records.append(StorageRecord(asu, key[1], size, opcode, 0.0))
        return records

    def _read_text_records(self, filename):
        from relstorage._compat import intern as _intern

        records = []
        with self._open_file(filename) as f:
            for line in f:
                if isinstance(line, bytes) and str is not bytes:
                    line = line.decode('ascii')
                fields = [x.strip() for x in line.split(",")]
                fields[0] = int(fields[0]) # asu
                try:
                    fields[1] = _intern(fields[1]) # lba
                    fields[2] = int(fields[2]) # size
                    fields[3] = _intern(fields[3].lower()) # opcode
                    fields[4] = float(fields[4]) # ts
                except IndexError:
                    print("Invalid line", line)
                    continue

                records.append(StorageRecord(*fields[:5]))

        return records

    def read_records(self, filename):
        if filename.endswith(".trace"):
            return self._read_binary_records(filename)
        return self._read_text_records(filename)

    def _report_one(self, stats, f, cache_local_mb, begin_time, end_time):
        stats['time'] = end_time - begin_time
        print("{:15s} {:>5s} {:>7s} {:>7s} {:>5s}".format(
            "File", "Limit", "Size", "Time", "Hits"))
        print("{:15s} {:5d} {:7.2f} {:7.2f} {:.3f}".format(
            os.path.basename(f), cache_local_mb,
            stats['bytes'] / 1024 / 1024, stats['time'],
            stats['ratio']))

    def _simulate_local(self, records, cache_local_mb, f):
        from relstorage.cache.local_client import LocalClient
        options = MockOptions()
        options.cache_local_mb = cache_local_mb
        options.cache_local_compression = 'none'
        client = LocalClient(options)

        now = time.time()
        for record in records:
            key = record.lba

            if record.opcode == 'r':
                data = client[key]
                if data is None:
                    # Fill it in from the backend
                    client[key] = b'r' * record.size
            else:
                assert record.opcode == 'w'
                client[key] = b'x' * record.size

        done = time.time()
        stats = client.stats()
        self._report_one(stats, f, cache_local_mb, now, done)

        return stats

    def _simulate_storage(self, records, cache_local_mb, f):
        # pylint:disable=too-many-locals,too-many-statements
        from relstorage.cache.storage_cache import StorageCache
        from relstorage.cache.tests import MockAdapter
        from ZODB.utils import p64

        TRANSACTION_SIZE = 10

        options = MockOptions()
        options.cache_local_mb = cache_local_mb
        options.cache_local_compression = 'none'
        #options.cache_delta_size_limit = 30000
        adapter = MockAdapter()

        # Populate the backend with data, all as of tid 1 Use the size
        # for the first time we see the data, just like
        # _simulate_local does. If we choose a small fixed size, we
        # get much better hit rates than _simulate_local If we use the
        # actual size of the first time we see each record, we use an
        # insane amount of memory even interning the strings
        # (WebSearch3 requires 12GB of memory), so we create just the biggest value
        # and then take memoryviews of it to avoid any copies.

        max_size = 0
        first_sizes = {}
        for record in records:
            max_size = max(record.size, max_size)
            if record.lba in first_sizes:
                continue
            first_sizes[record.lba] = record.size

        # Create one very big value, and then use subviews of a memoryview to reference
        # the same memory

        max_size = max(first_sizes.values())
        biggest_value = b'i' * max_size
        biggest_value = memoryview(biggest_value)

        for lba, size in first_sizes.items():
            oid = int(lba)
            adapter.mover.data[oid] = (biggest_value[:size], 1)
            assert len(adapter.mover.data[oid][0]) == size
        assert len(adapter.mover.data) == len(first_sizes)
        root_cache = StorageCache(adapter, options, None)

        if '--store-trace' in sys.argv:
            class RecordingCache(object):

                def __init__(self, cache):
                    self._cache = cache
                    self.operations = []

                def set_multi(self, data):
                    for k, v in data.items():
                        self.operations.append(('w', k, len(v)))
                    return self._cache.set_multi(data)

                def set(self, k, v):
                    self.operations.append(('w', k, len(v)))
                    self._cache[k] = v

                def get(self, k):
                    self.operations.append(('r', k, -1))
                    return self._cache.get(k)

                def get_multi(self, keys):
                    for k in keys:
                        self.operations.append(('r', k, -1))
                    return self._cache.get_multi(keys)

                def stats(self):
                    return self._cache.stats()

            local_client = root_cache.local_client
            local_client = RecordingCache(local_client)
            root_cache.local_client = root_cache.cache = local_client

        # Initialize to the current TID
        current_tid_int = 2
        root_cache.after_poll(None, 1, current_tid_int, [])

        # Each ASU is a connection, so it has its own storage cache instance.
        asu_caches = {asu: root_cache.new_instance()
                      for asu
                      in set((x.asu for x in records))}

        for cache in asu_caches.values():
            cache.after_poll(None, 0, current_tid_int, [])
            cache.bm_current_tid = current_tid_int
            cache.bm_changes = {}

        now = time.time()

        for record in records:
            oid_int = int(record.lba)
            cache = asu_caches[record.asu]

            # Poll after a certain number of operations, or of we know we would get a
            # conflict.
            if (current_tid_int - cache.bm_current_tid >= TRANSACTION_SIZE
                    or oid_int in cache.bm_changes):
                cache.after_poll(None, cache.bm_current_tid, current_tid_int,
                                 cache.bm_changes.items())
                cache.bm_current_tid = current_tid_int
                cache.bm_changes.clear()

            if record.opcode == 'r':
                cache.load(None, oid_int)
            else:
                assert record.opcode == 'w'
                current_tid_int += 1
                cache.tpc_begin()
                new_state = biggest_value[:record.size]
                cache.store_temp(oid_int, new_state)
                adapter.mover.data[oid_int] = (new_state, current_tid_int)
                cache.after_tpc_finish(p64(current_tid_int))

                for cache in asu_caches.values():
                    cache.bm_changes[oid_int] = current_tid_int

        done = time.time()
        stats = root_cache.stats()
        self._report_one(stats, f, cache_local_mb, now, done)

        if hasattr(root_cache.local_client, 'operations'):
            with open(f + '.' + str(options.cache_local_mb) + '.ctrace', 'w') as fp:
                for o in root_cache.local_client.operations:
                    fp.write("%s,%s,%d\n" % o)
        return stats

    def simulate(self, s_type='local'):
        meth = getattr(self, '_simulate_' + s_type)

        def _print(size, records):
            print("Simulating", len(records),
                  "operations (reads:", (len([x for x in records if x.opcode == 'r'])),
                  "writes:", (len([x for x in records if x.opcode == 'w'])), ")",
                  "to", len(set(x.lba for x in records)), "distinct keys",
                  "from", len(set((x.asu for x in records))), "connections",
                  "with cache limit", size)

        filename = sys.argv[2]
        filename = os.path.abspath(os.path.expanduser(filename))
        if os.path.isdir(filename):
            all_stats = []
            for f in sorted(os.listdir(filename)):
                records = self.read_records(os.path.join(filename, f))
                for size in (100, 512, 1024):
                    _print(size, records)
                    stats = meth(records, size, f)
                    all_stats.append((f, size, stats))

            print("{:15s} {:>5s} {:>7s} {:>7s} {:>5s}".format(
                "File", "Limit", "Size", "Time", "Hits"))
            for f, size, stats in all_stats:
                print("{:15s} {:5d} {:7.2f} {:7.2f} {:.3f}".format(
                    os.path.basename(f), size, stats['bytes'] / 1024 / 1024,
                    stats['time'], stats['ratio']))

        else:
            size = int(sys.argv[3])
            records = self.read_records(filename)
            _print(size, records)
            pr = cProfile.Profile()
            pr.enable()
            meth(records, size, filename)
            pr.disable()
            ps = pstats.Stats(pr).sort_stats('cumulative')
            ps.print_stats(.4)


def save_load_benchmark(runner):
    # pylint:disable=too-many-locals,too-many-statements
    import io
    from relstorage.cache.lru_sqlite import SqlMapping
    from relstorage.cache.mapping import SizedLRUMapping
    from relstorage.cache import persistence as _Loader
    from relstorage.cache.local_client import LocalClient

    import itertools

    sys.setrecursionlimit(500000)


    cache_pfx = "pfx"
    cache_options = MockOptions()
    cache_options.cache_local_dir = runner.args.temp #'/tmp'
    cache_options.cache_local_dir_compress = False
    cache_options.cache_local_mb = 525
    cache_options.cache_local_storage = SqlMapping

    # Note use of worker task number: This is fragile and directly relates to
    # the order in which we pass functions to run_and_report_funcs
    def create_and_populate_client():
        client = LocalClient(cache_options, cache_pfx)
        # Monkey with the internals so we don't have to
        # populate multiple times.
        bucket = client._bucket0
        #print("Testing", type(bucket._dict))

        # Only need to populate in the workers.
        size_dists = [100] * 800 + [300] * 500 + [1024] * 300 + [2048] * 200 + [4096] * 150

        with open('/dev/urandom', 'rb') as rnd:
            data = [rnd.read(x) for x in size_dists]
        data_iter = itertools.cycle(data)
        keys_and_values = []
        len_values = 0
        j = 0
        for j, datum in enumerate(data_iter):
            if len(datum) > bucket.limit or len_values + len(datum) > bucket.limit:
                break
            len_values += len(datum)
            # To ensure the pickle memo cache doesn't just write out "use object X",
            # but distinct copies of the strings, we need to copy them
            keys_and_values.append(
                ((j, j), (datum[:-1] + b'x', j))
            )

            # # We need to get the item so its frequency goes up enough to be written
            # # (this is while we're doing an aging at write time, which may go away).
            # # Using an assert statement causes us to write nothing if -O is used.
            # if bucket[(j, j)] is datum:
            #     raise AssertionError()
        mem_before = get_memory_usage()
        bucket.bulk_update(keys_and_values, mem_usage_before=mem_before)
        client.store_checkpoints(j, j)
        del keys_and_values
        assert len(bucket) > 0 # pylint:disable=len-as-condition
        assert len(bucket) == len(client)
        #print("Len", len(bucket), "size", bucket.size, "checkpoints", client.get_checkpoints())
        return client, bucket

    def _open(fd, mode):
        return io.open(fd, mode, buffering=16384)


    def write_mapping():
        if not runner.args.do_stream:
            return 3

        import tempfile
        try:
            os.makedirs(runner.args.temp)
        except OSError:
            pass
        _, bucket = create_and_populate_client()
        prefix = 'relstorage-cache-' + cache_pfx + '.'
        suffix = '.T'
        fd, _ = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=runner.args.temp)
        with _open(fd, 'wb') as f:
            begin = perf_counter()
            bucket.write_to_stream(f, pickle_fast=True)
            end = perf_counter()
        return end - begin

    def load_mapping():
        if not runner.args.do_stream:
            return 3
        load_from_file = write_mapping() # XXX: Broken

        begin = perf_counter()
        b2 = SizedLRUMapping(bucket.limit) # pylint:disable=undefined-variable
        with _open(load_from_file, 'rb') as f:
            b2.read_from_stream(f)
        end = perf_counter()
        return end - begin

    def write_client():
        client, _ = create_and_populate_client()
        begin = perf_counter()
        client.save(overwrite=True)
        end = perf_counter()
        return end - begin

    def write_client_dups():
        client, _ = create_and_populate_client()
        begin = perf_counter()
        client.save(overwrite=False, close_async=False)
        end = perf_counter()
        return end - begin

    def read_client():
        begin = perf_counter()
        c2 = LocalClient(cache_options, cache_pfx)
        c2.restore()
        end = perf_counter()
        return end - begin

    benchmarks = run_and_report_funcs(runner, (
        ('write stream', write_mapping),
        ('read stream', load_mapping),
        ('write client fresh', write_client),
        ('write client dups', write_client_dups),
        ('read client', read_client),
    ))

    if not runner.args.worker:
        stream = {'write': benchmarks['write stream'],
                  'read': benchmarks['read stream']}
        db = {'write': benchmarks['write client fresh'],
              'read': benchmarks['read client']}
        db_dups = {'write': benchmarks['write client dups'],
                   'read': benchmarks['read client']}
        _combine_benchmark_results(runner.args, 'stream', stream, cache_options)
        _combine_benchmark_results(runner.args, 'db', db, cache_options)
        _combine_benchmark_results(runner.args, 'db_dup', db_dups, cache_options)


def main():
    import pyperf
    import tempfile

    temp = None

    def args_hook(cmd, args):
        cmd.extend(('--type', args.type))
        if args.profile:
            cmd.extend(('--profile',))
        if args.do_stream:
            cmd.extend(('--do-stream',))
        if args.log:
            cmd.extend(('--log',))
        cmd.extend(('--temp', temp))
    runner = pyperf.Runner(add_cmdline_args=args_hook)
    runner.argparser.add_argument(
        '--type',
        default='io',
        choices=['local', 'io', 'simlocal', 'simstorage']
    )
    runner.argparser.add_argument(
        '--temp'
    )
    runner.argparser.add_argument(
        '--profile', action='store_true',
    )
    runner.argparser.add_argument(
        '--log', action='store_true'
    )
    runner.argparser.add_argument(
        '--do-stream', action='store_true',
    )
    runner.parse_args()
    kind = runner.args.type
    temp = runner.args.temp
    need_cleanup = False
    if not temp:
        temp = tempfile.mkdtemp('.rsbench')
        need_cleanup = True
    if runner.args.log:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    if kind == 'local':
        local_benchmark(runner)
    elif kind == 'io':
        save_load_benchmark(runner)
    elif kind == 'simlocal':
        StorageTraceSimulator().simulate('local')
    else:
        assert kind == 'simstorage'
        #import logging
        #logging.basicConfig(level=logging.DEBUG)

        StorageTraceSimulator().simulate('storage')

    if not runner.args.worker and need_cleanup:
        # Master process cleanup
        import shutil
        shutil.rmtree(temp)

if __name__ == '__main__':
    main()
