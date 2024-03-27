const std = @import("std");

// NOTE I ran some benchmarks to identify these load thresholds
// basically, lowering the average load makes the iteration slower
// and these values produce reasonable across the board performance.
// upping the bucket load max and bucket merge max by another 0.1 might improve it slightly
// however, I'm hesitant, since the linear probing could start to struggle for loads that high
// which could lead to performance degradation in some edge cases that are hard to test
const BUCKET_LOAD_MAX = 0.8;
const BUCKET_MERGE_MAX = 0.6;
const STORAGE_LOAD_MAX = 0.7;
const STORAGE_LOAD_MIN = 0.3;

// this is to ensure that we hash differently between the top level linear hashing and the buckets
// as otherwise, the linear hashing stage will bias towards collisions in the buckets
const PRIME_A = 2654435741;
const PRIME_B = 2654435789;

fn BucketPrototype(comptime K: type, comptime SIZE: comptime_int) type {
    std.debug.assert(SIZE <= 65536);
    return struct {
        const Self = @This();

        keys: [SIZE]K,
        vals: usize,
        skip: [SIZE + 1]u16,
        lens: [SIZE]u8,
        len: usize,
        next: ?*Self,
        size: usize,
        mask: u32,
        merge_max: usize,
        load_max: usize,
    };
}

fn IndexPrototype(comptime SIZE: comptime_int) type {
    return struct {
        buckets: [SIZE]*anyopaque,
    };
}

fn PageImpl(comptime SIZE: comptime_int, comptime ALIGN: comptime_int) type {
    return struct {
        bytes: [SIZE]u8 align(ALIGN),

        fn arrayMaxSize(comptime T: type) comptime_int {
            std.debug.assert(@alignOf(T) <= ALIGN);
            var n = 65536;
            while (@sizeOf([n]T) > SIZE) : (n >>= 1) {}
            std.debug.assert(n > 0);
            return n;
        }

        fn bucketMaxSize(comptime T: type) comptime_int {
            comptime {
                std.debug.assert(@alignOf(T) <= ALIGN);
                var n = 65536;
                while (@sizeOf(BucketPrototype(T, n)) > SIZE) : (n >>= 1) {}
                std.debug.assert(n > 0);
                return n;
            }
        }

        fn indexMaxSize() comptime_int {
            comptime {
                std.debug.assert(@alignOf(*anyopaque) <= ALIGN);
                var n = 1;
                while (@sizeOf(IndexPrototype(n)) < SIZE) : (n <<= 1) {}
                std.debug.assert(n > 1);
                return n - 1;
            }
        }
    };
}

// keep the same bucket design
// a fixed size hash-table that chains into more hash tables on overflow
fn BucketImpl(
    comptime PAGE_SIZE: comptime_int,
    comptime PAGE_ALIGN: comptime_int,
    comptime K: type,
    comptime nil: K,
) type {
    const Page = PageImpl(PAGE_SIZE, PAGE_ALIGN);
    const MAX_SIZE = Page.bucketMaxSize(K);

    // const LOAD_MAX = @as(comptime_int, @intFromFloat(BUCKET_LOAD_MAX * MAX_SIZE));
    // const MERGE_MAX = @as(comptime_int, @intFromFloat(BUCKET_MERGE_MAX * SIZE));

    // std.debug.assert(LOAD_MAX < SIZE); // avoids possible pitfalls in deletion

    return struct {
        const Self = @This();

        fn SIZE(comptime V: type) comptime_int {
            const n = @min(Page.arrayMaxSize(V), MAX_SIZE);
            std.debug.assert(std.math.isPowerOfTwo(n));
            return n;
        }

        keys: [MAX_SIZE]K,
        vals: usize, // [*]V and *Page
        skip: [MAX_SIZE + 1]u16, // indicates jumps while iterating
        lens: [MAX_SIZE]u8, // probe lengths for robin-hood
        len: usize,
        next: ?*Self, // for chaining to another bucket in case load is exceeded

        // number of items in vals, i.e. actual size
        // we also know that size is power of two, meaning modulo can be simplified to a masking
        // so we should also store that mask to speed up access
        // NOTE: it cannot be part of the type, because the type must be independent on V
        size: usize,
        mask: u32,
        merge_max: usize,
        load_max: usize,

        fn create(comptime V: type, alloc: std.mem.Allocator) !*Self {
            std.debug.assert(@sizeOf(Self) <= PAGE_SIZE);
            std.debug.assert(@sizeOf([SIZE(V)]V) <= PAGE_SIZE);
            std.debug.assert(@alignOf(Self) <= PAGE_ALIGN);
            std.debug.assert(@alignOf([SIZE(V)]V) <= PAGE_ALIGN);

            var bucket: *Self = @ptrCast(try alloc.create(Page));
            errdefer alloc.destroy(bucket);
            bucket.vals = @intFromPtr(try alloc.create(Page));
            errdefer alloc.destroy(@as(*Page, @ptrFromInt(bucket.vals)));

            bucket.size = SIZE(V);
            bucket.mask = @as(u32, @intCast(bucket.size)) - 1;
            bucket.merge_max = @intFromFloat(
                BUCKET_MERGE_MAX * @as(f64, @floatFromInt(bucket.size)),
            );
            bucket.load_max = @intFromFloat(
                BUCKET_LOAD_MAX * @as(f64, @floatFromInt(bucket.size)),
            );

            bucket.keys = .{nil} ** MAX_SIZE;
            bucket.len = 0;
            bucket.next = null;

            // https://plflib.org/matt_bentley_-_the_high_complexity_jump-counting_pattern.pdf
            // for real though, this paper is so well explained. thanks Matt :D
            for (1..bucket.size) |i| {
                bucket.skip[i] = @intCast(i + 1);
            }
            bucket.skip[0] = @intCast(bucket.size);
            bucket.skip[bucket.size] = 0; // this field just removes a branch during iteration

            return bucket;
        }

        fn destroy(bucket: *Self, alloc: std.mem.Allocator) void {
            if (bucket.next) |next| next.destroy(alloc);
            alloc.destroy(@as(*Page, @ptrFromInt(bucket.vals)));
            alloc.destroy(@as(*Page, @ptrCast(@alignCast(bucket))));
        }

        /// key must not be present
        fn add(bucket: *Self, comptime V: type, alloc: std.mem.Allocator, key: K, val: V) !void {
            std.debug.assert(key != nil);

            if (bucket.len > bucket.load_max) {
                if (bucket.next == null) {
                    bucket.next = try Self.create(V, alloc);
                }
                return bucket.next.?.add(V, alloc, key, val);
            }

            var loc = hash(key) & bucket.mask;

            // while (bucket.keys[loc] != nil) : (loc = (loc + 1) & bucket.mask) {
            //     std.debug.assert(bucket.keys[loc] != key);
            // }
            // bucket.keys[loc] = key;
            // @as([*]V, @ptrFromInt(bucket.vals))[loc] = val;

            var walk_key = key;
            var walk_val = val;
            var walk_dst: u8 = 0;
            const vals = @as([*]V, @ptrFromInt(bucket.vals));
            while (walk_dst < bucket.size) {
                if (bucket.keys[loc] == nil) {
                    bucket.keys[loc] = walk_key;
                    vals[loc] = walk_val;
                    bucket.lens[loc] = walk_dst;
                    break;
                } else if (bucket.lens[loc] < walk_dst) {
                    const tmp_key = bucket.keys[loc];
                    const tmp_val = vals[loc];
                    const tmp_dst = bucket.lens[loc];
                    bucket.keys[loc] = walk_key;
                    vals[loc] = walk_val;
                    bucket.lens[loc] = walk_dst;
                    walk_key = tmp_key;
                    walk_val = tmp_val;
                    walk_dst = tmp_dst;
                }
                loc = (loc + 1) & bucket.mask;
                walk_dst += 1;
            }

            bucket.setUnskip(loc);
            bucket.len += 1;
        }

        fn get(bucket: *Self, comptime V: type, key: K) ?*V {
            std.debug.assert(key != nil);

            var loc = hash(key) & bucket.mask;
            while (true) {
                // for (0..bucket.size) |_| {
                if (bucket.keys[loc] == key) {
                    return &@as([*]V, @ptrFromInt(bucket.vals))[loc];
                } else if (bucket.keys[loc] == nil) {
                    break;
                }
                loc = (loc + 1) & bucket.mask;
            }

            // if we didn't find the key here, check the overflow chain
            if (bucket.next != null) {
                return bucket.next.?.get(V, key);
            }
            return null;
        }

        fn has(bucket: *Self, key: K) bool {
            std.debug.assert(key != nil);

            var loc = hash(key) & bucket.mask;
            while (true) {
                // for (0..bucket.size) |_| {
                if (bucket.keys[loc] == key) {
                    return true;
                } else if (bucket.keys[loc] == nil) {
                    break;
                }
                loc = (loc + 1) & bucket.mask;
            }

            // if we didn't find the key here, check the overflow chain
            if (bucket.next != null) {
                return bucket.next.?.has(key);
            }
            return false;
        }

        /// key must be present
        fn del(bucket: *Self, comptime V: type, alloc: std.mem.Allocator, key: K) void {
            std.debug.assert(key != nil);

            var loc = hash(key) & bucket.mask;
            for (0..bucket.size) |_| {
                if (bucket.keys[loc] == key) {
                    bucket.len -= 1;
                    // found the key, delete the entry
                    var ix_remove = loc;
                    var ix_shift = ix_remove;
                    var dist: u32 = 1;

                    while (true) {
                        ix_shift = (ix_shift + 1) & bucket.mask;
                        if (bucket.keys[ix_shift] == nil) {
                            bucket.keys[ix_remove] = nil;
                            bucket.setSkip(ix_remove);
                            return;
                        }
                        const key_dist = (ix_shift -% hash(bucket.keys[ix_shift])) & bucket.mask;
                        if (key_dist >= dist) {
                            bucket.keys[ix_remove] = bucket.keys[ix_shift];
                            const vals: [*]V = @ptrFromInt(bucket.vals);
                            vals[ix_remove] = vals[ix_shift];
                            ix_remove = ix_shift;
                            dist = 1;
                        } else {
                            dist += 1;
                        }
                    }

                    unreachable;
                } else if (bucket.keys[loc] == nil) {
                    break;
                }
                loc = (loc + 1) & bucket.mask;
            }

            // we didn't find the key here, check the overflow chain
            if (bucket.next) |next| {
                next.del(V, alloc, key);

                if (next.len == 0) {
                    bucket.next = next.next;
                    next.next = null;
                    next.destroy(alloc);
                } else if (bucket.len + next.len < bucket.merge_max) {
                    bucket.next = next.next;
                    next.next = null;
                    var it = next.keyiter();
                    while (it.next()) |k| {
                        const v = next.get(V, k).?.*;
                        bucket.add(V, undefined, k, v) catch unreachable;
                    }
                    next.destroy(alloc);
                }

                return;
            }

            unreachable;
        }

        /// append entire chain buckets in other to bucket
        fn merge(bucket: *Self, other: *Self) void {
            var final = bucket;
            while (final.next) |next| final = next;
            final.next = other;
        }

        const KeyIterator = struct {
            bucket: ?*Self,
            cursor: usize = 0,

            pub fn next(it: *KeyIterator) ?K {
                if (it.bucket == null) return null;
                if (it.cursor >= it.bucket.?.size) {
                    it.bucket = it.bucket.?.next;
                    if (it.bucket != null) {
                        // it.cursor = 0;
                        it.cursor = it.bucket.?.skip[0];
                    }
                    return it.next();
                }

                const result = it.bucket.?.keys[it.cursor];
                it.cursor += 1;
                // while (it.bucket.?.keys[it.cursor] == nil) : (it.cursor += 1) {}
                it.cursor += it.bucket.?.skip[it.cursor];
                return result;
            }
        };

        pub fn keyiter(bucket: *Self) KeyIterator {
            return .{
                .bucket = bucket,
                .cursor = bucket.skip[0],
            };
        }

        fn hash(key: K) u32 {
            return std.hash.XxHash32.hash(PRIME_B, std.mem.asBytes(&key));
        }

        fn setSkip(bucket: *Self, loc: usize) void {
            std.debug.assert(loc < bucket.size);
            std.debug.assert(bucket.skip[loc] == 0);

            const l = if (loc > 0) bucket.skip[loc - 1] else 0;
            const r = bucket.skip[loc + 1];

            if (l + r == 0) {
                bucket.skip[loc] = 1;
            } else if (r == 0) {
                bucket.skip[loc] = 1 + bucket.skip[loc - 1];
                const y = loc - bucket.skip[loc - 1];
                bucket.skip[y] = bucket.skip[loc];
            } else if (l == 0) {
                var x = bucket.skip[loc + 1];
                bucket.skip[loc] = x + 1;
                var j: u16 = 1;
                while (x > 0) {
                    bucket.skip[loc + j] = j + 1;
                    j += 1;
                    x -= 1;
                }
            } else {
                var x = 1 + bucket.skip[loc + 1];
                var y = bucket.skip[loc - 1];
                bucket.skip[loc - y] += x;
                y += 1;
                var j = loc;
                while (x > 0) {
                    bucket.skip[j] = y;
                    j += 1;
                    y += 1;
                    x -= 1;
                }
            }
        }

        fn setUnskip(bucket: *Self, loc: usize) void {
            std.debug.assert(loc < bucket.size);
            std.debug.assert(bucket.skip[loc] != 0);

            const l = if (loc > 0) bucket.skip[loc - 1] else 0;
            const r = bucket.skip[loc + 1];

            if (l + r == 0) {
                std.debug.assert(bucket.skip[loc] == 1);
                bucket.skip[loc] = 0;
            } else if (r == 0) {
                const x = bucket.skip[loc] - 1;
                bucket.skip[loc - x] = x;
                bucket.skip[loc] = 0;
            } else if (l == 0) {
                var x = bucket.skip[loc] - 1;
                bucket.skip[loc] = 0;
                bucket.skip[loc + 1] = x;
                x -= 1;
                var j: u16 = 2;
                while (x > 0) {
                    bucket.skip[loc + j] = j;
                    j += 1;
                    x -= 1;
                }
            } else {
                // phase 1
                const y = bucket.skip[loc];
                const z = loc - (y - 1);
                var x = bucket.skip[z] - y;
                bucket.skip[loc + 1] = x;
                // phase 2
                bucket.skip[z] = y - 1;
                x -= 1;
                bucket.skip[loc] = 0;
                // phase 3
                var j: u16 = 2;
                while (x > 0) {
                    bucket.skip[loc + j] = j;
                    j += 1;
                    x -= 1;
                }
            }
        }

        fn debugPrint(bucket: *Self) void {
            std.debug.print(" [ ", .{});
            for (0..bucket.size) |i| {
                if (bucket.keys[i] == nil) continue;
                std.debug.print("{} ", .{bucket.keys[i]});
            }
            if (bucket.next) |next| {
                std.debug.print("] ->", .{});
                next.debugPrint();
            } else {
                std.debug.print("]\n", .{});
            }
        }
    };
}

test "bucket fuzz (add, get & del)" {
    for (0..50) |_| {
        const p = try BucketImpl(16384, 64, u32, std.math.maxInt(u32))
            .create(u32, std.testing.allocator);
        defer p.destroy(std.testing.allocator);
        var h = std.AutoHashMap(u32, u32).init(std.testing.allocator);
        defer h.deinit();

        var rng = std.rand.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        var total: i64 = 0;
        for (0..10000) |_| {
            const x = rng.random().int(u16); // so we avoid our nil key
            if (h.contains(x)) {
                try std.testing.expectEqual(h.contains(x), p.has(x));
                try std.testing.expectEqual(h.getPtr(x).?.*, p.get(u32, x).?.*);
                total -= x + @as(i64, @intCast(p.get(u32, x).?.*));
                p.del(u32, std.testing.allocator, x);
                _ = h.remove(x);
            } else {
                try std.testing.expectEqual(null, h.getPtr(x)); // not like i need to test but w/e
                try std.testing.expectEqual(null, p.get(u32, x));
                try p.add(u32, std.testing.allocator, x, x);
                try h.put(x, x);
                total += 2 * @as(i64, @intCast(x));
            }
        }

        var sum_h: i64 = 0;
        var it_h = h.iterator();
        while (it_h.next()) |kv| {
            sum_h += kv.key_ptr.* + kv.value_ptr.*;
        }
        try std.testing.expectEqual(total, sum_h);
        {
            var sum_p: i64 = 0;
            var it_p = p.keyiter();
            while (it_p.next()) |k| {
                const v = p.get(u32, k).?.*;
                sum_p += k + v;
            }
            try std.testing.expectEqual(total, sum_p);
        }
        {
            var sum_p: i64 = 0;
            var it_p = p.iter(u32);
            while (it_p.next()) |kv| {
                sum_p += kv.key + kv.val_ptr.*;
            }
            try std.testing.expectEqual(total, sum_p);
        }
    }
}

pub fn StorageImpl(
    comptime PAGE_SIZE: comptime_int,
    comptime PAGE_ALIGN: comptime_int,
    comptime K: type,
    comptime nil: K,
) type {
    const Page = PageImpl(PAGE_SIZE, PAGE_ALIGN);
    const INDEX_SIZE = Page.indexMaxSize();

    return struct {
        const Self = @This();

        pub const Bucket = BucketImpl(PAGE_SIZE, PAGE_ALIGN, K, nil);

        pub const Index = struct {
            buckets: [INDEX_SIZE]?*Bucket,
        };

        alloc: std.mem.Allocator,
        len: usize,
        index: ?*Index,
        split: usize, // marks the bucket that should be split next
        round: usize,
        n_buckets: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            const storage = Self{
                .alloc = alloc,
                .len = 0,
                .index = null,
                .split = 0,
                .round = 0,
                .n_buckets = 0,
            };
            return storage;
        }

        pub fn deinit(storage: *Self) void {
            if (storage.index) |index| {
                for (0..storage.n_buckets) |i| {
                    index.buckets[i].?.destroy(storage.alloc);
                }
                storage.alloc.destroy(index);
            }
            storage.* = undefined;
        }

        pub fn get(storage: *Self, comptime V: type, key: K) ?*V {
            std.debug.assert(key != nil);

            if (storage.index) |index| {
                const loc = storage.bucket(key);
                return index.buckets[loc].?.get(V, key);
            }

            return null;
        }

        pub fn has(storage: *Self, key: K) bool {
            std.debug.assert(key != nil);

            if (storage.index) |index| {
                const loc = storage.bucket(key);
                return index.buckets[loc].?.has(key);
            }

            return false;
        }

        // key must not be in storage
        pub fn add(storage: *Self, comptime V: type, key: K, val: V) void {
            std.debug.assert(key != nil);

            if (storage.index) |index| {
                const loc = storage.bucket(key);
                index.buckets[loc].?.add(V, storage.alloc, key, val) catch @panic("oom");
                storage.len += 1;

                if (storage.load() > STORAGE_LOAD_MAX and storage.n_buckets < INDEX_SIZE) {
                    const splitting = index.buckets[storage.split].?;
                    // TODO test to make sure that if allocation fails
                    // it does not break the datastructure
                    index.buckets[storage.split] = Bucket.create(V, storage.alloc) catch
                        @panic("oom");
                    index.buckets[storage.n_buckets] = Bucket.create(V, storage.alloc) catch
                        @panic("oom");
                    storage.split += 1;
                    storage.n_buckets += 1;

                    var it = splitting.keyiter();
                    while (it.next()) |k| {
                        const v = splitting.get(V, k).?.*;
                        const new_loc = storage.bucket(k);
                        std.debug.assert(new_loc == storage.split - 1 or
                            new_loc == storage.n_buckets - 1);
                        // if these allocations fail, the earlier errdefers will kill the chains
                        // so we don't need to specifically dealloc these
                        index.buckets[new_loc].?.add(V, storage.alloc, k, v) catch
                            @panic("oom");
                    }
                    splitting.destroy(storage.alloc);

                    if (storage.split == (@as(usize, 1) << @intCast(storage.round))) {
                        storage.round += 1;
                        storage.split = 0;
                    }
                }
            } else {
                storage.index = storage.alloc.create(Index) catch @panic("oom");
                storage.index.?.buckets = .{null} ** INDEX_SIZE;
                storage.index.?.buckets[0] = Bucket.create(V, storage.alloc) catch @panic("oom");

                storage.n_buckets = 1;
                return storage.add(V, key, val);
            }
        }

        // key must be in storage
        pub fn del(storage: *Self, comptime V: type, key: K) void {
            std.debug.assert(key != nil);
            std.debug.assert(storage.has(key));

            const index = storage.index.?;
            const loc = storage.bucket(key);
            index.buckets[loc].?.del(V, storage.alloc, key);
            storage.len -= 1;

            if (storage.load() > STORAGE_LOAD_MIN) return;
            std.debug.assert(storage.n_buckets > 0);

            if (storage.n_buckets > 1) {
                const merging = index.buckets[storage.n_buckets - 1].?;
                index.buckets[storage.n_buckets - 1] = null;

                if (storage.split > 0) {
                    storage.split -= 1;
                } else {
                    storage.split = (@as(usize, 1) << @intCast(storage.round - 1)) - 1;
                    storage.round -= 1;
                }
                storage.n_buckets -= 1;
                index.buckets[storage.split].?.merge(merging);
            } else if (storage.len == 0) {
                // we have deleted everything, reset to init state
                index.buckets[0].?.destroy(storage.alloc);
                storage.alloc.destroy(index);
                storage.* = Self.init(storage.alloc);
            }
        }

        fn load(storage: *Self) f64 {
            return if (storage.n_buckets > 0)
                @as(f64, @floatFromInt(storage.len)) /
                    @as(f64, @floatFromInt(storage.index.?.buckets[0].?.size * storage.n_buckets))
            else
                return 0.0;
        }

        fn hash(key: K) u32 {
            return std.hash.XxHash32.hash(PRIME_A, std.mem.asBytes(&key));
        }

        fn bucket(storage: *Self, key: K) usize {
            const h = hash(key);
            var loc = h & ((@as(usize, 1) << @intCast(storage.round)) - 1);
            if (loc < storage.split) { // i wonder if this branch predicts well
                loc = h & ((@as(usize, 1) << (@intCast(storage.round + 1))) - 1);
            }
            return loc;
        }

        pub const KeyIterator = struct {
            storage: *Self,
            cursor: usize,
            bucket_it: ?Bucket.KeyIterator,

            pub fn next(it: *KeyIterator) ?K {
                if (it.bucket_it != null) {
                    if (it.bucket_it.?.next()) |k| {
                        return k;
                    }

                    if (it.cursor < it.storage.n_buckets - 1) {
                        it.cursor += 1;
                        it.bucket_it = it.storage.index.?.buckets[it.cursor].?.keyiter();
                        return it.next();
                    }

                    it.bucket_it = null;
                }

                return null;
            }
        };

        pub fn keyiter(storage: *Self) KeyIterator {
            return .{
                .storage = storage,
                .cursor = 0,
                .bucket_it = if (storage.index) |index| index.buckets[0].?.keyiter() else null,
            };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print(
                "Storage <{s}> len: {} load: {}\n",
                .{ @typeName(K), storage.len, storage.load() },
            );
            if (storage.index) |index| {
                for (0..storage.n_buckets) |i| {
                    if (i == storage.split) {
                        std.debug.print("*", .{});
                    } else {
                        std.debug.print(" ", .{});
                    }
                    index.buckets[i].?.debugPrint();
                }
            }
        }
    };
}

test "storage fuzz (add, get & del)" {
    for (0..25) |_| {
        var s = StorageImpl(16 * 1024, 64, u32, std.math.maxInt(u32))
            .init(std.testing.allocator);
        defer s.deinit();
        var h = std.AutoHashMap(u32, u32).init(std.testing.allocator);
        defer h.deinit();

        var rng = std.rand.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        var total: i64 = 0;
        for (0..10000) |_| {
            const x = rng.random().int(u16); // so we avoid our nil key
            if (h.contains(x)) {
                try std.testing.expectEqual(h.contains(x), s.has(x));
                try std.testing.expectEqual(h.getPtr(x).?.*, s.get(u32, x).?.*);
                total -= x + @as(i64, @intCast(s.get(u32, x).?.*));
                s.del(u32, x);
                _ = h.remove(x);
            } else {
                try std.testing.expectEqual(null, h.getPtr(x)); // not like i need to test but w/e
                try std.testing.expectEqual(null, s.get(u32, x));
                s.add(u32, x, x);
                try h.put(x, x);
                total += 2 * @as(i64, @intCast(x));
            }
        }

        var sum_h: i64 = 0;
        var it_h = h.iterator();
        while (it_h.next()) |kv| {
            sum_h += kv.key_ptr.* + kv.value_ptr.*;
        }
        try std.testing.expectEqual(total, sum_h);
        {
            var sum_s: i64 = 0;
            var it_s = s.keyiter();
            while (it_s.next()) |k| {
                const v = s.get(u32, k).?.*;
                sum_s += k + v;
            }
            try std.testing.expectEqual(total, sum_s);
        }
        {
            var sum_s: i64 = 0;
            var it_s = s.iter(u32);
            while (it_s.next()) |kv| {
                sum_s += kv.key + kv.val_ptr.*;
            }
            try std.testing.expectEqual(total, sum_s);
        }
    }
}

pub fn main() void {}
