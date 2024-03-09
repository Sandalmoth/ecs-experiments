const std = @import("std");

// sadly, the spiral storage has issues with floating point accuracy
// and I don't know how to fix them, nor can I come up with a way of distributing the
// records with integer arithmetic only

// the performance was great though
// and the most similar alternative is linear hashing
// so let's try that

// we need to use different hash functions for the buckets and the index
// a simple way to do that is to multiply by a large prime before hashing
// which seems to perform ok in practice
const PRIME_A = 2654435761;
const PRIME_B = 2654435789;

// keep the same bucket design
// a fixed size hash-table that chains into more hash tables on overflow
fn BucketImpl(comptime K: type, comptime V: type, comptime SIZE: comptime_int) type {
    if (!std.math.isPowerOfTwo(SIZE)) {
        @compileLog("BucketImpl.SIZE ought to be power of two");
    }

    const nil = std.math.maxInt(K);
    // note, this LOAD_MAX is completely independent of the one in Storage
    // this is about when to chain the buckets to fit more data in them
    const LOAD_MAX = @as(comptime_int, @intFromFloat(0.9 * SIZE));

    return struct {
        const Self = @This();

        keys: [SIZE]K,
        vals: [SIZE]V,
        // skip: [SIZE]u16,
        len: usize,
        next: ?*Self, // for chaining to another bucket in case load is exceeded

        fn create(alloc: std.mem.Allocator) !*Self {
            var bucket = try alloc.create(Self);
            bucket.keys = .{nil} ** SIZE;
            bucket.len = 0;
            bucket.next = null;
            return bucket;
        }

        fn destroy(bucket: *Self, alloc: std.mem.Allocator) void {
            if (bucket.next) |next| next.destroy(alloc);
            alloc.destroy(bucket);
        }

        fn add(bucket: *Self, alloc: std.mem.Allocator, key: K, val: V) !void {
            if (bucket.len > LOAD_MAX) {
                if (bucket.next == null) {
                    bucket.next = try Self.create(alloc);
                }
                return bucket.next.?.add(alloc, key, val);
            }

            var loc = std.hash.uint32(key *% PRIME_B) % SIZE;

            while (bucket.keys[loc] != nil) : (loc = (loc + 1) % SIZE) {
                std.debug.assert(bucket.keys[loc] != key);
            }
            bucket.keys[loc] = key;
            bucket.vals[loc] = val;
            bucket.len += 1;
        }

        fn get(bucket: *Self, key: K) ?*V {
            var loc = std.hash.uint32(key *% PRIME_B) % SIZE;

            var result: ?*V = null;
            for (0..SIZE) |_| {
                if (bucket.keys[loc] == key) {
                    result = &bucket.vals[loc];
                    break;
                } else if (bucket.keys[loc] == nil) {
                    break;
                }
                loc = (loc + 1) % SIZE;
            }

            // if we didin't find the key here, check the overflow chain
            if (result == null and bucket.next != null) {
                result = bucket.next.?.get(key);
            }

            return result;
        }

        // we can almost surely speed up the bucket iterator
        // like with the skipfields, or by doing what std.ArrayHasMap does
        const Iterator = struct {
            const KV = struct { key: K, val: V };

            bucket: ?*Self,
            cursor: usize = 0,

            pub fn next(it: *Iterator) ?KV {
                if (it.bucket == null) return null;

                while (it.cursor < SIZE and
                    it.bucket.?.keys[it.cursor] == nil) : (it.cursor += 1)
                {}
                if (it.cursor == SIZE) {
                    it.bucket = it.bucket.?.next;
                    it.cursor = 0;
                    return it.next();
                }

                it.cursor += 1;
                return .{
                    .key = it.bucket.?.keys[it.cursor - 1],
                    .val = it.bucket.?.vals[it.cursor - 1],
                };
            }
        };

        pub fn iterator(bucket: *Self) Iterator {
            return .{ .bucket = bucket };
        }

        fn debugPrint(bucket: *Self) void {
            std.debug.print(" [ ", .{});
            for (0..SIZE) |i| {
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

test "bucket fuzz" {
    const p = try BucketImpl(u32, u32, 16).create(std.testing.allocator);
    defer p.destroy(std.testing.allocator);
    var h = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer h.deinit();

    var rng = std.rand.DefaultPrng.init(@intCast(std.time.microTimestamp()));
    for (0..10000) |_| {
        const x = rng.random().int(u16); // so we avoid our nil key
        if (h.contains(x)) {
            try std.testing.expectEqual(h.getPtr(x).?.*, p.get(x).?.*);
        } else {
            try std.testing.expectEqual(null, h.getPtr(x)); // not like i need to test but w/e
            try std.testing.expectEqual(null, p.get(x));
            try p.add(std.testing.allocator, x, x);
            try h.put(x, x);
        }
    }
}

pub fn Storage(
    comptime K: type,
    comptime V: type,
    comptime INDEX_SIZE: comptime_int,
    comptime BUCKET_SIZE: comptime_int,
    comptime MAX_LOAD: comptime_float,
) type {
    if (!std.math.isPowerOfTwo(INDEX_SIZE)) {
        @compileLog("Storage.INDEX_SIZE ought to be power of two");
    }

    const nil = std.math.maxInt(K);
    // these loads are completely separate from the ones in Bucket
    // these specify when to add/remove buckets based on the overall spiral-storage load
    // const LOAD_MIN = 0.2;
    // const LOAD_MAX = 0.6;

    return struct {
        const Self = @This();

        const Bucket = BucketImpl(K, V, BUCKET_SIZE);

        const Index = struct {
            buckets: [INDEX_SIZE]?*Bucket,
        };

        alloc: std.mem.Allocator,
        len: usize,
        index: ?*Index,
        split: usize, // marks the bucket that should be split next
        round: usize,
        n_buckets: usize,

        pub fn init(alloc: std.mem.Allocator) !Self {
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

        pub fn get(storage: *Self, key: K) ?*V {
            if (key == nil) return null;

            if (storage.index) |index| {
                const loc = storage.bucket(key);
                return index.buckets[loc].?.get(key);
            }

            return null;
        }

        pub fn add(storage: *Self, key: K, val: V) !void {
            if (key == nil) return;
            if (storage.index) |index| {
                const loc = storage.bucket(key);
                try index.buckets[loc].?.add(storage.alloc, key, val);
                storage.len += 1;

                if (storage.load() > MAX_LOAD and storage.n_buckets < INDEX_SIZE) {
                    const splitting = index.buckets[storage.split].?;
                    // TODO if allocation fails, we should not break the datastructure
                    index.buckets[storage.split] = try Bucket.create(storage.alloc);
                    index.buckets[storage.n_buckets] = try Bucket.create(storage.alloc);
                    storage.split += 1;
                    storage.n_buckets += 1;

                    var it = splitting.iterator();
                    while (it.next()) |kv| {
                        const new_loc = storage.bucket(kv.key);
                        std.debug.assert(new_loc == storage.split - 1 or
                            new_loc == storage.n_buckets - 1);
                        try index.buckets[new_loc].?.add(storage.alloc, kv.key, kv.val);
                    }
                    splitting.destroy(storage.alloc);

                    if (storage.split == (@as(usize, 1) << @intCast(storage.round))) {
                        storage.round += 1;
                        storage.split = 0;
                    }
                }
            } else {
                storage.index = try storage.alloc.create(Index);
                errdefer storage.alloc.destroy(storage.index.?);
                storage.index.?.buckets = .{null} ** INDEX_SIZE;
                storage.index.?.buckets[0] = try Bucket.create(storage.alloc);

                storage.n_buckets = 1;
                return storage.add(key, val);
            }
        }

        fn load(storage: *Self) f64 {
            return @as(f64, @floatFromInt(storage.len)) /
                @as(f64, @floatFromInt(BUCKET_SIZE * storage.n_buckets));
        }

        fn bucket(storage: *Self, key: K) usize {
            const h = std.hash.uint32(key *% PRIME_A);
            var loc = h & ((@as(usize, 1) << @intCast(storage.round)) - 1);
            if (loc < storage.split) { // i wonder if this branch predicts well
                loc = h & ((@as(usize, 1) << (@intCast(storage.round + 1))) - 1);
            }
            return loc;
        }

        const Iterator = struct {
            const KV = struct { key: K, val: V };

            storage: *Self,
            cursor: usize,
            bucket_it: ?Bucket.Iterator,

            pub fn next(it: *Iterator) ?KV {
                if (it.bucket_it != null) {
                    if (it.bucket_it.?.next()) |kv| {
                        return .{ .key = kv.key, .val = kv.val };
                    }

                    if (it.cursor < it.storage.n_buckets - 1) {
                        it.cursor += 1;
                        it.bucket_it = it.storage.index.?.buckets[it.cursor].?.iterator();
                        return it.next();
                    }

                    it.bucket_it = null;
                }

                return null;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            return .{
                .storage = storage,
                .cursor = 0,
                .bucket_it = if (storage.index) |index| index.buckets[0].?.iterator() else null,
            };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print(
                "Storage <{s} {s}> len: {} load: {}\n",
                .{ @typeName(K), @typeName(V), storage.len, storage.load() },
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

test "storage fuzz" {
    var s = try Storage(u32, u32, 2048, 16, 0.7).init(std.testing.allocator);
    defer s.deinit();
    var h = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer h.deinit();

    var rng = std.rand.DefaultPrng.init(@intCast(std.time.microTimestamp()));
    for (0..10000) |_| {
        const x = rng.random().int(u16); // so we avoid our nil key
        if (h.contains(x)) {
            try std.testing.expectEqual(h.getPtr(x).?.*, s.get(x).?.*);
        } else {
            try std.testing.expectEqual(null, h.getPtr(x)); // not like i need to test but w/e
            try std.testing.expectEqual(null, s.get(x));
            try s.add(x, x);
            try h.put(x, x);
        }
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = try Storage(u32, u32, 8, 8, 0.7).init(alloc);
    defer s.deinit();

    s.debugPrint();
    for (0..32) |i| {
        const x: u32 = @intCast(i * 51 % 32);
        std.debug.print("\ninserting {}\n", .{x});
        try s.add(x, x);
        s.debugPrint();
        std.debug.assert(s.get(x).?.* == x);
        std.debug.assert(s.get(x + 32) == null);
    }

    var it = s.iterator();
    while (it.next()) |kv| {
        std.debug.print("{}\n", .{kv.key});
    }

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u32, 2048, 1024, 0.7).Index),
        @sizeOf(Storage(u32, u32, 2048, 1024, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u64, 2048, 1024, 0.7).Index),
        @sizeOf(Storage(u32, u64, 2048, 1024, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u128, 2048, 512, 0.7).Index),
        @sizeOf(Storage(u32, u128, 2048, 512, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u256, 2048, 256, 0.7).Index),
        @sizeOf(Storage(u32, u256, 2048, 256, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u512, 2048, 128, 0.7).Index),
        @sizeOf(Storage(u32, u512, 2048, 128, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u1024, 2048, 64, 0.7).Index),
        @sizeOf(Storage(u32, u1024, 2048, 64, 0.7).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u32, 2048, 1024, 0.7)),
        @alignOf(Storage(u32, u32, 2048, 1024, 0.7)),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(std.mem.Allocator),
        @alignOf(std.mem.Allocator),
    });
}
