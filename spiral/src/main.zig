const std = @import("std");

// learned about spiral storage and it's extremely neat
// the behaviour matches what i want really well
// - very consistent behaviour
// - incremental growth/shrinking
// and though the original versions use dynamic array buckets
// for extra good searching with large buckets i'll use hashmaps
// so the spiral storage will index into a series of small hashmap buckets

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

            var loc = std.hash.uint32(key) % SIZE;

            while (bucket.keys[loc] != nil) : (loc = (loc + 1) % SIZE) {
                std.debug.assert(bucket.keys[loc] != key);
            }
            bucket.keys[loc] = key;
            bucket.vals[loc] = val;
            bucket.len += 1;
        }

        fn get(bucket: *Self, key: K) ?*V {
            // var loc = std.hash.uint32(key) % SIZE;
            var loc = key & (SIZE - 1);

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

// hash to a floating point uniform on [0, 1)
// by just inserting the bits into a double
fn floatHash(a: u32) f64 {
    var b: usize = std.hash.uint32(a);
    b <<= 20;
    b |= 1023 << 52;
    return @as(f64, @bitCast(b)) - 1.0;
}

test "floatHash" {
    var rng = std.rand.DefaultPrng.init(@intCast(std.time.microTimestamp()));
    for (0..100) |_| {
        const x = floatHash(@intCast(rng.random().int(u32)));
        try std.testing.expect(x >= 0);
        try std.testing.expect(x < 1);
    }
}

pub fn Storage(
    comptime K: type,
    comptime V: type,
    comptime INDEX_SIZE: comptime_int,
    comptime BUCKET_SIZE: comptime_int,
) type {
    if (!std.math.isPowerOfTwo(INDEX_SIZE)) {
        @compileLog("Storage.INDEX_SIZE ought to be power of two");
    }

    const nil = std.math.maxInt(K);
    // these loads are completely separate from the ones in Bucket
    // these specify when to add/remove buckets based on the overall spiral-storage load
    // const LOAD_MIN = 0.2;
    const LOAD_MAX = 0.6;

    return struct {
        const Self = @This();

        const Bucket = BucketImpl(K, V, BUCKET_SIZE);

        const Index = struct {
            buckets: [INDEX_SIZE]?*Bucket,
        };

        alloc: std.mem.Allocator,
        len: usize,
        index: *Index,
        n_buckets: usize,
        log2_n_buckets: f64,

        pub fn init(alloc: std.mem.Allocator) !Self {
            // opting to always have the index and 1 bucket
            var storage = Self{
                .alloc = alloc,
                .len = 0,
                .index = undefined,
                .n_buckets = 1,
                .log2_n_buckets = 0, // log2(1) == 0
            };

            storage.index = try alloc.create(Index);
            storage.index.buckets = .{null} ** INDEX_SIZE;
            errdefer alloc.destroy(storage.index);

            storage.index.buckets[storage.n_buckets - 1] = try Bucket.create(alloc);

            return storage;
        }

        pub fn deinit(storage: *Self) void {
            for (storage.n_buckets - 1..2 * storage.n_buckets - 1) |i| {
                storage.index.buckets[i % INDEX_SIZE].?.destroy(storage.alloc);
            }
            storage.alloc.destroy(storage.index);
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: K) ?*V {
            if (key == nil) return null;
            // var timer = std.time.Timer.start() catch unreachable;
            @prefetch(&storage.index.buckets[storage.n_buckets - 1], .{});
            const loc = bucketIndex(storage.log2_n_buckets, key);
            std.debug.assert(storage.index.buckets[loc] != null);
            const result = storage.index.buckets[loc].?.get(key);
            // std.debug.print("{}\n", .{timer.read()});
            return result;
        }

        pub fn add(storage: *Self, key: K, val: V) !void {
            if (key == nil) return;

            const loc = bucketIndex(storage.log2_n_buckets, key);
            // std.debug.print("{}\n", .{loc});
            std.debug.assert(storage.index.buckets[loc] != null);
            try storage.index.buckets[loc].?.add(storage.alloc, key, val);
            storage.len += 1;

            // NOTE bucket indices vs n_buckets
            // 1 : 0 - - - - - - - - - - -
            // 2 : - 1 2 - - - - - - - - -
            // 3 : - - 2 3 4 - - - - - - -
            // 4 : - - - 3 4 5 6 - - - - -
            // 5 : - - - - 4 5 6 7 8 - - -
            // etc.
            // the first is n_buckets - 1
            // the last is at 2 * (n_buckets - 1)
            // and the range is n_buckets - 1 .. 2 * n_buckets - 1

            if (storage.n_buckets < INDEX_SIZE and storage.load() > LOAD_MAX) {
                const emptied = storage.index.buckets[storage.n_buckets - 1].?;
                storage.index.buckets[storage.n_buckets - 1] = null;
                // in case we fail to allocate the new buckets, don't destroy this one
                errdefer storage.index.buckets[storage.n_buckets - 1] = emptied;

                const ix_new_a = (2 * storage.n_buckets - 1) % INDEX_SIZE;
                const ix_new_b = (2 * storage.n_buckets) % INDEX_SIZE;
                storage.index.buckets[ix_new_a] = try Bucket.create(storage.alloc);
                errdefer storage.index.buckets[ix_new_a].?.destroy(storage.alloc);
                storage.index.buckets[ix_new_b] = try Bucket.create(storage.alloc);

                storage.n_buckets += 1;
                storage.log2_n_buckets = std.math.log2(@as(f64, @floatFromInt(storage.n_buckets)));

                var it = emptied.iterator();
                while (it.next()) |kv| {
                    const i = bucketIndex(storage.log2_n_buckets, kv.key);
                    std.debug.assert(i == ix_new_a or i == ix_new_b);
                    // since we're splitting a bucket, we're almost guaranteed success
                    // though in theory, if the splitting buckets have enough overflow
                    // we could fail to allocate chaining buckets.
                    // however, statistically this should be almost impossible
                    // but we might want to think carefully about the order of operations
                    // in a proper implemetation such that nothing breaks on an allocation failure
                    storage.index.buckets[i].?
                        .add(storage.alloc, kv.key, kv.val) catch unreachable;
                }
                emptied.destroy(storage.alloc);
            }
        }

        fn bucketIndex(log2_n_buckets: f64, key: K) usize {
            var h = floatHash(key);
            // std.debug.print("{} -> ", .{h});
            h = @ceil(log2_n_buckets - h) + h;
            // std.debug.print("{} -> ", .{h});
            h = std.math.pow(f64, 2, h);
            // std.debug.print("{} -> {}\n", .{ h, @as(usize, @intFromFloat(h)) });
            return (@as(usize, @intFromFloat(h)) - 1 + INDEX_SIZE) % INDEX_SIZE;
        }

        fn load(storage: *Self) f64 {
            return @as(f64, @floatFromInt(storage.len)) /
                @as(f64, @floatFromInt(BUCKET_SIZE * storage.n_buckets));
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
                        it.bucket_it = it.storage.index.buckets[
                            (it.storage.n_buckets - 1 + it.cursor) % INDEX_SIZE
                        ].?.iterator();
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
                .bucket_it = storage.index.buckets[storage.n_buckets - 1].?.iterator(),
            };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s} {s}>\n", .{ @typeName(K), @typeName(V) });
            for (storage.n_buckets - 1..2 * storage.n_buckets - 1) |i| {
                storage.index.buckets[i % INDEX_SIZE].?.debugPrint();
            }
        }
    };
}

test "bucketIndex" {
    const S = Storage(u32, u32, 8, 8);
    try std.testing.expectEqual(0, S.bucketIndex(std.math.log2(1), 25));
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = try Storage(u32, u32, 8, 8).init(alloc);
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
        @sizeOf(Storage(u32, u32, 2048, 1024).Index),
        @sizeOf(Storage(u32, u32, 2048, 1024).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u64, 2048, 1024).Index),
        @sizeOf(Storage(u32, u64, 2048, 1024).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u128, 2048, 512).Index),
        @sizeOf(Storage(u32, u128, 2048, 512).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u256, 2048, 256).Index),
        @sizeOf(Storage(u32, u256, 2048, 256).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u512, 2048, 128).Index),
        @sizeOf(Storage(u32, u512, 2048, 128).Bucket),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u1024, 2048, 64).Index),
        @sizeOf(Storage(u32, u1024, 2048, 64).Bucket),
    });
}
