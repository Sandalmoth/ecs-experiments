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
            var loc = std.hash.uint32(key) % SIZE;

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

        scale: f64,
        bucket_begin: usize,
        bucket_end: usize,

        pub fn init(alloc: std.mem.Allocator) !Self {
            // opting to always have the index and 1 bucket
            var storage = Self{
                .alloc = alloc,
                .len = 0,
                .index = undefined,
                .scale = undefined,
                .bucket_begin = undefined,
                .bucket_end = undefined,
            };

            storage.scale = idistr(0);
            storage.bucket_begin = bucketBegin(storage.scale);
            storage.bucket_end = bucketEnd(storage.scale);

            storage.index = try alloc.create(Index);
            storage.index.buckets = .{null} ** INDEX_SIZE;
            errdefer alloc.destroy(storage.index);

            for (storage.bucket_begin..storage.bucket_end) |i| {
                storage.index.buckets[i % INDEX_SIZE] = try Bucket.create(alloc);
            }

            return storage;
        }

        pub fn deinit(storage: *Self) void {
            for (storage.bucket_begin..storage.bucket_end) |i| {
                storage.index.buckets[i % INDEX_SIZE].?.destroy(storage.alloc);
            }
            storage.alloc.destroy(storage.index);
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: K) ?*V {
            if (key == nil) return null;

            const loc = bucketIndex(storage.scale, key);
            std.debug.assert(loc >= storage.bucket_begin);
            std.debug.assert(loc < storage.bucket_end);
            std.debug.assert(storage.index.buckets[loc % INDEX_SIZE] != null);
            return storage.index.buckets[loc % INDEX_SIZE].?.get(key);

            // _ = storage;
            // var timer = std.time.Timer.start() catch unreachable;
            // @prefetch(&storage.index.buckets[storage.n_buckets - 1], .{});
            // const loc = bucketIndex(storage.s, key);
            // std.debug.assert(storage.index.buckets[loc] != null);
            // const result = storage.index.buckets[loc].?.get(key);
            // std.debug.print("{}\n", .{timer.read()});
            // return result;
            // return null;
        }

        pub fn add(storage: *Self, key: K, val: V) !void {

            // TODO this function breaks everything badly if memory allocation fails

            if (key == nil) return;

            const loc = bucketIndex(storage.scale, key);
            std.debug.assert(loc >= storage.bucket_begin);
            std.debug.assert(loc < storage.bucket_end);
            std.debug.assert(storage.index.buckets[loc % INDEX_SIZE] != null);
            try storage.index.buckets[loc % INDEX_SIZE].?.add(storage.alloc, key, val);
            storage.len += 1;

            size_up: {
                // increase the size of the storage if needed
                // do so by increasing scale so that the first bucket is deleted
                // allocating however many new buckets are needed
                // and then reinserting the items from the first bucket wherever they go

                if (storage.load() < LOAD_MAX) break :size_up;

                // see if it's possible to increase scale
                var new_scale = storage.scale;
                var new_begin = storage.bucket_begin;
                var new_end = storage.bucket_end;
                while (new_end - new_begin <= storage.bucket_end - storage.bucket_begin) {
                    // increase enough that we actually get more space
                    // (might need a few steps for certain distrbution functions)
                    new_scale = idistr(@ceil(distr(new_scale)) + 1);
                    new_begin = bucketBegin(new_scale);
                    new_end = bucketEnd(new_scale);
                }
                if (new_end - new_begin > INDEX_SIZE) break :size_up;
                if (new_end - storage.bucket_begin > INDEX_SIZE) {
                    // to maximally use the index we should consider the case where
                    // the new buckets overlap the buckets we're deleting
                    // but, it's
                    break :size_up;
                }

                // do the increase
                // first initialize the new buckets
                for (storage.bucket_end..new_end) |i| {
                    // this assert relates to the above break where new/deleted overlap
                    std.debug.assert(i % INDEX_SIZE != storage.bucket_begin % INDEX_SIZE);
                    storage.index.buckets[i % INDEX_SIZE] = try Bucket.create(storage.alloc);
                }

                // then reallocate the bucket(s) we're eliminating
                for (storage.bucket_begin..new_begin) |i| {
                    var it = storage.index.buckets[i % INDEX_SIZE].?.iterator();
                    while (it.next()) |kv| {
                        const new_loc = bucketIndex(new_scale, kv.key);
                        // std.debug.assert(new_loc >= storage.bucket_end - 1);
                        std.debug.assert(new_loc >= new_begin);
                        std.debug.assert(new_loc < new_end);
                        std.debug.assert(storage.index.buckets[new_loc % INDEX_SIZE] != null);
                        try storage.index.buckets[new_loc % INDEX_SIZE].?
                            .add(storage.alloc, kv.key, kv.val);
                    }
                    storage.index.buckets[i % INDEX_SIZE].?.destroy(storage.alloc);
                }
                storage.bucket_begin = new_begin;
                storage.bucket_end = new_end;
                storage.scale = new_scale;
            }
        }

        fn idistr(h: f64) f64 {
            // return @sqrt(h);
            return std.math.pow(f64, h, 1.0 / 3.0);
        }

        fn distr(h: f64) f64 {
            // return h * h;
            return h * h * h;
        }

        fn bucketMap(scale: f64, h: f64) usize {
            var t = @ceil(scale - h) + h;
            t = distr(t);
            return @as(usize, @intFromFloat(t));
        }

        fn bucketBegin(scale: f64) usize {
            const t = distr(scale) + 0.5;
            return @as(usize, @intFromFloat(t));
        }

        fn bucketEnd(scale: f64) usize {
            const t = @ceil(distr(scale + 1));
            return @as(usize, @intFromFloat(t));
        }

        fn bucketIndex(scale: f64, key: K) usize {
            const h = floatHash(key);
            return bucketMap(scale, h);
        }

        fn load(storage: *Self) f64 {
            return @as(f64, @floatFromInt(storage.len)) /
                @as(f64, @floatFromInt(BUCKET_SIZE * (storage.bucket_end - storage.bucket_begin)));
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

                    if (it.cursor < it.storage.bucket_end - 1) {
                        it.cursor += 1;
                        it.bucket_it = it.storage.index.buckets[
                            it.cursor % INDEX_SIZE
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
                .cursor = storage.bucket_begin,
                .bucket_it = storage.index.buckets[storage.bucket_begin % INDEX_SIZE].?.iterator(),
            };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print(
                "Storage <{s} {s}> len: {} load: {}\n",
                .{ @typeName(K), @typeName(V), storage.len, storage.load() },
            );
            for (storage.bucket_begin..storage.bucket_end) |i| {
                storage.index.buckets[i % INDEX_SIZE].?.debugPrint();
            }
        }
    };
}

// test "distr" {
//     const S = Storage(u32, u32, 2048, 16);

//     std.debug.print("\n", .{});
//     for (0..20) |i| {
//         var x: f64 = @floatFromInt(i);
//         x = S.idistr(x);

//         std.debug.print("{} {}\n", .{ S.bucketBegin(x), S.bucketEnd(x) });
//     }
// }

test "storage fuzz" {
    var s = try Storage(u32, u32, 2048, 16).init(std.testing.allocator);
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
