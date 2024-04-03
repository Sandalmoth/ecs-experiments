const std = @import("std");

const Entity = u64;
const nil: Entity = 0;

fn orderEntity(context: void, lhs: usize, rhs: usize) std.math.Order {
    _ = context;
    return std.math.order(lhs, rhs);
}

fn BucketImpl(comptime V: type) type {
    return struct {
        const Self = @This();

        data: std.AutoHashMap(Entity, V),

        fn init(alloc: std.mem.Allocator) Self {
            return .{
                .data = std.AutoHashMap(Entity, V).init(alloc),
            };
        }

        fn deinit(bucket: *Self) void {
            bucket.data.deinit();
        }

        fn add(bucket: *Self, key: Entity, val: V) void {
            std.debug.assert(!bucket.data.contains(key));
            bucket.data.put(key, val) catch unreachable;
        }

        fn del(bucket: *Self, key: Entity) bool {
            return bucket.data.remove(key);
        }

        fn has(bucket: *Self, key: Entity) bool {
            return bucket.data.contains(key);
        }

        fn get(bucket: *Self, key: Entity) ?*V {
            return bucket.data.getPtr(key);
        }

        pub fn count(bucket: *Self) usize {
            return bucket.data.count();
        }

        fn debugPrint(bucket: *Self) void {
            var it = bucket.data.keyIterator();
            std.debug.print("  [ ", .{});
            while (it.next()) |k| {
                std.debug.print("{} ", .{k.*});
            }
            std.debug.print("]\n", .{});
        }
    };
}

fn RunImpl(comptime V: type) type {
    return struct {
        const Self = @This();

        keys: []Entity,
        vals: []V,
        dels: []u64, // timestep when the matching entry was deleted
        rc: *usize,
        len: usize,

        fn init(alloc: std.mem.Allocator, capacity: usize) Self {
            var run = Self{
                .keys = undefined,
                .vals = undefined,
                .dels = undefined,
                .rc = undefined,
                .len = 0,
            };
            run.keys = alloc.alloc(Entity, capacity) catch unreachable;
            run.vals = alloc.alloc(V, capacity) catch unreachable;
            run.dels = alloc.alloc(u64, capacity) catch unreachable;
            run.rc = alloc.create(usize) catch unreachable;
            run.rc.* = 1;
            return run;
        }

        fn deinit(run: *Self, alloc: std.mem.Allocator) void {
            run.rc.* -= 1;
            if (run.rc.* == 0) {
                alloc.free(run.keys);
                alloc.free(run.vals);
                alloc.free(run.dels);
                alloc.destroy(run.rc);
            }
        }

        fn append(run: *Self, key: Entity, val: V) void {
            std.debug.assert(run.len < run.keys.len);
            run.keys[run.len] = key;
            run.vals[run.len] = val;
            run.dels[run.len] = std.math.maxInt(u64);
            run.len += 1;
        }

        fn appendt(run: *Self, t: u64, key: Entity, val: V) void {
            std.debug.assert(run.len < run.keys.len);
            run.keys[run.len] = key;
            run.vals[run.len] = val;
            run.dels[run.len] = t;
            run.len += 1;
        }

        fn sort(run: *Self) void {
            for ([_]usize{ 488, 187, 72, 27, 10, 4, 1 }) |gap| {
                if (gap >= run.len) continue;
                for (gap..run.len) |j| {
                    const tmpk = run.keys[j];
                    const tmpv = run.vals[j];
                    const tmpd = run.dels[j];
                    var k = j;
                    while (k >= gap and run.keys[k - gap] > tmpk) : (k -= gap) {
                        run.keys[k] = run.keys[k - gap];
                        run.vals[k] = run.vals[k - gap];
                        run.dels[k] = run.dels[k - gap];
                    }
                    run.keys[k] = tmpk;
                    run.vals[k] = tmpv;
                    run.dels[k] = tmpd;
                }
            }
        }

        fn del(run: *Self, t: u64, key: Entity) void {
            std.debug.assert(key != nil);
            const loc = std.sort.binarySearch(
                usize,
                key,
                run.keys[0..run.len],
                {},
                orderEntity,
            ) orelse {
                std.debug.print("Run.del failed to find entity {}\n", .{key});
                unreachable;
            };
            if (run.dels[loc] != std.math.maxInt(u64)) {
                std.debug.print("Run.del double delete of entity {}\n", .{key});
                unreachable;
            }
            run.dels[loc] = t;
        }

        fn has(run: *Self, t: u64, key: Entity) bool {
            std.debug.assert(key != nil);
            const loc = std.sort.binarySearch(
                usize,
                key,
                run.keys[0..run.len],
                {},
                orderEntity,
            ) orelse return false;
            return t < run.dels[loc];
        }

        fn get(run: *Self, t: u64, key: Entity) ?*V {
            std.debug.assert(key != nil);
            const loc = std.sort.binarySearch(
                usize,
                key,
                run.keys[0..run.len],
                {},
                orderEntity,
            ) orelse return null;
            return if (t < run.dels[loc]) &run.vals[loc] else null;
        }

        fn debugPrint(run: *Self, t: u64) void {
            std.debug.print("  [ ", .{});
            for (0..run.len) |i| {
                if (t >= run.dels[i]) std.debug.print("~", .{});
                std.debug.print("{} ", .{run.keys[i]});
            }
            std.debug.print("]\n", .{});
        }
    };
}

pub fn StorageImpl(comptime V: type) type {
    return struct {
        const Self = @This();
        const Bucket = BucketImpl(V);
        const Run = RunImpl(V);

        alloc: std.mem.Allocator,
        bucket: Bucket,
        run: Run,
        timepoint: u64,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .bucket = Bucket.init(alloc),
                .run = Run.init(alloc, 0),
                .timepoint = 0,
            };
        }

        pub fn deinit(storage: *Self) void {
            storage.bucket.deinit();
            storage.run.deinit(storage.alloc);
        }

        pub fn add(storage: *Self, key: Entity, val: V) void {
            if (!storage.bucket.has(key) and storage.run.has(storage.timepoint, key)) {
                storage.run.del(storage.timepoint, key);
            }
            storage.bucket.add(key, val);
        }

        pub fn del(storage: *Self, key: Entity) void {
            if (!storage.bucket.del(key)) storage.run.del(storage.timepoint, key);
        }

        pub fn has(storage: *Self, key: Entity) bool {
            return storage.bucket.has(key) or storage.run.has(storage.timepoint, key);
        }

        pub fn get(storage: *Self, key: Entity) ?*V {
            return storage.bucket.get(key) orelse storage.run.get(storage.timepoint, key);
        }

        pub fn compact(storage: *Self) void {
            var new_run = Run.init(storage.alloc, storage.run.len + storage.bucket.count());
            for (0..storage.run.len) |i| {
                if (storage.run.dels[i] != std.math.maxInt(u64)) continue;
                new_run.appendt(
                    storage.run.dels[i],
                    storage.run.keys[i],
                    storage.run.vals[i],
                );
            }
            var it = storage.bucket.data.iterator();
            while (it.next()) |kv| {
                new_run.append(kv.key_ptr.*, kv.value_ptr.*);
            }
            new_run.sort();

            // putting everything into one array and then sorting is extremely trash
            // and we should obviously sort the bucket and then do a sort-merge
            // but this is just a proof of concept

            storage.bucket.data.clearRetainingCapacity();
            storage.run.deinit(storage.alloc);
            storage.run = new_run;
            storage.timepoint += 1;
        }

        // pub fn cycle(storage: *Self) void {}

        fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s}>\n", .{@typeName(V)});
            storage.bucket.debugPrint();
            storage.run.debugPrint(storage.timepoint);
        }
    };
}

test "storage scratch" {
    var s = StorageImpl(f64).init(std.testing.allocator);
    defer s.deinit();

    std.debug.print("\n", .{});

    s.add(2, 2.0);
    s.add(1, 1.0);
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});

    s.compact();
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});

    s.add(4, 4.0);
    s.add(3, 3.0);
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});

    s.compact();
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});

    s.del(2);
    s.add(3, 3.0);
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});

    s.compact();
    s.debugPrint();

    std.debug.print("{}\n", .{s.has(1)});
    std.debug.print("{}\n", .{s.has(2)});
    std.debug.print("{}\n", .{s.has(3)});
    std.debug.print("{}\n", .{s.has(4)});
}

test "storage fuzz" {
    const n = 1024;
    const m = 16;
    var s = StorageImpl(f64).init(std.testing.allocator);
    defer s.deinit();
    var h = std.AutoHashMap(Entity, f64).init(std.testing.allocator);
    defer h.deinit();
    var c = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
    defer c.deinit();

    var rng = std.rand.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..m) |_| {
        var a = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
        defer a.deinit();

        for (0..n) |_| {
            const k = rand.int(Entity) | 1;
            const v = rand.float(f64);
            try std.testing.expect(s.has(k) == h.contains(k));
            if (s.has(k)) continue;
            s.add(k, v);
            try h.put(k, v);
            try a.append(k);
            try c.append(k);
        }

        for (a.items) |k| {
            if (rand.boolean()) continue;
            try std.testing.expect(s.get(k).?.* == h.getPtr(k).?.*);
            s.del(k);
            try std.testing.expect(h.remove(k));
            try std.testing.expect(s.has(k) == h.contains(k));
        }

        for (c.items) |k| {
            if (rand.boolean()) continue;
            try std.testing.expect(s.has(k) == h.contains(k));
            if (s.has(k)) {
                s.del(k);
                try std.testing.expect(h.remove(k));
            } else {
                const v = rand.float(f64);
                s.add(k, v);
                try h.put(k, v);
            }
            try std.testing.expect(s.has(k) == h.contains(k));
        }

        // std.debug.print("{}\t{}\n", .{ s.bucket.count(), s.run.len });
        s.compact();
        // std.debug.print("{}\t{}\n", .{ s.bucket.count(), s.run.len });
    }
}

pub fn main() void {}
