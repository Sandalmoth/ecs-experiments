const std = @import("std");
const primes = @import("primes.zig").primes;

const Entity = u64;
const nil: Entity = 0;

const Meta = packed struct {
    tombstone: bool,
    _pad: u7 = undefined, // TODO try a fingerprint like in std.HashMap
};

// const PAGE_SIZE = 4 * 1024;
const PAGE_SIZE = 256;
const Page = struct {
    bytes: [PAGE_SIZE]u8 align(64),
};

const Header = struct {
    keys: [*]Entity,
    vals: *anyopaque,
    meta: [*]Meta, // unused in a compacted run
    len: usize,
    depth: usize,
    run: ?*Bucket, // null if a compacted run, ptr if hashmap
    rc: usize, // refcount (TODO atomic? concurrency?)
};

comptime {
    std.debug.assert(@sizeOf(Header) < 64);
}

const Data = struct {
    bytes: [PAGE_SIZE - 64]u8 align(64),

    fn Layout(comptime cap: comptime_int, comptime V: type) type {
        return struct {
            const capacity = cap;

            keys: [cap]Entity,
            vals: [cap]V,
            meta: [cap]Meta,
        };
    }

    /// find the biggest prime number size that fits
    fn capacity(comptime V: type) comptime_int {
        var i: usize = 0;
        while (@sizeOf(Layout(primes[i], V)) < @sizeOf(Data)) : (i += 1) {}
        if (i == 0) @compileError(@typeName(V) ++ " is too large");
        return primes[i - 1];
    }
};

const Bucket = struct {
    head: Header,
    data: Data,

    comptime {
        std.debug.assert(@sizeOf(Bucket) <= @sizeOf(Page));
        std.debug.assert(@alignOf(Bucket) <= @alignOf(Page));
    }

    fn createRun(comptime Layout: type, alloc: std.mem.Allocator) !*Bucket {
        const bucket: *Bucket = @ptrCast(@alignCast(try alloc.create(Page)));
        // std.debug.print("createRun {x}\n", .{@intFromPtr(bucket)});

        const data = @intFromPtr(bucket) + @offsetOf(Bucket, "data");
        bucket.head.keys = @ptrFromInt(data + @offsetOf(Layout, "keys"));
        bucket.head.vals = @ptrFromInt(data + @offsetOf(Layout, "vals"));
        bucket.head.meta = @ptrFromInt(data + @offsetOf(Layout, "meta"));
        bucket.head.len = 0;
        bucket.head.depth = undefined;
        bucket.head.run = null;

        bucket.head.rc = 1;
        return bucket;
    }

    fn createMap(
        comptime Layout: type,
        alloc: std.mem.Allocator,
        depth: usize,
        run: *Bucket,
    ) !*Bucket {
        const bucket: *Bucket = @ptrCast(@alignCast(try alloc.create(Page)));
        // std.debug.print("createRun {x}\n", .{@intFromPtr(bucket)});

        const data = @intFromPtr(bucket) + @offsetOf(Bucket, "data");
        bucket.head.keys = @ptrFromInt(data + @offsetOf(Layout, "keys"));
        bucket.head.vals = @ptrFromInt(data + @offsetOf(Layout, "vals"));
        bucket.head.meta = @ptrFromInt(data + @offsetOf(Layout, "meta"));
        bucket.head.len = 0;
        bucket.head.depth = depth;
        bucket.head.run = run;

        for (0..Layout.capacity) |i| {
            bucket.head.keys[i] = nil;
            bucket.head.meta[i] = .{ .tombstone = false };
        }

        bucket.head.rc = 1;
        return bucket;
    }

    fn destroy(bucket: *Bucket, alloc: std.mem.Allocator) void {
        bucket.head.rc -= 1;
        if (bucket.head.rc == 0) {
            if (bucket.head.run) |run| {
                run.destroy(alloc);
            }
            const p: *Page = @alignCast(@ptrCast(bucket));
            alloc.destroy(p);
            // std.debug.print("destroy {x}\n", .{@intFromPtr(bucket)});
        }
    }

    fn add(
        bucket: *Bucket,
        comptime V: type,
        mod: *const fn (usize) usize,
        h: u32,
        key: Entity,
        val: V,
    ) void {
        std.debug.assert(bucket.head.run != null);

        // for the location in the hashmap we're scrapping the bits used to select the bucket
        // since the entropy of those bits has already been spent
        var loc: usize = mod(h >> @intCast(bucket.head.depth));

        // TODO assert that key is not in bucket (or run)

        while (true) : (loc = mod(loc + 1)) {
            if (bucket.head.keys[loc] == nil) break;
            if (bucket.head.keys[loc] == key) {
                std.debug.assert(bucket.head.meta[loc].tombstone);
                break;
            }
        }

        const vals: [*]V = @alignCast(@ptrCast(bucket.head.vals));
        bucket.head.keys[loc] = key;
        vals[loc] = val;
        bucket.head.meta[loc].tombstone = false;
        bucket.head.len += 1;
    }

    fn compact(bucket: *Bucket, comptime V: type, cap: usize) !*Bucket {
        // so here's the complication:
        // what if compaction causes overflow?
        // it's not impossible to work around
        // however, an "overflow-proof" design like linear hashing is much simpler
    }

    pub fn debugPrint(bucket: *Bucket, cap: usize) void {
        if (bucket.head.run == null) {
            std.debug.print(" [ ", .{});
            for (bucket.head.keys[0..bucket.head.len]) |k| std.debug.print("{} ", .{k});
            std.debug.print("] rc:{}\n", .{bucket.head.rc});
        } else {
            // for (0..bucket.head.depth) |_| std.debug.print("  ", .{});
            std.debug.print(" {} [ ", .{bucket.head.depth});
            for (bucket.head.keys[0..cap]) |k| {
                if (k == nil) continue;
                std.debug.print("{} ", .{k});
            }
            std.debug.print("] rc:{} ->", .{bucket.head.rc});
            bucket.head.run.?.debugPrint(cap);
        }
    }
};

const Storage = struct {
    const Index = struct {
        buckets: [PAGE_SIZE / @sizeOf(?*Bucket)]?*Bucket,
    };

    comptime {
        std.debug.assert(@sizeOf(Index) == @sizeOf(Page));
    }

    alloc: std.mem.Allocator, // TODO change to a Page memorypool
    index: ?*Index,
    len: usize,
    depth: usize,
    immutable: bool,

    capacity: usize, // capacity of buckets
    mod: *const fn (usize) usize, // x -> x % capacity

    pub fn init(alloc: std.mem.Allocator) Storage {
        return .{
            .alloc = alloc,
            .index = null,
            .len = 0,
            .depth = 0,
            .immutable = false,
            .capacity = undefined,
            .mod = undefined,
        };
    }

    pub fn deinit(storage: *Storage) void {
        if (storage.index) |index| {
            for (0..(@as(usize, 1) << @intCast(storage.depth))) |i| {
                index.buckets[i].?.destroy(storage.alloc); // decrement refcount and free if 0
            }
            storage.alloc.destroy(@as(*Page, @alignCast(@ptrCast(index))));
        }
        storage.* = undefined;
    }

    pub fn add(storage: *Storage, comptime V: type, key: Entity, val: V) !void {
        std.debug.assert(key != nil);
        std.debug.assert(!storage.immutable);

        const index = storage.index orelse {
            storage.index = @ptrCast(@alignCast(try storage.alloc.create(Page)));
            errdefer storage.alloc.destroy(storage.index.?);

            const bucket_cap = Data.capacity(V);
            const Layout = Data.Layout(bucket_cap, V);
            storage.index.?.buckets[0] = try Bucket.createRun(Layout, storage.alloc);
            storage.capacity = bucket_cap;
            storage.mod = struct {
                fn mod(x: usize) usize {
                    return x % bucket_cap;
                }
            }.mod;

            return storage.add(V, key, val);
        };

        const h = hash(key);
        const loc = if (storage.depth == 0)
            0
        else
            h & (@as(usize, 1) << @intCast(storage.depth - 1));

        if (index.buckets[loc].?.head.run == null) {
            // bucket is immutable run, create hashmap overlay
            // NOTE the refcount of the run is the same
            // as it's dropped from the index but added in the overlay bucket
            const bucket = try Bucket.createMap(
                Data.Layout(Data.capacity(V), V),
                storage.alloc,
                storage.depth,
                index.buckets[loc].?,
            );
            index.buckets[loc] = bucket;
        }

        // test if bucket is full and split if needed
        if (index.buckets[loc].?.head.len > storage.capacity * 3 / 4) {
            // try to create these first, as in case it fails we haven't changed anything
            const new_bucket_a = try Bucket.createMap(
                Data.Layout(Data.capacity(V), V),
                storage.alloc,
                index.buckets[loc].?.head.depth + 1,
                index.buckets[loc].?.head.run.?,
            );
            index.buckets[loc].?.head.run.?.head.rc += 1;
            errdefer new_bucket_a.destroy(storage.alloc);
            const new_bucket_b = try Bucket.createMap(
                Data.Layout(Data.capacity(V), V),
                storage.alloc,
                index.buckets[loc].?.head.depth + 1,
                index.buckets[loc].?.head.run.?,
            );
            index.buckets[loc].?.head.run.?.head.rc += 1;
            errdefer new_bucket_b.destroy(storage.alloc);

            // expand the index if needed (fail if index is full)
            if (index.buckets[loc].?.head.depth == storage.depth) {
                const dir_len: usize = @as(usize, 1) << @intCast(storage.depth);
                if (dir_len == index.buckets.len) {
                    return error.IndexFull;
                }

                for (0..dir_len) |i| {
                    index.buckets[i].?.head.rc += 1;
                }
                @memcpy(index.buckets[dir_len .. 2 * dir_len], index.buckets[0..dir_len]);
                storage.depth += 1;
            }

            // now split the overflowing bucket
            const overflowing = index.buckets[loc].?;
            overflowing.head.rc += 1;
            const dir_len: usize = @as(usize, 1) << @intCast(storage.depth);
            index.buckets[loc].?.head.rc -= 1;
            index.buckets[loc] = new_bucket_a;
            index.buckets[loc + dir_len / 2].?.head.rc -= 1;
            index.buckets[loc + dir_len / 2] = new_bucket_b;
            errdefer {
                overflowing.head.rc += 1; // -1 from overflowing ref, +2 from bucket refs
                index.buckets[loc] = overflowing;
                index.buckets[loc + dir_len] = overflowing;
            }

            const keys = overflowing.head.keys[0..storage.capacity];
            const _vals: [*]V = @alignCast(@ptrCast(overflowing.head.vals));
            const vals = _vals[0..storage.capacity];
            const meta = overflowing.head.meta[0..storage.capacity];
            for (keys, vals, meta) |k, v, m| {
                if (k == nil) continue;
                if (m.tombstone) {
                    // TODO transfer deletions
                } else {
                    try storage.add(V, k, v);
                }
            }

            overflowing.destroy(storage.alloc);

            return storage.add(V, key, val);
        }

        index.buckets[loc].?.add(V, storage.mod, h, key, val);
    }

    fn lock(storage: *Storage) !void {
        // rewrite all hash buckets as runs
        storage.immutable = true;
    }

    fn step(storage: *Storage) !*Storage {
        std.debug.assert(storage.immutable);
    }

    fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(2654435761, std.mem.asBytes(&key));
    }

    fn debugPrint(storage: *Storage) void {
        std.debug.print(
            "Storage cap: {} len: {} depth: {}\n",
            .{ storage.capacity, storage.len, storage.depth },
        );
        if (storage.index) |index| {
            for (0..@as(usize, 1) << @intCast(storage.depth)) |i| {
                index.buckets[i].?.debugPrint(storage.capacity);
            }
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage.init(alloc);
    defer s.deinit();

    for (1..20) |i| {
        std.debug.print("inserting {}\n", .{i});
        try s.add(u32, i, @intCast(i));
        s.debugPrint();
    }
}
