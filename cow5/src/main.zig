const std = @import("std");
const primes = @import("primes.zig").primes;

// extremely proof of concept and unusable

const Entity = u64;
const nil: Entity = 0;

const Meta = packed struct {
    tombstone: bool,
    _pad: u7 = undefined,
    // TODO use extra space
    // try a fingerprint like in std.HashMap for faster searching
    // or alternatively a displacement distance for robinhood
};

const PAGE_SIZE = 64 * 1024;
// const PAGE_SIZE = 256;
const Page = struct {
    bytes: [PAGE_SIZE]u8 align(64),
};

const MapHeader = struct {
    keys: [*]Entity,
    vals: *anyopaque,
    meta: [*]Meta,
    len: i32, // if we have more deletes than creates, this is negative
    capacity: usize,
    mod: *const fn (usize) usize, // x -> x % capacity
};

const RunHeader = struct {
    keys: [*]Entity,
    vals: *anyopaque,
    len: usize,
    capacity: usize,
    map: ?*Map,
    next: ?*Run, // after compaction, map should be null but we might have several run pages
    rc: usize,
};

comptime {
    std.debug.assert(@sizeOf(MapHeader) <= 64);
    std.debug.assert(@sizeOf(RunHeader) <= 64);
}

const Data = struct {
    bytes: [PAGE_SIZE - 64]u8 align(64),

    fn MapLayout(comptime cap: comptime_int, comptime V: type) type {
        return struct {
            const capacity = cap;

            keys: [cap]Entity,
            vals: [cap]V,
            meta: [cap]Meta,
        };
    }

    fn RunLayout(comptime cap: comptime_int, comptime V: type) type {
        return struct {
            const capacity = cap;

            keys: [cap]Entity,
            vals: [cap]V,
        };
    }

    /// find the biggest prime number size that fits
    fn mapCapacity(comptime V: type) comptime_int {
        // terrible, should be a binary search or similar
        @setEvalBranchQuota(10000);
        var i: usize = PAGE_SIZE / (4 * @max(@sizeOf(Entity), @sizeOf(V)));
        while (@sizeOf(MapLayout(primes[i], V)) < @sizeOf(Data)) : (i += 1) {}
        while (@sizeOf(MapLayout(primes[i], V)) > @sizeOf(Data)) : (i -= 1) {}
        return primes[i - 1];
    }

    /// find the biggest size that fits
    fn runCapacity(comptime V: type) comptime_int {
        // terrible, should be a binary search or similar
        @setEvalBranchQuota(10000);
        var i: usize = PAGE_SIZE / (2 * @max(@sizeOf(Entity), @sizeOf(V)));
        while (@sizeOf(RunLayout(i, V)) < @sizeOf(Data)) : (i += 1) {}
        while (@sizeOf(RunLayout(i, V)) > @sizeOf(Data)) : (i -= 1) {}
        return i;
    }
};

const Map = struct {
    head: MapHeader,
    data: Data,

    fn create(comptime V: type, alloc: std.mem.Allocator) !*Map {
        const map: *Map = @ptrCast(@alignCast(try alloc.create(Page)));

        const capacity = Data.mapCapacity(V);
        const Layout = Data.MapLayout(capacity, V);
        const data = @intFromPtr(map) + @offsetOf(Map, "data");
        map.head.keys = @ptrFromInt(data + @offsetOf(Layout, "keys"));
        map.head.vals = @ptrFromInt(data + @offsetOf(Layout, "vals"));
        map.head.meta = @ptrFromInt(data + @offsetOf(Layout, "meta"));
        map.head.len = 0;
        map.head.capacity = capacity;
        map.head.mod = struct {
            fn mod(x: usize) usize {
                return x % capacity;
            }
        }.mod;

        for (0..capacity) |i| {
            map.head.keys[i] = nil;
            map.head.meta[i] = .{ .tombstone = false };
        }

        return map;
    }

    fn destroy(map: *Map, alloc: std.mem.Allocator) void {
        const p: *Page = @alignCast(@ptrCast(map));
        alloc.destroy(p);
    }

    fn add(map: *Map, comptime V: type, key: Entity, val: V) !void {
        var loc = map.head.mod(hash(key));

        while (true) : (loc = map.head.mod(loc + 1)) {
            if (map.head.keys[loc] == nil) break;
            if (map.head.keys[loc] == key) {
                std.debug.assert(map.head.meta[loc].tombstone);
                break;
            }
        }

        const vals: [*]V = @alignCast(@ptrCast(map.head.vals));
        map.head.keys[loc] = key;
        vals[loc] = val;
        map.head.meta[loc].tombstone = false;
        map.head.len += 1;
    }

    fn has(map: *Map, key: Entity) bool {
        var loc = map.head.mod(hash(key));

        while (true) : (loc = map.head.mod(loc + 1)) {
            if (map.head.keys[loc] == nil) return false;
            if (map.head.keys[loc] == key) return true;
        }
    }

    /// NOTE this destroys the internal structure, map can not be used after this is called
    fn sorted(map: *Map, comptime V: type) SortedIterator(V) {
        const keys = map.head.keys;
        const vals: [*]V = @alignCast(@ptrCast(map.head.vals));

        // compact values at front of hashmap
        var i: usize = 0;
        for (0..map.head.capacity) |j| {
            if (keys[j] != nil) {
                keys[i] = keys[j];
                vals[i] = vals[j];
                i += 1;
            }
        }

        // then sort them
        for ([_]usize{ 488, 187, 72, 27, 10, 4, 1 }) |gap| {
            if (gap >= i) continue;
            for (gap..i) |j| {
                const tmpk = keys[j];
                const tmpv = vals[j];
                var k = j;
                while (k >= gap and keys[k - gap] > tmpk) : (k -= gap) {
                    keys[k] = keys[k - gap];
                    vals[k] = vals[k - gap];
                }
                keys[k] = tmpk;
                vals[k] = tmpv;
            }
        }

        // std.debug.print("{any}\n", .{keys[0..i]});
        // std.debug.assert(std.sort.isSorted(u64, keys[0..i], {}, std.sort.asc(u64)));
        // for (1..i) |j| {
        //     std.debug.assert(keys[j - 1] < keys[j]);
        // }

        return .{
            .keys = keys,
            .vals = vals,
            .capacity = i,
            .cursor = 0,
        };
    }

    fn SortedIterator(comptime V: type) type {
        return struct {
            const Entry = struct { key: Entity, val_ptr: *V };

            keys: [*]Entity,
            vals: [*]V,
            capacity: usize,
            cursor: usize,

            fn next(it: *@This()) ?Entry {
                if (it.cursor >= it.capacity) return null;
                const result = Entry{
                    .key = it.keys[it.cursor],
                    .val_ptr = &it.vals[it.cursor],
                };
                it.cursor += 1;
                return result;
            }
        };
    }

    fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(2654435761, std.mem.asBytes(&key));
    }

    fn debugPrint(map: *Map) void {
        std.debug.print("  [ ", .{});
        for (0..map.head.capacity) |i| {
            if (map.head.keys[i] != nil) std.debug.print("{} ", .{map.head.keys[i]});
        }
        std.debug.print("] : ", .{});
    }
};

const Run = struct {
    head: RunHeader,
    data: Data,

    fn create(comptime V: type, alloc: std.mem.Allocator) !*Run {
        const run: *Run = @ptrCast(@alignCast(try alloc.create(Page)));

        const capacity = Data.runCapacity(V);
        const Layout = Data.RunLayout(capacity, V);
        const data = @intFromPtr(run) + @offsetOf(Run, "data");
        run.head.keys = @ptrFromInt(data + @offsetOf(Layout, "keys"));
        run.head.vals = @ptrFromInt(data + @offsetOf(Layout, "vals"));
        run.head.len = 0;
        run.head.capacity = capacity;
        run.head.map = null;
        run.head.next = null;
        run.head.rc = 0;

        return run;
    }

    fn destroy(run: *Run, alloc: std.mem.Allocator) void {
        run.head.rc -= 1;
        if (run.head.rc == 0) {
            if (run.head.map) |map| map.destroy(alloc);
            if (run.head.next) |next| next.destroy(alloc);
            const p: *Page = @alignCast(@ptrCast(run));
            alloc.destroy(p);
        }
    }

    fn add(run: *Run, comptime V: type, alloc: std.mem.Allocator, key: Entity, val: V) !void {
        const map = run.head.map orelse {
            run.head.map = @ptrCast(@alignCast(try Map.create(V, alloc)));
            return run.add(V, alloc, key, val);
        };

        try map.add(V, key, val);
    }

    fn has(run: *Run, key: Entity) bool {
        if (run.head.map) |map| {
            // NOTE incorrect handling of (not implemented) deletions
            if (map.has(key)) return true;
        }

        var left: u32 = 0;
        var right: u32 = @intCast(run.head.len);
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (run.head.keys[mid] >= key) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return run.head.keys[left] == key;
    }

    fn compact(run: *Run, comptime V: type, alloc: std.mem.Allocator) !*Run {
        const map = run.head.map orelse {
            // no edits to this run, return as is
            return run;
        };
        std.debug.assert(run.head.next == null);

        const len = @as(i32, @intCast(run.head.len)) + map.head.len;
        if (len <= 0) {
            @panic("TODO");
        }
        const n_pages = @as(usize, @intCast(len)) / run.head.capacity + 1;
        const new_page_len = @as(usize, @intCast(len)) / n_pages;

        const new_run_head = try Run.create(V, alloc);
        errdefer new_run_head.destroy(alloc);
        new_run_head.head.rc = 1;
        var new_run = new_run_head;
        for (1..n_pages) |_| {
            new_run.head.next = try Run.create(V, alloc);
            new_run = new_run.head.next.?;
        }
        new_run = new_run_head;

        var it_map = map.sorted(V);
        var i: usize = 0;
        var j: usize = 0;
        while (it_map.next()) |kv_map| {
            const vals: [*]V = @alignCast(@ptrCast(run.head.vals));
            const new_vals: [*]V = @alignCast(@ptrCast(new_run.head.vals));
            while (j < run.head.len and run.head.keys[j] < kv_map.key) : (j += 1) {
                new_run.head.keys[i] = run.head.keys[j];
                new_vals[i] = vals[j];
                new_run.head.len += 1;
                i += 1;

                if (i == new_page_len and new_run.head.next != null) {
                    i = 0;
                    new_run = new_run.head.next.?;
                }
            }

            new_run.head.keys[i] = kv_map.key;
            new_vals[i] = kv_map.val_ptr.*;
            new_run.head.len += 1;
            i += 1;

            if (i == new_page_len and new_run.head.next != null) {
                i = 0;
                new_run = new_run.head.next.?;
            }
        }

        const vals: [*]V = @alignCast(@ptrCast(run.head.vals));
        const new_vals: [*]V = @alignCast(@ptrCast(new_run.head.vals));
        while (j < run.head.len) : (j += 1) {
            new_run.head.keys[i] = run.head.keys[j];
            new_vals[i] = vals[j];
            new_run.head.len += 1;
            i += 1;

            if (i == new_page_len and new_run.head.next != null) {
                i = 0;
                new_run = new_run.head.next.?;
            }
        }

        run.destroy(alloc);
        return new_run_head;
    }

    fn debugPrint(run: *Run) void {
        if (run.head.map) |map| map.debugPrint() else std.debug.print("  [ ] : ", .{});
        std.debug.print("[ ", .{});
        for (0..run.head.len) |i| std.debug.print("{} ", .{run.head.keys[i]});
        std.debug.print("]\n", .{});
    }
};

comptime {
    std.debug.assert(@sizeOf(Map) <= @sizeOf(Page));
    std.debug.assert(@alignOf(Map) <= @alignOf(Page));
    std.debug.assert(@sizeOf(Run) <= @sizeOf(Page));
    std.debug.assert(@alignOf(Run) <= @alignOf(Page));
}

pub const Storage = struct {
    const Index = struct {
        const index_size = PAGE_SIZE / (2 * @max(@sizeOf(?*Run), @sizeOf(Entity)));

        bounds: [index_size]Entity,
        buckets: [index_size]?*Run,
    };

    comptime {
        std.debug.assert(@sizeOf(Index) == @sizeOf(Page));
    }

    alloc: std.mem.Allocator, // TODO change to a Page memorypool
    immutable: bool,
    index: ?*Index,
    len: usize,
    n_runs: usize,

    pub fn init(alloc: std.mem.Allocator) Storage {
        return .{
            .alloc = alloc,
            .immutable = false,
            .index = null,
            .len = 0,
            .n_runs = 0,
        };
    }

    pub fn deinit(storage: *Storage) void {
        if (storage.index) |index| {
            for (0..storage.n_runs) |i| {
                index.buckets[i].?.destroy(storage.alloc);
            }
            storage.alloc.destroy(@as(*Page, @alignCast(@ptrCast(index))));
        }
        storage.* = undefined;
    }

    pub fn add(storage: *Storage, comptime V: type, key: Entity, val: V) !void {
        if (storage.immutable) return error.Immutable;

        const index = storage.index orelse {
            storage.index = @ptrCast(@alignCast(try storage.alloc.create(Page)));
            errdefer storage.alloc.destroy(@as(*Page, @ptrCast(@alignCast(storage.index))));
            storage.index.?.buckets[0] = try Run.create(V, storage.alloc);
            storage.index.?.bounds[0] = 0;
            storage.index.?.buckets[0].?.head.rc = 1;
            storage.n_runs += 1;

            return storage.add(V, key, val);
        };

        // TODO something better than linear?
        var i: usize = 0;
        while (i < storage.n_runs - 1 and key < index.bounds[i]) : (i += 1) {}
        try index.buckets[i].?.add(V, storage.alloc, key, val);
        storage.len += 1;
    }

    pub fn has(storage: *Storage, key: Entity) bool {
        std.debug.assert(key != nil);
        const index = storage.index orelse return false;
        var i: usize = 0;
        while (i < storage.n_runs - 1 and key <= index.bounds[i]) : (i += 1) {}
        return index.buckets[i].?.has(key);
    }

    pub fn compact(storage: *Storage, comptime V: type) !void {
        if (storage.immutable) return error.Immutable;

        const index = storage.index orelse return;
        std.debug.assert(storage.n_runs > 0);

        for (0..storage.n_runs) |i| {
            // ugh, what if memory alloc fails, not messing up is so annoying
            // TODO switch to just panicking if memory should run out probably
            // it's not like we can or care to fix it anyway
            index.buckets[i] = try index.buckets[i].?.compact(V, storage.alloc);
        }
    }

    pub fn cycle(storage: *Storage) !Storage {
        storage.immutable = true; // TODO move to replicate

        var new = Storage.init(storage.alloc);
        new.index = @ptrCast(@alignCast(try storage.alloc.create(Page)));
        new.len = storage.len;

        for (0..storage.n_runs) |i| {
            var run: ?*Run = storage.index.?.buckets[i].?;
            while (run != null) {
                run.?.head.rc += 1;
                new.index.?.buckets[new.n_runs] = run;
                new.index.?.bounds[new.n_runs] = run.?.head.keys[0];
                run = run.?.head.next;
                new.index.?.buckets[new.n_runs].?.head.next = null;
                new.n_runs += 1;
            }
        }

        return new;
    }

    pub fn debugPrint(storage: *Storage) void {
        std.debug.print("Storage", .{});
        if (storage.index) |index| {
            std.debug.print(" [ ", .{});
            for (0..storage.n_runs) |i| std.debug.print("{} ", .{index.bounds[i]});
            std.debug.print("]\n", .{});

            for (0..storage.n_runs) |i| index.buckets[i].?.debugPrint();
        } else {
            std.debug.print(" (empty)\n", .{});
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage.init(alloc);
    defer s.deinit();

    for (1..10) |i| {
        if (i % 2 == 0) continue;
        std.debug.print("inserting {}\n", .{i});
        try s.add(u32, i, @intCast(i));
        s.debugPrint();
    }

    try s.compact(u32);
    s.debugPrint();

    {
        var s_old = s;
        s = try s.cycle();
        s_old.deinit();
    }
    s.debugPrint();

    for (1..10) |i| {
        if (i % 2 == 1) continue;
        std.debug.print("inserting {}\n", .{i});
        try s.add(u32, i, @intCast(i));
        s.debugPrint();
    }

    try s.compact(u32);
    s.debugPrint();
    {
        var s_old = s;
        s = try s.cycle();
        s_old.deinit();
    }
    s.debugPrint();

    for (11..20) |i| {
        std.debug.print("inserting {}\n", .{i});
        try s.add(u32, i, @intCast(i));
        s.debugPrint();
    }

    try s.compact(u32);
    s.debugPrint();
    {
        var s_old = s;
        s = try s.cycle();
        s_old.deinit();
    }
    s.debugPrint();
}
