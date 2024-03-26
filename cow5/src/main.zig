const std = @import("std");
const primes = @import("primes.zig").primes;

const Entity = u64;
const nil: Entity = 0;

const Meta = packed struct {
    tombstone: bool,
    _pad: u7 = undefined,
    // TODO use extra space
    // try a fingerprint like in std.HashMap for faster searching
    // or alternatively a displacement distance for robinhood
};

// const PAGE_SIZE = 4 * 1024;
const PAGE_SIZE = 256;
const Page = struct {
    bytes: [PAGE_SIZE]u8 align(64),
};

const MapHeader = struct {
    keys: [*]Entity,
    vals: *anyopaque,
    meta: [*]Meta,
    len: usize,
    capacity: usize,
    mod: *const fn (usize) usize, // x -> x % capacity
};

const RunHeader = struct {
    keys: [*]Entity,
    vals: *anyopaque,
    len: usize,
    capacity: usize,
    map: ?*Map,
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
        var i: usize = 0;
        while (@sizeOf(MapLayout(primes[i], V)) < @sizeOf(Data)) : (i += 1) {}
        if (i == 0) @compileError(@typeName(V) ++ " is too large");
        return primes[i - 1];
    }

    /// find the biggest size that fits
    fn runCapacity(comptime V: type) comptime_int {
        var i: usize = 0;
        while (@sizeOf(RunLayout(i, V)) < @sizeOf(Data)) : (i += 1) {}
        if (i == 0) @compileError(@typeName(V) ++ " is too large");
        return i - 1;
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
        map.head.capacity = 0;
        map.head.mod = struct {
            fn mod(x: usize) usize {
                return x % capacity;
            }
        }.mod;

        return map;
    }

    fn destroy(map: *Map, alloc: std.mem.Allocator) void {
        const p: *Page = @alignCast(@ptrCast(map));
        alloc.destroy(p);
    }

    fn debugPrint(map: *Map) void {
        _ = map;
        std.debug.print("  [ ", .{});
        // for (0..run.head.len) |i| std.debug.print("{} ", .{run.head.keys[i]});
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
        run.head.capacity = 0;
        run.head.map = null;
        run.head.rc = 0;

        return run;
    }

    fn destroy(run: *Run, alloc: std.mem.Allocator) void {
        run.head.rc -= 1;
        if (run.head.rc == 0) {
            if (run.head.map) |map| map.destroy(alloc);
            const p: *Page = @alignCast(@ptrCast(run));
            alloc.destroy(p);
        }
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

const Storage = struct {
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
        const index = storage.index orelse {
            storage.index = @ptrCast(@alignCast(try storage.alloc.create(Page)));
            errdefer storage.alloc.destroy(@as(*Page, @ptrCast(@alignCast(storage.index))));
            storage.index.?.buckets[0] = try Run.create(V, storage.alloc);
            storage.index.?.bounds[0] = 0;
            storage.index.?.buckets[0].?.head.rc = 1;
            storage.n_runs += 1;

            return storage.add(V, key, val);
        };

        _ = index;
    }

    fn debugPrint(storage: *Storage) void {
        std.debug.print("Storage", .{});
        if (storage.index) |index| {
            std.debug.print(" [ ", .{});
            for (0..storage.n_runs) |i| std.debug.print("{} ", .{index.bounds[i]});
            std.debug.print("]\n", .{});

            for (0..storage.n_runs) |i| index.buckets[i].?.debugPrint();
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage.init(alloc);
    defer s.deinit();

    std.debug.print("{}\n", .{@sizeOf(Storage)});
    std.debug.print("{}\n", .{@sizeOf(Storage.Index)});

    for (1..20) |i| {
        std.debug.print("inserting {}\n", .{i});
        try s.add(u32, i, @intCast(i));
        s.debugPrint();
    }
}
