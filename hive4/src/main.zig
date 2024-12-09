const builtin = @import("builtin");
const std = @import("std");

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
const single_threaded = builtin.single_threaded;

const BlockPoolConfig = struct {
    size: usize = 16 * 1024,
    initial_capacity: usize = 16,
    can_expand: bool = true,
};
// for simplicity, just define the type here, but may be more inlfexible than needed
// so if possible, storage should probably take the pool type as a parameter
// although, that would require propagating that parameter to buckets and pages as well
const BlockPool = BlockPoolType(.{});

pub fn BlockPoolType(comptime config: BlockPoolConfig) type {
    // TODO rewrite such that reclaiming memory is possible
    // probably by keeping track of block arrays, and always allocating into the biggest one
    // then freeing whenever one becomes empty

    // TODO protect by mutex if not single threaded

    std.debug.assert(std.math.isPowerOfTwo(config.size));
    std.debug.assert(config.initial_capacity > 0);

    return struct {
        const Self = @This();
        const size = config.size; // s.t. it can be referenced by users

        // extern union because the size needs to equal config.size even in debug mode
        const Block = extern union {
            bytes: [config.size]u8,
            next: ?*Block,
        };
        comptime {
            std.debug.assert(@sizeOf(Block) == config.size);
        }

        arena: std.heap.ArenaAllocator,
        capacity: usize,
        free: ?*Block,

        pub fn init() !Self {
            var pool = Self{
                .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
                .capacity = 0,
                .free = null,
            };
            try pool.expand(config.initial_capacity);
            return pool;
        }

        pub fn deinit(pool: *Self) void {
            pool.arena.deinit();
            pool.* = undefined;
        }

        pub fn create(pool: *Self, comptime T: type) !*T {
            std.debug.assert(@sizeOf(T) <= config.size);
            std.debug.assert(@alignOf(T) <= config.size);
            if (pool.free == null) {
                if (!config.can_expand) return error.OutOfMemory;
                try pool.expand(pool.capacity);
            }
            const block = pool.free.?;
            pool.free = block.next;
            return @alignCast(@ptrCast(block));
        }

        pub fn destroy(pool: *Self, ptr: anytype) void {
            const block: *Block = @alignCast(@ptrCast(ptr));
            block.next = pool.free;
            pool.free = block;
        }

        fn expand(pool: *Self, n: usize) !void {
            // we are guaranteed to find at least n blocks with the right alignment
            // in any continuous memory region of length n + 1
            var segment = try pool.arena.allocator().alloc(Block, n + 1);
            var a = @intFromPtr(&segment[0]);
            for (0..n) |_| {
                a = std.mem.alignForward(usize, a, config.size);
                const block: *Block = @ptrFromInt(a);
                block.next = pool.free;
                pool.free = block;
                a += config.size;
                pool.capacity += 1;
            }
        }
    };
}

test "block pool basic functions" {
    var pool = try BlockPool.init();
    defer pool.deinit();

    const types = [_]type{ u8, u32, @Vector(4, f32), [1024]f64 };

    inline for (types) |T| {
        var addresses = std.AutoArrayHashMap(usize, void).init(std.testing.allocator);
        defer addresses.deinit();

        for (0..1000) |_| {
            const p = try pool.create(T);
            const a = @intFromPtr(p);
            try std.testing.expect(a % BlockPool.size == 0);
            try addresses.put(a, {});
        }

        for (addresses.keys()) |a| {
            const p: *T = @ptrFromInt(a);
            pool.destroy(p);
        }

        for (0..2000) |i| {
            const p = try pool.create(T);
            const a = @intFromPtr(p);
            try std.testing.expect(a % BlockPool.size == 0);
            if (i < 1000) {
                try std.testing.expect(addresses.contains(a));
            } else {
                try std.testing.expect(!addresses.contains(a));
            }
        }
    }
}

const PageHeader = struct {
    capacity: usize,
    len: usize,
    _keys: [*]u64,
    _skip: [*]u16, // low complexity jump counting pattern
    _vals: usize,
    free: u16,
};

const Page = struct {
    head: PageHeader,
    bytes: [BlockPool.size - 64]u8,

    fn getKeys(page: Page) []u64 {
        return page.head._keys[0..page.head.capacity];
    }

    fn getSkip(page: Page) []u16 {
        return page.head._skip[0 .. page.head.capacity + 1];
    }

    fn getVals(page: Page, comptime T: type) []Data(T) {
        const v: [*]Data(T) = @ptrFromInt(page.head._vals);
        return v[0..page.head.capacity];
    }

    fn lessThan(context: void, a: *Page, b: *Page) std.math.Order {
        _ = context;
        return std.math.order(a.head.len, b.head.len);
    }

    fn full(page: Page) bool {
        std.debug.assert(page.head.len <= page.head.capacity);
        return page.head.len == page.head.capacity;
    }

    const Node = struct {
        next: u16,
        prev: u16,
    };
    fn KeyNode(comptime T: type) type {
        return union { key: u64, node: Node };
    }

    fn create(comptime T: type, pool: *BlockPool) !*Page {
        const page = try pool.create(Page);
        const capacity: usize = @min(
            (page.bytes.len - @sizeOf(u16)) / (@sizeOf(u64) + @sizeOf(u16) + @sizeOf(T)),
            std.math.maxInt(u16) - 1,
        );
        page.head.capacity = capacity;
        page.head.len = 0;

        // suballocate data to hold arrays of keys, skipfields, and values
        var p = @intFromPtr(&page.bytes[0]);
        p = std.mem.alignForward(usize, p, @alignOf(u64));
        page.head._keys = @ptrFromInt(p);
        p += @sizeOf(u64) * capacity;
        p = std.mem.alignForward(usize, p, @alignOf(u16));
        page.head._skip = @ptrFromInt(p);
        p += @sizeOf(u16) * (capacity + 1);
        p = std.mem.alignForward(usize, p, @alignOf(T));
        page.head._vals = p;
        p += @sizeOf(T) * capacity;
        std.debug.assert(p < @intFromPtr(&page.bytes[0]) + page.bytes.len);

        // skipfield setup
        page.head.free = 0;
        const skip = page.getSkip();
        skip[0] = capacity;
        skip[capacity - 1] = capacity;
        skip[capacity] = 0;
        const vals = page.getVals(T);
        vals[0] = .{ .node = .{
            .prev = 0,
            .next = 0,
        } };

        return page;
    }

    fn destroy(page: *Page, pool: *BlockPool) void {
        pool.destroy(page);
    }

    fn insert(page: *Page, comptime T: type, key: u64, val: T) u16 {
        std.debug.assert(page.head.len < page.head.capacity);
        const ix = page.head.free;
        const keys = page.getKeys();
        const vals = page.getVals(T);
        const skip = page.getSkip();
        std.debug.assert(skip[ix] > 0);
        std.debug.assert(skip[ix] == skip[ix + skip[ix] - 1]);
        const free_block = vals[ix].node;
        const free_block_len = skip[ix];
        // update the skip list
        skip[ix + 1] = skip[ix] - 1;
        if (skip[ix] > 2) skip[ix + skip[ix] - 1] -= 1;
        skip[ix] = 0;
        std.debug.print("{} {} {}\n", .{ ix, skip[ix + 1], page.head.capacity - ix });
        std.debug.assert(skip[ix + 1] < page.head.capacity - ix);
        // update erasure list
        if (free_block_len > 1) {
            vals[ix + 1] = .{ .node = .{
                .prev = ix + 1,
                .next = if (free_block.next != ix) free_block.next else ix + 1,
            } };
            page.head.free += 1;
        } else {
            // free block is exhausted
            std.debug.assert(vals[ix].node.prev == ix);
            if (free_block.next != ix) vals[free_block.next].node.prev = free_block.next;
            page.head.free = vals[ix].node.next;
        }
        keys[ix] = key;
        vals[ix] = .{ .value = val };
        page.head.len += 1;
        return ix;
    }

    fn debugPrint(page: *Page, comptime T: type) void {
        std.debug.print("  Page - {x} - len={} - [ ", .{ @intFromPtr(page), page.head.len });
        for (0..page.head.capacity) |i| {
            // if (bucket.indices[i] == ix_nil) continue;
            // std.debug.print("({x} {})\n", .{ @intFromPtr(bucket.pages[i]), bucket.indices[i] });
            _ = i;
        }
        _ = T;
        std.debug.print("]\n", .{});
    }
};

const Bucket = struct {
    const capacity = std.math.floorPowerOfTwo(
        usize,
        (BlockPool.size - 64) / std.math.ceilPowerOfTwoAssert(
            usize,
            @sizeOf(usize) + @sizeOf(u16) + @sizeOf(u8),
        ),
    );

    const ix_nil = std.math.maxInt(u16);

    len: usize,
    pages: [capacity]*Page,
    indices: [capacity]u16,
    fingerprints: [capacity]u8,

    fn create(pool: *BlockPool) !*Bucket {
        const bucket = try pool.create(Bucket);
        bucket.len = 0;
        bucket.indices = .{ix_nil} ** capacity;
        return bucket;
    }

    fn destroy(bucket: *Bucket, pool: *BlockPool) void {
        pool.destroy(bucket);
    }

    fn full(bucket: Bucket) bool {
        return bucket.len * 7 > capacity * 6;
    }

    const InsertResult = struct {
        success: bool,
        page: **Page,
        index: *u16,
    };
    fn insert(bucket: *Bucket, key: u64) InsertResult {
        std.debug.assert(bucket.len < capacity);

        const fingerprint: u8 = @intCast(key >> 56);
        var ix = (key >> 32) % capacity;

        while (bucket.indices[ix] != ix_nil) : (ix = (ix + 1) % capacity) {
            if (bucket.fingerprints[ix] == fingerprint) {
                const page = bucket.pages[ix];
                std.debug.assert(bucket.indices[ix] < page.head.capacity);
                const keys = page.getKeys();
                const k = keys[bucket.indices[ix]];
                if (k == key) return .{
                    .success = false,
                    .page = undefined,
                    .index = undefined,
                };
            }
        }

        std.debug.assert(bucket.indices[ix] == ix_nil);
        bucket.len += 1;
        return .{
            .success = true,
            .page = &bucket.pages[ix],
            .index = &bucket.indices[ix],
        };
    }

    fn debugPrint(bucket: Bucket) void {
        std.debug.print("  Bucket - len={} - [ ", .{bucket.len});
        for (0..capacity) |i| {
            if (bucket.indices[i] == ix_nil) continue;
            std.debug.print("({x} {}) ", .{ @intFromPtr(bucket.pages[i]), bucket.indices[i] });
        }
        std.debug.print("]\n", .{});
    }
};

comptime {
    std.debug.assert(@sizeOf(Page) < BlockPool.size);
    std.debug.assert(@sizeOf(Bucket) < BlockPool.size);
    std.debug.assert(Bucket.capacity > 1);
}

const Hive = struct {
    pool: *BlockPool,
    buckets: std.ArrayList(*Bucket), // extendible hashing index
    levels: std.ArrayList(usize), // level of each bucket
    pages: std.PriorityQueue(*Page, void, Page.lessThan),
    len: usize,
    level: usize, // global hash level

    pub fn init(alloc: std.mem.Allocator, pool: *BlockPool) Hive {
        const hive = Hive{
            .pool = pool,
            .buckets = std.ArrayList(*Bucket).init(alloc),
            .levels = std.ArrayList(usize).init(alloc),
            .pages = std.PriorityQueue(*Page, void, Page.lessThan).init(alloc, {}),
            .len = 0,
            .level = 0,
        };
        return hive;
    }

    pub fn deinit(hive: *Hive) void {
        hive.buckets.deinit();
        hive.levels.deinit();
        hive.pages.deinit();
        hive.* = undefined;
    }

    pub fn contains(hive: Hive, key: u64) bool {
        if (hive.size == 0) return false;
        _ = key;
    }

    pub fn get(hive: *Hive, comptime T: type, key: u64) ?T {
        _ = hive;
        _ = key;
    }

    pub fn getPtr(hive: *Hive, comptime T: type, key: u64) ?*T {
        _ = hive;
        _ = key;
    }

    /// noop if present (returns false), true means key was added
    pub fn insert(hive: *Hive, comptime T: type, key: u64, val: T) !bool {
        try hive.ensureCapacity(T);

        // first try to insert into bucket, if it fails the key is already present
        const ix = hive.bucketIndex(key);
        std.debug.assert(hive.buckets.items.len > ix);
        const result = hive.buckets.items[ix].insert(
            key,
        );
        if (!result.success) return false;

        // key not present, actually store data on page, note the updating on the bucket
        result.page.* = hive.pages.items[0];
        std.debug.assert(!hive.pages.items[0].full());
        result.index.* = hive.pages.items[0].insert(T, key, val);

        // finally, if we filled the bucket then split the bucket
        if (hive.buckets.items[ix].full()) {
            @panic("TODO");
        }

        return true;
    }

    /// noop if not present (returns false), true means key was removed
    pub fn remove(hive: *Hive, comptime T: type, key: u64) !bool {
        _ = hive;
        _ = T;
        _ = key;
    }

    fn ensureCapacity(hive: *Hive, comptime T: type) !void {
        if (hive.buckets.items.len == 0) {
            try hive.buckets.ensureUnusedCapacity(1);
            const bucket = try Bucket.create(hive.pool);
            hive.buckets.append(bucket) catch unreachable;
            hive.levels.append(0) catch unreachable;
        }

        if (hive.pages.items.len == 0 or hive.pages.items[0].full()) {
            try hive.pages.ensureUnusedCapacity(1);
            const page = try Page.create(T, hive.pool);
            hive.pages.add(page) catch unreachable;
        }
    }

    fn bucketIndex(hive: Hive, key: u64) usize {
        const mask: u64 = @intCast((@as(usize, 1) >> @intCast(hive.level)) - 1);
        return key & mask;
    }

    fn debugPrint(hive: Hive, comptime T: type) void {
        std.debug.print("Hive <{s}> - len={}\n", .{ @typeName(T), hive.len });
        for (hive.buckets.items) |bucket| bucket.debugPrint();
        for (hive.pages.items) |page| page.debugPrint(T);
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var pool = try BlockPool.init();
    defer pool.deinit();

    var hive = Hive.init(alloc, &pool);
    defer hive.deinit();

    _ = try hive.insert(u32, 12, 12);
    _ = try hive.insert(u32, 23, 23);
    _ = try hive.insert(u32, 34, 34);
    hive.debugPrint(u32);
}
