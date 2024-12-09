const builtin = @import("builtin");
const std = @import("std");

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
const single_threaded = builtin.single_threaded;

const BlockPoolConfig = struct {
    block_size: usize = 16 * 1024,
    n_blocks_min: usize = 4,
};
pub const BlockPool = BlockPoolType(.{});

fn BlockPoolType(comptime config: BlockPoolConfig) type {
    std.debug.assert(std.math.isPowerOfTwo(config.block_size));
    std.debug.assert(config.n_blocks_min > 0);

    return struct {
        const Pool = @This();
        const block_size = config.block_size;

        const DummyMutex = struct {
            fn lock(_: *DummyMutex) void {}
            fn unlock(_: *DummyMutex) void {}
        };

        const Segment = struct {
            capacity: usize,
            n_free: usize,
            free: ?*Block,
            bytes: []u8,

            fn lessThan(context: void, a: Segment, b: Segment) std.math.Order {
                _ = context;
                return std.math.order(a.n_free -% 1, b.n_free -% 1);
            }

            fn contains(segment: Segment, ptr: usize) bool {
                const base = @intFromPtr(segment.bytes.ptr);
                return ptr >= base and ptr < base + segment.bytes.len;
            }
        };

        // must be extern union, since the size must match block size even in debug mode
        const Block = extern union {
            bytes: [block_size]u8,
            next: ?*Block,
        };
        comptime {
            std.debug.assert(@sizeOf(Block) == block_size);
        }

        alloc: std.mem.Allocator,
        segments: std.PriorityQueue(Segment, void, Segment.lessThan),
        reserve: ?Segment,
        capacity: usize,
        n_free: usize,
        mutex: if (single_threaded) DummyMutex else std.Thread.Mutex,

        pub fn init(alloc: std.mem.Allocator) !Pool {
            return .{
                .alloc = alloc,
                .segments = std.PriorityQueue(Segment, void, Segment.lessThan).init(
                    alloc,
                    {},
                ),
                .reserve = null,
                .capacity = 0,
                .n_free = 0,
                .mutex = if (single_threaded) DummyMutex{} else std.Thread.Mutex{},
            };
        }

        pub fn deinit(pool: *Pool) void {
            for (pool.segments.items) |segment| pool.alloc.free(segment.bytes);
            if (pool.reserve) |reserve| pool.alloc.free(reserve.bytes);
            pool.segments.deinit();
            pool.* = undefined;
        }

        pub fn create(pool: *Pool, comptime T: type) !*T {
            pool.mutex.lock();
            defer pool.mutex.unlock();
            std.debug.assert(@sizeOf(T) <= block_size);
            std.debug.assert(@alignOf(T) <= block_size);
            if (pool.n_free == 0) try pool.expand();
            std.debug.assert(pool.segments.items.len > 0);
            var segment = pool.segments.remove();
            std.debug.assert(segment.n_free > 0);
            std.debug.assert(segment.free != null);
            const block = segment.free.?;
            segment.free = block.next;
            segment.n_free -= 1;
            pool.n_free -= 1;
            pool.segments.add(segment) catch unreachable;
            return @alignCast(@ptrCast(block));
        }

        pub fn destroy(pool: *Pool, ptr: anytype) void {
            pool.mutex.lock();
            defer pool.mutex.unlock();
            std.debug.assert(pool.segments.items.len > 0);
            var ix: usize = undefined;
            for (pool.segments.items, 0..) |segment, i| {
                if (segment.contains(@intFromPtr(ptr))) {
                    ix = i;
                    break;
                }
            } else unreachable;
            var segment = pool.segments.removeIndex(ix);
            const block: *Block = @alignCast(@ptrCast(ptr));
            block.next = segment.free;
            segment.free = block;
            segment.n_free += 1;
            pool.n_free += 1;

            if (segment.n_free < segment.capacity) {
                pool.segments.add(segment) catch unreachable;
                return;
            }

            pool.capacity -= segment.capacity;
            pool.n_free -= segment.capacity;
            if (pool.reserve == null) {
                pool.reserve = segment;
            } else {
                const reserve = pool.reserve.?;
                if (reserve.capacity < segment.capacity) {
                    pool.alloc.free(reserve.bytes);
                    pool.reserve = segment;
                } else {
                    pool.alloc.free(segment.bytes);
                }
            }
        }

        fn expand(pool: *Pool) !void {
            try pool.segments.ensureUnusedCapacity(1);

            if (pool.reserve) |reserve| {
                std.debug.assert(reserve.n_free == reserve.capacity);
                pool.segments.add(reserve) catch unreachable;
                std.debug.assert(reserve.n_free == reserve.capacity);
                pool.capacity += reserve.capacity;
                pool.n_free += reserve.capacity;
                pool.reserve = null;
                return;
            }

            // we are guaranteed to fit n blocks with the block_size alignment
            // in a continuous memory region of length (n + 1) * block_size
            const n_new = @max(config.n_blocks_min, pool.capacity);
            var segment = Segment{
                .capacity = 0,
                .n_free = 0,
                .free = null,
                .bytes = try pool.alloc.alloc(u8, block_size * (n_new + 1)),
            };
            var ptr = @intFromPtr(segment.bytes.ptr);
            for (0..n_new) |_| {
                ptr = std.mem.alignForward(usize, ptr, block_size);
                const block: *Block = @ptrFromInt(ptr);
                block.next = segment.free;
                segment.free = block;
                ptr += block_size;
                segment.capacity += 1;
            }
            segment.n_free = segment.capacity;
            pool.capacity += segment.capacity;
            pool.n_free += segment.capacity;
            pool.segments.add(segment) catch unreachable;
        }
    };
}

test "block pool basic functions" {
    var pool = try BlockPool.init(std.testing.allocator);
    defer pool.deinit();

    const types = [_]type{ u8, u32, @Vector(4, f32), [1024]f64 };
    const N = 1000;

    inline for (types) |T| {
        var addresses = std.AutoArrayHashMap(usize, void).init(std.testing.allocator);
        defer addresses.deinit();

        for (0..N) |_| {
            const p = try pool.create(T);
            const a = @intFromPtr(p);
            try std.testing.expect(a % BlockPool.block_size == 0);
            try std.testing.expect(!addresses.contains(a));
            try addresses.put(a, {});
        }

        for (addresses.keys()) |a| {
            const p: *T = @ptrFromInt(a);
            pool.destroy(p);
        }

        var addresses2 = std.AutoArrayHashMap(usize, void).init(std.testing.allocator);
        defer addresses2.deinit();

        for (0..2 * N) |_| {
            const p = try pool.create(T);
            const a = @intFromPtr(p);
            try std.testing.expect(a % BlockPool.block_size == 0);
            try std.testing.expect(!addresses2.contains(a));
            try addresses2.put(a, {});
        }

        for (addresses2.keys()) |a| {
            const p: *T = @ptrFromInt(a);
            pool.destroy(p);
        }
    }
}
