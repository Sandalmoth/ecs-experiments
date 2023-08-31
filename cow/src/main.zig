const std = @import("std");

// idea
// sparse set on top of a shared object pool
// all entities are shallow-copied each timestep
// by writing a sparse set, but without the data
// and then using copy-on-write when actually updated
// based around the idea where
// an entities state at t+1 depends on the world at t

// for this prototype, we're doing megastruct without active/inactive components

pub const Handle = u32;
pub const HandleDetail = packed struct {
    generation: u24,
    slot: u8,
};

pub fn MemoryPool(comptime Item: type) type {
    // this is loosely based on std.heap.MemoryPool
    // but needs to include reference counting

    return struct {
        const Self = @This();

        pub const RefCounted = struct {
            item: Item,
            count: u32,
        };

        pub const Node = union {
            rc: RefCounted,
            next: ?*Node,
        };

        arena: std.heap.ArenaAllocator,
        free_list: ?*Node = null,

        // not needed, but fun stats
        total_allocations: usize = 0,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .arena = std.heap.ArenaAllocator.init(alloc),
            };
        }

        pub fn deinit(pool: *Self) void {
            pool.arena.deinit();
            pool.* = undefined;
        }

        pub fn create(pool: *Self) *RefCounted {
            const node = if (pool.free_list) |item| blk: {
                std.debug.print("reused\n", .{});
                pool.free_list = item.next;
                break :blk item;
            } else blk: {
                std.debug.print("allocated\n", .{});
                pool.total_allocations += 1;
                break :blk pool.arena.allocator().create(Node) catch unreachable;
            };

            node.* = Node{ .rc = RefCounted{ .item = undefined, .count = 1 } };
            return @ptrCast(node);
        }

        /// return an Item to the memorypool so it can be reused
        pub fn destroy(pool: *Self, ptr: *RefCounted) void {
            std.debug.print("returned\n", .{});
            const node: *Node = @ptrCast(ptr);

            node.* = Node{ .next = pool.free_list };
            pool.free_list = node;
        }
    };
}

pub fn State(comptime Item: type) type {
    // NOTE: these are meant to be heap allocated
    // and passed around as pointers
    return struct {
        const Self = @This();

        pub const max_items = 1024;
        pub const max_steps = 16;
        pub const RefCounted = MemoryPool(Item).RefCounted;

        alloc: std.mem.Allocator,
        pool: *MemoryPool(Item),
        prev: ?*Self,

        sparse: [max_items]?*RefCounted,
        n_items: u32 = 0, // TODO properly reuse slots

        pub fn init(alloc: std.mem.Allocator, pool: *MemoryPool(Item)) *Self {
            var prev = alloc.create(Self) catch unreachable;
            prev.* = Self{
                .alloc = alloc,
                .pool = pool,
                .prev = null,
                .sparse = [_]?*RefCounted{null} ** max_items,
            };

            var state = alloc.create(Self) catch unreachable;
            state.* = Self{
                .alloc = alloc,
                .pool = pool,
                .prev = prev,
                .sparse = [_]?*RefCounted{null} ** max_items,
            };
            return state;
        }

        /// state transition, sets current state as prev
        /// deletes states older than max_steps
        pub fn step(state: *Self) *Self {
            var next = state.alloc.create(Self) catch unreachable;
            next.* = Self{
                .alloc = state.alloc,
                .pool = state.pool,
                .prev = state,
                .sparse = state.sparse,
            };
            for (0..max_items) |i| {
                if (next.sparse[i] != null) {
                    next.sparse[i].?.count += 1;
                }
            }

            var first = next;
            var second = first;
            var j: usize = 0;
            while (true) {
                if (first.prev != null) {
                    j += 1;
                    second = first;
                    first = first.prev.?;
                } else {
                    break;
                }
            }
            std.debug.print("j {}\n", .{j});
            if (j > max_steps) {
                second.prev = null;
                std.debug.print("YO\n", .{});
                for (0..max_items) |i| {
                    if (first.sparse[i] != null) {
                        std.debug.print("{}\n", .{first.sparse[i].?.*});
                        std.debug.assert(first.sparse[i].?.count > 0);
                        if (first.sparse[i].?.count == 1) {
                            state.pool.destroy(first.sparse[i].?);
                        } else {
                            first.sparse[i].?.count -= 1;
                        }
                    }
                }
                state.alloc.destroy(first);
            }

            return next;
        }

        pub fn create(state: *Self) Handle {
            const handle = state.n_items;
            const rc = state.pool.create();
            state.sparse[handle % max_items] = rc;
            return handle;
        }

        pub fn destroy(state: *Self, handle: Handle) void {
            if (state.sparse[handle % max_items]) |rc| {
                rc.count -= 1;
                if (rc.count == 0) {
                    state.pool.destroy(rc);
                    state.sparse[handle % max_items] = null;
                }
            } else {
                std.log.warn("called destroy with invalid handle {} (NOOP)", .{handle});
            }
        }

        pub fn get(state: *Self, handle: Handle) ?*const Item {
            // note how COW means we shouldn't unconditionally return a pointer
            if (state.sparse[handle % max_items]) |rc| {
                return &rc.item;
            }
            return null;
        }

        pub fn set(state: *Self, handle: Handle, item: Item) void {
            // copy if refcount > 1, else just overwrite
            if (state.sparse[handle % max_items]) |rc| {
                std.debug.assert(rc.count > 0);
                if (rc.count == 1) {
                    rc.item = item;
                } else {
                    const newrc = state.pool.create();
                    newrc.item = item;
                    state.sparse[handle % max_items] = newrc;
                    rc.count -= 1;
                }
            } else {
                std.log.warn("called set with invalid handle {} (NOOP)", .{handle});
            }
        }

        pub const Iterator = struct {
            sparse: [max_items]?*RefCounted,
            cursor: usize,

            /// captures the entities present when created, hence creating during iteration is safe, though they will be ignored
            fn init(sparse: [max_items]?*RefCounted) Iterator {
                return Iterator{
                    .sparse = sparse,
                    .cursor = 0,
                };
            }

            /// NOTE the handles returned are not real, they're only the slot part but skips the generation
            fn next(iter: *Iterator) ?Handle {
                while (iter.cursor < iter.sparse.len and
                    iter.sparse[iter.cursor] == null) : (iter.cursor += 1)
                {}
                if (iter.cursor < iter.sparse.len) {
                    // const tmp = iter.sparse[iter.cursor].?;
                    const tmp: Handle = @intCast(iter.cursor);
                    iter.cursor += 1;
                    return tmp;
                } else {
                    return null;
                }
            }
        };
    };
}

const _E = struct {
    pos: @Vector(4, f32),
    hp: u8,
};

pub fn benchmark(alloc: std.mem.Allocator) void {
    var rng = std.rand.DefaultPrng.init(2701);
    const N = State(_E).max_items;

    var pool = MemoryPool(_E).init(alloc);
    defer pool.deinit();

    var state = State(_E).init(alloc, &pool);

    var n: usize = 0;
    for (0..N / 2) |_| {
        const e = state.create();
        state.set(e, _E{ .pos = @splat(rng.random().float(f32)), .hp = rng.random().int(u8) });
        n += 1;
    }

    for (0..10000) |_| {
        if (rng.random().boolean()) {
            if (rng.random().boolean() and n < N) {
                // create
                const e = state.create();
                state.set(e, _E{ .pos = @splat(rng.random().float(f32)), .hp = rng.random().int(u8) });
                n += 1;
            } else if (n > 0) {
                // destroy
                while (true) {
                    const handle: Handle = @intCast(rng.random().int(u10)); // ugly hack
                    std.debug.print("YO {} {}\n", .{ n, handle });
                    std.debug.print("{?}\n", .{state.get(handle)});
                    if (state.get(handle) != null) {
                        state.destroy(handle);
                        n -= 1;
                        break;
                    }
                }
            }
        } else {
            // TODO abstract the iter construction better
            var iter = State(_E).Iterator.init(state.sparse);
            while (iter.next()) |handle| {
                const e = state.get(handle).?;
                state.set(handle, _E{
                    .pos = e.pos * @as(@Vector(4, f32), @splat(rng.random().float(f32))),
                    .hp = if (e.hp % 2 == 0) e.hp / 2 else e.hp *| 3 - 1,
                });
            }
            state = state.step();
        }
    }

    std.debug.print("total allocations {}\n", .{pool.total_allocations});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    benchmark(alloc);

    // var pool = MemoryPool(_E).init(alloc);
    // defer pool.deinit();

    // std.debug.print("{} {}\n", .{
    //     @sizeOf(@TypeOf(pool).Node),
    //     @alignOf(@TypeOf(pool).Node),
    // });

    // var state = State(_E).init(alloc, &pool);
    // std.debug.print("{*}\n", .{state});
    // std.debug.print("{*}\n", .{state.prev});
    // std.debug.print("{*}\n", .{state.prev.?.prev});

    // var a = state.create();
    // std.debug.print("{*}\n", .{state.get(a)});
    // std.debug.print("{}\n", .{state.get(a).?.*});
    // state.set(a, _E{ .pos = @splat(1), .hp = 123 });
    // std.debug.print("{*}\n", .{state.get(a)});
    // std.debug.print("{}\n", .{state.get(a).?.*});

    // state = state.step();
    // std.debug.print("{*}\n", .{state.get(a)});
    // std.debug.print("{}\n", .{state.get(a).?.*});
    // state.set(a, _E{ .pos = @splat(2), .hp = 234 });
    // std.debug.print("{*}\n", .{state.get(a)});
    // std.debug.print("{}\n", .{state.get(a).?.*});

    // var b = state.create();
    // for (0..50) |i| {
    //     if ((i * i + i) % 3 == 0) {
    //         state.set(a, _E{ .pos = @splat(@floatFromInt(i)), .hp = 234 });
    //     }
    //     if ((i) % 5 == 0) {
    //         state.set(b, _E{ .pos = @splat(@floatFromInt(i)), .hp = 123 });
    //     }
    //     std.debug.print("{}\n", .{i});
    //     state = state.step();
    //     std.debug.print("{*}\n", .{state});
    // }

    // std.debug.print("total allocations {}\n", .{pool.total_allocations});
}

test "simple test" {}
