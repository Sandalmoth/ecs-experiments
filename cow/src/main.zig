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
                pool.free_list = item.next;
                break :blk item;
            } else pool.arena.allocator().create(Node) catch unreachable;

            node.* = Node{ .rc = RefCounted{ .item = undefined, .count = 1 } };
            return @ptrCast(node);
        }
    };
}

pub fn State(comptime Item: type) type {
    // NOTE: these are meant to be heap allocated
    // and passed around as pointers
    return struct {
        const Self = @This();

        pub const max_items = 256;
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
        pub fn step(state: *Self) *Self {
            var next = state.alloc.create(Self) catch unreachable;
            next.* = state.*;
            next.prev = state;
            for (0..max_items) |i| {
                if (next.sparse[i] != null) {
                    next.sparse[i].?.count += 1;
                }
            }
            return next;
        }

        pub fn create(state: *Self) Handle {
            const handle = state.n_items;
            const rc = state.pool.create();
            state.sparse[handle] = rc;
            return handle;
        }

        pub fn get(state: *Self, handle: Handle) ?*const Item {
            // note how COW means we shouldn't unconditionally return a pointer
            if (state.sparse[handle]) |rc| {
                return &rc.item;
            }
            return null;
        }

        pub fn set(state: *Self, handle: Handle, item: Item) void {
            // copy if refcount > 1, else just overwrite
            if (state.sparse[handle]) |rc| {
                std.debug.print("{*}\n", .{rc});
                std.debug.assert(rc.count > 0);
                if (rc.count == 1) {
                    rc.item = item;
                } else {
                    const newrc = state.pool.create();
                    newrc.item = item;
                    state.sparse[handle] = newrc;
                    rc.count -= 1;
                }
            } else {
                std.log.warn("called set with invalid handle {} (NOOP)", .{handle});
            }
        }
    };
}

const _E = struct {
    pos: @Vector(4, f32),
    hp: u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var pool = MemoryPool(_E).init(alloc);
    defer pool.deinit();

    std.debug.print("{} {}\n", .{
        @sizeOf(@TypeOf(pool).Node),
        @alignOf(@TypeOf(pool).Node),
    });

    var state = State(_E).init(alloc, &pool);
    std.debug.print("{*}\n", .{state});
    std.debug.print("{*}\n", .{state.prev});
    std.debug.print("{*}\n", .{state.prev.?.prev});

    var a = state.create();
    std.debug.print("{*}\n", .{state.get(a)});
    std.debug.print("{}\n", .{state.get(a).?.*});
    state.set(a, _E{ .pos = @splat(1), .hp = 123 });
    std.debug.print("{*}\n", .{state.get(a)});
    std.debug.print("{}\n", .{state.get(a).?.*});

    state = state.step();
    std.debug.print("{*}\n", .{state.get(a)});
    std.debug.print("{}\n", .{state.get(a).?.*});
    state.set(a, _E{ .pos = @splat(2), .hp = 234 });
    std.debug.print("{*}\n", .{state.get(a)});
    std.debug.print("{}\n", .{state.get(a).?.*});
}

test "simple test" {}
