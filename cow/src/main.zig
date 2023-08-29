const std = @import("std");

// idea
// sparse set on top of a shared object pool
// all entities are shallow-copied each timestep
// by writing a sparse set, but without the data
// and then using copy-on-write when actually updated
// based around the idea where
// an entities state at t+1 depends on the world at t

// for this prototype, we're doing megastruct without active/inactive components

pub const Handle = packed struct {
    generation: u24,
    slot: u8,
};

pub fn MemoryPool(comptime Item: type) type {
    // this is basically std.heap.MemoryPool
    // with some features added/removed

    return struct {
        const Self = @This();

        pub const item_size = @max(@sizeOf(Node), @sizeOf(Item));
        // note the hack for the alignment of a node
        // since the node definition needs to access this
        // using Node in the definition would be circular
        // however, presumably all pointers have the same alignment
        // and the fact that it's a struct member should add nothing
        // so we can just use a pointer to whatever as a proxy
        pub const item_alignment = @max(@alignOf(?*usize), @alignOf(Item));

        const Node = struct {
            next: ?*align(item_alignment) @This(),
        };

        arena: std.heap.ArenaAllocator,
        free_list: ?*align(item_alignment) Node = null,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .arena = std.heap.ArenaAllocator.init(alloc),
            };
        }

        pub fn deinit(pool: *Self) void {
            pool.arena.deinit();
            pool.* = undefined;
        }

        pub fn create(pool: *Self) *Item {
            const node = if (pool.free_list) |item| blk: {
                pool.free_list = item.next;
                break :blk item;
            } else @as(*align(item_alignment) Node, @ptrCast(pool.allocNew()));

            const ptr = @as(*Item, @alignCast(@ptrCast(node)));
            ptr.* = undefined;
            return ptr;
        }

        fn allocNew(pool: *Self) *align(item_alignment) [item_size]u8 {
            const mem = pool.arena.allocator().alignedAlloc(u8, item_alignment, item_size) catch unreachable;
            return mem[0..item_size];
        }
    };
}

pub fn State(comptime Item: type) type {
    // NOTE: these are meant to be heap allocated
    // and passed around as pointers
    return struct {
        const Self = @This();

        pub const max_items = 256;

        alloc: std.mem.Allocator,
        pool: *MemoryPool(Item),
        prev: ?*Self,

        sparse: [max_items]?*Item,

        pub fn init(alloc: std.mem.Allocator, pool: *MemoryPool(Item)) *Self {
            var prev = alloc.create(Self) catch unreachable;
            prev.* = Self{
                .alloc = alloc,
                .pool = pool,
                .prev = null,
                .sparse = [_]?*Item{null} ** max_items,
            };

            var state = alloc.create(Self) catch unreachable;
            state.* = Self{
                .alloc = alloc,
                .pool = pool,
                .prev = prev,
            };
            return state;
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

    std.debug.print("{} {}\n", .{ @TypeOf(pool).item_size, @TypeOf(pool).item_alignment });

    var state = State(_E).init(alloc, &pool);
    std.debug.print("{*}\n", .{state});
    std.debug.print("{*}\n", .{state.prev});
    std.debug.print("{*}\n", .{state.prev.?.prev});

    var a = pool.create();
    std.debug.print("{*}\n", .{a});
    std.debug.print("{}\n", .{a.*});
    var b = pool.create();
    std.debug.print("{*}\n", .{b});
    std.debug.print("{}\n", .{b.*});
}

test "simple test" {}
