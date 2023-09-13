const std = @import("std");

// Thoughts:
// Is the MemoryPool free_list unneccessary in the context of the State chain?
// Since we only use a pool transiently (then transitioning state)
// we might be more performant leaking memory
// since otherwise, create & destroy comes at the cost of finding
// and deactivating the item slot in the right page.
// We could:
// - remove the free list, just leak memory and reclaim with State.step
// - store a .{next: ?*Node, .slot: *bool} in the node (doubles the size though) to make create faster

fn getIndex(ptr: anytype, slice: []@TypeOf(ptr.*)) ?usize {
    // based on https://github.com/FlorenceOS/Florence/blob/master/lib/util/pointers.zig
    const a = @intFromPtr(ptr);
    const b = @intFromPtr(slice.ptr);
    if (b > a) {
        return null;
    }
    // can @sizeOf be different from the spacing in an array?
    // I don't think so, but doesn't hurt to make sure
    std.debug.assert(@sizeOf(@TypeOf(ptr.*)) == @intFromPtr(&slice[1]) - @intFromPtr(&slice[0]));
    return (a - b) / @sizeOf(@TypeOf(ptr.*));
}

pub fn MemoryPool(comptime Item: type) type {
    // this is loosely based on std.heap.MemoryPool
    // however, I think this union-based implementation fixes some issues with the std version (alignment related)
    // additionally, the paging allows us to (relatively) efficiently iterate across all items
    // though this supports running on a general allocator, it probably makes sense to use with an arena

    return struct {
        const Self = @This();

        pub const Node = union {
            item: Item,
            next: ?*Node,
        };

        const page_size = 64;

        pub const Page = struct {
            next: ?*Page,
            items: [page_size]Node,
            active: [page_size]bool,
        };

        pub const Iterator = struct {
            page: ?*Page,
            cursor: usize,

            pub fn next(iter: *Iterator) ?*Item {
                if (iter.page == null) {
                    return null;
                }

                while (iter.cursor < page_size and
                    !iter.page.?.active[iter.cursor]) : (iter.cursor += 1)
                {}

                if (iter.cursor == page_size) {
                    iter.page = iter.page.?.next;
                    iter.cursor = 0;
                    return iter.next(); // consider forcing tail-call optimization
                }

                std.debug.assert(iter.page.?.active[iter.cursor]);
                const node = &iter.page.?.items[iter.cursor];
                iter.cursor += 1;
                return @ptrCast(node);
            }
        };

        alloc: std.mem.Allocator,

        free_list: ?*Node = null,
        page_list: ?*Page = null,
        last_page: ?*Page = null, // so that we can add pages to the end instead of beginning

        len: usize = 0,
        peak: usize = 0,
        n_pages: usize = 0,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
            };
        }

        fn deinitPage(page: *Page, alloc: std.mem.Allocator) void {
            if (page.next) |next| {
                deinitPage(next, alloc);
            }
            alloc.destroy(page);
        }

        pub fn deinit(pool: *Self) void {
            if (pool.page_list) |page| {
                deinitPage(page, pool.alloc);
            }
            pool.* = undefined;
        }

        pub fn create(pool: *Self) !*Item {

            // allocated more pages if needed
            std.debug.assert(pool.len <= page_size * pool.n_pages);
            if (pool.len == page_size * pool.n_pages) {
                var page = try pool.alloc.create(Page);
                page.* = Page{
                    .next = null,
                    .items = undefined,
                    .active = [_]bool{false} ** page_size,
                };
                if (pool.n_pages == 0) {
                    pool.page_list = page;
                } else {
                    pool.last_page.?.next = page;
                }
                pool.last_page = page;
                pool.n_pages += 1;
            }

            // TODO possible optimization:
            // if we allocated a page, we know pool.free_list is empty

            const node = if (pool.free_list) |item| blk: {
                pool.free_list = item.next;

                var page = pool.page_list.?;
                var slot: usize = blk2: {
                    while (true) {
                        const slot = getIndex(item, &page.items);
                        if (slot != null and slot.? < page_size) {
                            break :blk2 slot.?;
                        }
                        page = page.next.?; // is never null; items in free_list are in a page by definition
                    }
                };
                page.active[slot] = true;

                break :blk item;
            } else blk: {
                const i = pool.len - (pool.n_pages - 1) * page_size;
                pool.last_page.?.active[i] = true;
                pool.peak += 1; // we're not reusing, so peak slot-use is increased
                break :blk &pool.last_page.?.items[i];
            };
            pool.len += 1;

            node.* = Node{ .item = undefined };
            return @ptrCast(node);
        }

        /// create an item that is placed at the end of the MemoryPool
        /// useful if creating while iterating, ensuring that the newly created element is iterated over
        pub fn createAtEnd(pool: *Self) !*Item {
            std.debug.assert(pool.peak <= page_size * pool.n_pages); // NOTE peak instead of len
            if (pool.peak == page_size * pool.n_pages) { // NOTE peak instead of len
                var page = try pool.alloc.create(Page);
                page.* = Page{
                    .next = null,
                    .items = undefined,
                    .active = [_]bool{false} ** page_size,
                };
                if (pool.n_pages == 0) {
                    pool.page_list = page;
                } else {
                    pool.last_page.?.next = page;
                }
                pool.last_page = page;
                pool.n_pages += 1;
            }

            const node = blk: {
                const i = pool.peak - (pool.n_pages - 1) * page_size; // NOTE use of peak instead of len
                pool.last_page.?.active[i] = true;
                pool.peak += 1; // we're not reusing, so peak slot-use is increased
                break :blk &pool.last_page.?.items[i];
            };
            pool.len += 1;

            node.* = Node{ .item = undefined };
            return @ptrCast(node);
        }

        /// return an Item to the memorypool so it can be reused
        /// if the item does not point to an active item in the pool, this function does nothing
        pub fn destroy(pool: *Self, ptr: *Item) void {
            // TODO make safe if ptr is invalid (in any way like pool is empty or handle is old, etc)

            const node: *Node = @alignCast(@ptrCast(ptr));

            // do pointer arithmetic to find what page, and what slot, the Item was in
            var page = pool.page_list.?;
            var slot: usize = blk: {
                while (true) {
                    const slot = getIndex(node, &page.items);
                    if (slot != null and slot.? < page_size) {
                        break :blk slot.?;
                    }
                    if (page.next == null) {
                        // item not found in any of the pages (NOOP)
                        return;
                    }
                    page = page.next.?;
                }
            };
            if (!page.active[slot]) {
                // item is already inactive (NOOP)
                return;
            }
            page.active[slot] = false; // all that for this...
            pool.len -= 1;

            // we keep a linked list of empty slots to be reused
            node.* = Node{ .next = pool.free_list };
            pool.free_list = node;
        }

        pub fn iterator(pool: *Self) Iterator {
            return Iterator{
                .page = pool.page_list,
                .cursor = 0,
            };
        }
    };
}

test "MemoryPool create & destroy" {
    var pool = MemoryPool(i32).init(std.testing.allocator);
    defer pool.deinit();

    const a = try pool.create();
    a.* = 123;

    const b = try pool.create();
    b.* = 234;

    const c = try pool.create();
    c.* = 345;

    try std.testing.expectEqual(@as(usize, 3), pool.len);
    try std.testing.expectEqual(@as(usize, 3), pool.peak);

    pool.destroy(b);
    pool.destroy(a);

    try std.testing.expectEqual(@as(usize, 1), pool.len);
    try std.testing.expectEqual(@as(usize, 3), pool.peak);

    try std.testing.expectEqual(a, try pool.create());
    try std.testing.expectEqual(b, try pool.create());

    // ensure we allocate enough items to test the paging
    const N = @TypeOf(pool).page_size + 1;

    for (0..N) |_| {
        _ = try pool.create();
    }

    pool.destroy(a);

    for (0..N) |_| {
        _ = try pool.create();
    }

    pool.destroy(c);

    for (0..N) |_| {
        _ = try pool.create();
    }

    // the slots should have been reused earlier on
    try std.testing.expect(c != try pool.create());
    try std.testing.expect(a != try pool.create());
}

test "MemoryPool create & destroy 2" {
    var pool = MemoryPool(i32).init(std.testing.allocator);
    defer pool.deinit();

    const a = try pool.create();
    a.* = 123;

    const b = try pool.create();
    b.* = 234;

    const c = try pool.create();
    c.* = 345;

    pool.destroy(b);

    try std.testing.expectEqual(@as(usize, 2), pool.len);
    try std.testing.expectEqual(@as(usize, 3), pool.peak);

    var i: u32 = 0;
    var iter = pool.iterator();
    while (iter.next()) |item| {
        _ = item;
        i += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), i);

    const d = try pool.create();
    d.* = 456;

    try std.testing.expectEqual(@as(usize, 3), pool.len);
    try std.testing.expectEqual(@as(usize, 3), pool.peak);

    i = 0;
    iter = pool.iterator();
    while (iter.next()) |item| {
        _ = item;
        i += 1;
    }

    try std.testing.expectEqual(@as(u32, 3), i);
}

test "MemoryPool iterator" {
    var pool = MemoryPool(i32).init(std.testing.allocator);
    defer pool.deinit();

    const N = @TypeOf(pool).page_size + 1;

    for (0..N) |_| {
        _ = try pool.create();
    }

    var i: i32 = 0;
    var iter = pool.iterator();
    while (iter.next()) |x| {
        x.* = i * i;
        i += 1;
    }

    i = 0;
    iter = pool.iterator();
    while (iter.next()) |x| {
        try std.testing.expectEqual(i * i, x.*);
        i += 1;
    }
}

const F = struct {
    n: u128,
    prev: ?*F,
};

test "MemoryPool fib" {
    var pool = MemoryPool(F).init(std.testing.allocator);
    defer pool.deinit();

    const N = 129;

    {
        const a = try pool.create();
        a.* = .{ .n = 0, .prev = null };
        const b = try pool.create();
        b.* = .{ .n = 1, .prev = a };
    }

    // create new terms of the fibonacci series by creating while iterating
    {
        var iter = pool.iterator();
        var i: usize = 2;
        while (iter.next()) |item| {
            if (i == 2) {
                i += 1;
                continue;
            }

            // if replaced with pool.create() this fails
            // (because of the destroy call at i==42 below)
            // since the iteration will stop prematurely
            // as we'd add an element to a slot we've already iterated past
            const a = try pool.createAtEnd();
            a.* = .{ .n = item.n + item.prev.?.n, .prev = item };

            i += 1;
            if (i > N) {
                break;
            }

            // we can return some old item to the pool
            // without affecting the result, since the memory slot will not be reused
            if (i == 42) {
                pool.destroy(a.prev.?.prev.?.prev.?.prev.?);
            }
        }
    }

    var last_number: u128 = 1337; // not a fibonacci number
    {
        var iter = pool.iterator();
        while (iter.next()) |item| {
            last_number = item.n;
        }
    }

    try std.testing.expectEqual(@as(u128, 251728825683549488150424261), last_number);
}

test "MemoryPool randomized" {
    // just do a bunch of operations randomly to test for unexpected memory errors or leaks
    var pool = MemoryPool(@Vector(4, f32)).init(std.testing.allocator);
    defer pool.deinit();

    var iter = pool.iterator();
    var rng = std.rand.DefaultPrng.init(2701);
    for (0..1234567) |_| {
        var olditem: ?*@Vector(4, f32) = null;
        if (pool.len == 0 or olditem == null or rng.random().float(f32) < 0.5) {
            const x = try pool.create();
            x.* = @splat(0);
            if (olditem == null or rng.random().float(f32) < 0.33) {
                olditem = x;
            }
        } else if (rng.random().float(f32) < 0.5) {
            pool.destroy(olditem.?);
        } else {
            iter = pool.iterator();
            while (iter.next()) |x| {
                x.* += @splat(1);
            }
        }
    }
}

fn typeId(comptime T: type) u32 {
    // simplified from prime31's zig-ecs https://github.com/prime31/zig-ecs/tree/master
    const prime: u32 = 16777619;
    var value: u32 = 2166136261;
    for (@typeName(T)) |c| {
        value = (value ^ @as(u32, @intCast(c))) *% prime;
    }
    return value;
}

pub const State = struct {
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator, // pools live in the arena
    prev: ?*State,

    pools: std.AutoHashMap(u32, usize),
    remap: std.AutoHashMap(usize, usize),

    /// init the k-tuple buffer of gamestates
    pub fn init(alloc: std.mem.Allocator, n_states: usize) !*State {
        var state = try alloc.create(State);
        state.* = State{
            .alloc = alloc,
            .arena = std.heap.ArenaAllocator.init(alloc),
            .prev = null,
            .pools = std.AutoHashMap(u32, usize).init(alloc), // should this too live in the arena?
            .remap = std.AutoHashMap(usize, usize).init(alloc),
        };

        for (1..n_states) |_| {
            const next = try alloc.create(State);
            next.* = State{
                .alloc = alloc,
                .arena = std.heap.ArenaAllocator.init(alloc),
                .prev = null,
                .pools = std.AutoHashMap(u32, usize).init(alloc), // should this too live in the arena?
                .remap = std.AutoHashMap(usize, usize).init(alloc),
            };
            next.prev = state;
            state = next;
        }

        return state;
    }

    pub fn deinit(state: *State) void {
        if (state.prev) |prev| {
            prev.deinit();
        }
        state.remap.deinit();
        state.pools.deinit();
        state.arena.deinit(); // effectively deinits the pools
        state.alloc.destroy(state);
    }

    fn reinit(state: *State) void {
        _ = state.arena.reset(.retain_capacity);
        state.remap.clearRetainingCapacity();
        state.pools.clearRetainingCapacity();
    }

    fn transfer(old_state: *State, new_state: *State, comptime T: type, item: *T) void {

        // we can only transfer a type that is in the old_state
        const info = @typeInfo(T);
        const id = typeId(T);
        if (!old_state.pools.contains(id)) {
            return;
        }

        // if we have already copied this item, skip it
        if (new_state.remap.contains(@intFromPtr(item))) {
            return;
        }

        // std.debug.print("transfer of type {s}\n", .{@typeName(T)});
        // std.debug.print("            item {}\n", .{item.*});

        const pool = new_state.getPool(T);
        const new = pool.create() catch unreachable;
        new.* = item.*;
        new_state.remap.put(@intFromPtr(item), @intFromPtr(new)) catch unreachable;

        // if the type points to an item in old_state.pools
        // then we should recursively transfer that too
        switch (info) {
            .Pointer => |pointer| {
                old_state.transfer(new_state, pointer.child, item.*);
            },
            .Struct => |_struct| {
                inline for (_struct.fields) |field| {
                    const fieldinfo = @typeInfo(field.type);
                    if (fieldinfo == .Pointer) {
                        old_state.transfer(
                            new_state,
                            fieldinfo.Pointer.child,
                            @field(item, field.name),
                        );
                    }
                }
            },
            else => {},
        }
    }

    /// transition to the next step in the k-tuple buffer
    /// copies all components of anchor types and
    /// any other components they reference to the new state
    /// should be called on the most recent step
    pub fn step(state: *State, anchors: anytype) *State {

        // repurpose the oldest step as the new one
        var next = state;
        while (next.prev) |prev| {
            if (prev.prev == null) {
                next.prev = null;
            }
            next = prev;
        }
        std.debug.assert(next.prev == null);
        next.reinit();
        next.prev = state;

        std.debug.assert(next != state);

        inline for (anchors) |anchor| {
            const old_pool = state.getPool(anchor);
            var iter = old_pool.iterator();
            while (iter.next()) |item| {
                transfer(state, next, anchor, item);
            }
        }

        return next;
    }

    /// reset the gamestate to the k-th state from the head
    pub fn rebase(state: *State, k: usize) *State {
        _ = state;
        _ = k;
        @compileError("rebase not yet implemented");
    }

    /// fetches the MemoryPool for type T if it exits
    /// or creates it if it doesn't
    pub fn getPool(state: *State, comptime T: type) *MemoryPool(T) {
        const id = comptime typeId(T);

        if (state.pools.get(id)) |v| {
            return @ptrFromInt(v);
        }

        // error handling?
        var pool = state.arena.allocator().create(MemoryPool(T)) catch unreachable;
        pool.* = MemoryPool(T).init(state.arena.allocator());
        state.pools.put(id, @intFromPtr(pool)) catch unreachable;
        return pool;
    }

    pub fn create(state: *State, comptime T: type) *T {
        const pool = state.getPool(T);
        return pool.create() catch unreachable;
    }

    pub fn createAtEnd(state: *State, comptime T: type) *T {
        const pool = state.getPool(T);
        return pool.createAtEnd() catch unreachable;
    }

    pub fn destroy(state: *State, comptime T: type, item: *T) void {
        const pool = state.getPool(T);
        pool.destroy(item);
    }

    pub fn iterator(state: *State, comptime T: type) MemoryPool(T).Iterator {
        const pool = state.getPool(T);
        return pool.iterator();
    }
};

const A = struct { x: u32, y: f32 };
const B = struct { a: *A, n: u32 };
const C = struct { n: u128, b: *B };

test "State create, destroy, & step" {
    var state = try State.init(std.testing.allocator, 2);
    defer state.deinit();

    {
        const a = state.create(u32);
        a.* = 123;

        const b = state.create(f32);
        b.* = 2.34;
        const c = state.create(f32);
        c.* = 3.45;

        const d = state.create(*f32);
        d.* = b;
    }

    try std.testing.expectEqual(@as(usize, 1), state.getPool(u32).len);

    state = state.step(.{ u32, *f32 });

    try std.testing.expectEqual(@as(usize, 1), state.getPool(f32).len);

    // it's also possible to create by going through the pool
    const pool_A = state.getPool(A);
    const pool_B = state.getPool(B);
    const pool_C = state.getPool(C);

    {
        const a = try pool_A.create();
        a.* = .{ .x = 2, .y = 0.5 };

        const a2 = try pool_A.create();
        a2.* = .{ .x = 33, .y = 33.3 };

        const b = try pool_B.create();
        b.* = .{ .a = a, .n = 123 };

        const c = try pool_C.create();
        c.* = .{ .n = 0, .b = b };
    }

    try std.testing.expectEqual(@as(usize, 2), state.getPool(A).len);

    state = state.step(.{C});

    try std.testing.expectEqual(@as(usize, 1), state.getPool(A).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(B).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(C).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(u32).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(f32).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(*f32).len);

    {
        const a = state.create(u32);
        a.* = 1;
        const b = state.create(u32);
        b.* = 1;

        var iter = state.iterator(u32);
        try std.testing.expectEqual(a.*, iter.next().?.*);
        try std.testing.expectEqual(b.*, iter.next().?.*);
    }

    state = state.step(.{});
}
