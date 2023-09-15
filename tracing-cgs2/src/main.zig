const std = @import("std");

// reimplementation of the tracing-cgs
// - remove the remap hashmap and instead store new locations in an array (performance)
// - remove the free list in the memorypool (redundant for my predicted use case)
// - require static knowledge of all types that could be stored in state
// - assume that memory allocation never fails

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
    // not a general purpose memory pool, as
    // - it just leaks elements
    // - it has a completely unneccessary (for normal use) remap array

    return struct {
        const Self = @This();

        const page_size = 64;
        const Page = struct {
            next: ?*Page,
            items: [page_size]Item,
            active: [page_size]bool,
            remap: [page_size]?*Item,
        };

        const Slot = struct {
            page: ?*Page,
            slot: usize,
        };

        pub const Iterator = struct {
            page: ?*Page,
            cursor: usize,
            count: usize, // position of last element counting from page 0, item 0

            pub fn next(iter: *Iterator) ?*Item {
                if (iter.page == null or iter.count == 0) {
                    return null;
                }

                while (iter.count > 0 and iter.cursor < page_size and
                    !iter.page.?.active[iter.cursor])
                {
                    iter.cursor += 1;
                    iter.count -= 1;
                }

                // kinda awkward to test this twice?
                // but should branch predict well I guess?
                if (iter.count == 0) {
                    return null;
                }

                if (iter.cursor == page_size) {
                    iter.page = iter.page.?.next;
                    iter.cursor = 0;
                    return @call(.always_tail, Iterator.next, .{iter});
                }

                std.debug.assert(iter.page.?.active[iter.cursor]);
                const node = &iter.page.?.items[iter.cursor];
                iter.cursor += 1;
                iter.count -= 1;
                return @ptrCast(node);
            }
        };

        alloc: std.mem.Allocator,

        page_list: ?*Page = null,
        last_page: ?*Page = null,

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
            std.debug.assert(pool.peak <= page_size * pool.n_pages);
            if (pool.peak == page_size * pool.n_pages) {
                var page = try pool.alloc.create(Page);
                page.* = Page{
                    .next = null,
                    .items = undefined,
                    .active = [_]bool{false} ** page_size,
                    .remap = [_]?*Item{null} ** page_size,
                };
                if (pool.n_pages == 0) {
                    pool.page_list = page;
                } else {
                    pool.last_page.?.next = page;
                }
                pool.last_page = page;
                pool.n_pages += 1;
            }

            const item = blk: {
                const i = pool.peak - (pool.n_pages - 1) * page_size;
                pool.last_page.?.active[i] = true;
                break :blk &pool.last_page.?.items[i];
            };
            pool.len += 1;
            pool.peak += 1;

            return @ptrCast(item);
        }

        pub fn slot(pool: *Self, ptr: *Item) Slot {
            if (pool.page_list == null) {
                return Slot{ .page = null, .slot = 0 };
            }

            var page = pool.page_list.?;
            while (true) {
                const s = getIndex(ptr, &page.items);
                if (s != null and s.? < page_size) {
                    return Slot{ .page = page, .slot = s.? };
                }
                if (page.next == null) {
                    // item not found in any of the pages (NOOP)
                    return Slot{ .page = null, .slot = 0 };
                }
                page = page.next.?;
            }
        }

        /// return an Item to the memorypool so it's skipped in future iteration
        /// if the item does not point to an active item in the pool, this function does nothing
        pub fn destroy(pool: *Self, ptr: *Item) void {
            if (pool.page_list == null) {
                // pool is empty (NOOP)
                return;
            }

            // do pointer arithmetic to find what page, and what slot, the Item was in
            var page = pool.page_list.?;
            var s: usize = blk: {
                while (true) {
                    const s = getIndex(ptr, &page.items);
                    if (s != null and s.? < page_size) {
                        break :blk s.?;
                    }
                    if (page.next == null) {
                        // item not found in any of the pages (NOOP)
                        return;
                    }
                    page = page.next.?;
                }
            };
            if (!page.active[s]) {
                // item is already inactive (NOOP)
                return;
            }
            page.active[s] = false; // all that for this...
            pool.len -= 1;
            ptr.* = undefined; // help prevent use after "free" bugs
        }

        /// iterator for all the items currently in the pool
        pub fn iterCurrent(pool: *Self) Iterator {
            return Iterator{
                .page = pool.page_list,
                .cursor = 0,
                .count = pool.peak, // position of the last element
            };
        }

        /// iterator that will iterate over items added while iterating
        pub fn iterAll(pool: *Self) Iterator {
            return Iterator{
                .page = pool.page_list,
                .cursor = 0,
                .count = std.math.maxInt(usize), // effectively forever, we can't have that many items
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

    pool.destroy(a);

    try std.testing.expectEqual(@as(usize, 2), pool.len);
    try std.testing.expectEqual(@as(usize, 3), pool.peak);

    // ensure we allocate enough items to test the paging
    const N = @TypeOf(pool).page_size + 1;

    for (0..N) |_| {
        _ = try pool.create();
    }

    pool.destroy(b);

    for (0..N) |_| {
        _ = try pool.create();
    }

    pool.destroy(c);

    for (0..N) |_| {
        _ = try pool.create();
    }

    try std.testing.expectEqual(@as(usize, 3 * N), pool.len);
    try std.testing.expectEqual(@as(usize, 3 * N + 3), pool.peak);
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
    var iter = pool.iterAll();
    while (iter.next()) |item| {
        _ = item;
        i += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), i);

    const d = try pool.create();
    d.* = 456;

    try std.testing.expectEqual(@as(usize, 3), pool.len);
    try std.testing.expectEqual(@as(usize, 4), pool.peak);

    i = 0;
    iter = pool.iterAll();
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
    var iter = pool.iterAll();
    while (iter.next()) |x| {
        x.* = i * i;
        i += 1;
    }

    i = 0;
    iter = pool.iterAll();
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
        var iter = pool.iterAll();
        var i: usize = 2;
        while (iter.next()) |item| {
            if (i == 2) {
                i += 1;
                continue;
            }

            const a = try pool.create();
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
        var iter = pool.iterAll();
        while (iter.next()) |item| {
            last_number = item.n;
        }
    }

    try std.testing.expectEqual(@as(u128, 251728825683549488150424261), last_number);
}

test "MemoryPool iterCurrent" {
    var pool = MemoryPool(u32).init(std.testing.allocator);
    defer pool.deinit();

    const a = try pool.create();
    a.* = 0;
    const b = try pool.create();
    b.* = 1;
    const c = try pool.create();
    c.* = 2;

    var i: u32 = 0;
    // itercurrent doesn't go past the last element when the iterator is initialized
    // so in this case, only iterates until c, even if a and c are deleted and more elements added
    var iter = pool.iterCurrent();
    while (iter.next()) |_| {
        if (i == 1) {
            pool.destroy(a);
            pool.destroy(c);
        }
        i += 1;
        _ = try pool.create();
    }

    try std.testing.expectEqual(@as(u32, 2), i);
    try std.testing.expectEqual(@as(usize, 3), pool.len);
    try std.testing.expectEqual(@as(usize, 5), pool.peak);
}

test "MemoryPool randomized" {
    // just do a bunch of operations randomly to test for unexpected memory errors or leaks
    var pool = MemoryPool(@Vector(4, f32)).init(std.testing.allocator);
    defer pool.deinit();

    var iter = pool.iterAll();
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
            iter = pool.iterAll();
            while (iter.next()) |x| {
                x.* += @splat(1);
            }
        }
    }
}

pub fn State(comptime T: type) type {
    const TFields = std.meta.fields(T);
    const Component = std.meta.FieldEnum(T);
    const n_pools = std.meta.fields(Component).len;

    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,
        arena: std.heap.ArenaAllocator, // pools live in the arena
        prev: ?*Self,

        pools: [n_pools]usize, // type-erased pointers to MemoryPools

        fn initPools(state: *Self) void {
            inline for (0..n_pools) |i| {
                const c: Component = @enumFromInt(i);
                const PT = PoolType(c);
                const pool = state.arena.allocator().create(PT) catch @panic("out of memory");
                pool.* = PT.init(state.arena.allocator());
                state.pools[i] = @intFromPtr(pool);
            }
        }

        pub fn init(alloc: std.mem.Allocator, n_states: usize) *Self {
            var state = alloc.create(Self) catch @panic("out of memory");
            state.* = Self{
                .alloc = alloc,
                .arena = std.heap.ArenaAllocator.init(alloc),
                .prev = null,
                .pools = [_]usize{0} ** n_pools,
            };
            state.initPools();

            for (1..n_states) |_| {
                const next = alloc.create(Self) catch @panic("out of memory");
                next.* = Self{
                    .alloc = alloc,
                    .arena = std.heap.ArenaAllocator.init(alloc),
                    .prev = null,
                    .pools = [_]usize{0} ** n_pools,
                };
                next.initPools();
                next.prev = state;
                state = next;
            }

            return state;
        }

        pub fn deinit(state: *Self) void {
            if (state.prev) |prev| {
                prev.deinit();
            }
            state.arena.deinit(); // effectively deinits the pools
            state.alloc.destroy(state);
        }

        fn reinit(state: *Self) void {
            _ = state.arena.reset(.retain_capacity);
            state.initPools();
        }

        fn transfer(old_state: *Self, new_state: *Self, comptime c: Component, item: ?*FieldType(c)) ?*FieldType(c) {
            if (item == null) {
                return null;
            }

            const old_pool = old_state.getPool(c);
            const slot = old_pool.slot(item.?);

            if (slot.page) |page| {
                if (page.remap[slot.slot]) |remap| {
                    // we have already copied this, return a reference
                    return remap;
                } else if (!page.active[slot.slot]) {
                    // this element has been destroyed and the reference is invalid
                    return null;
                }
            }

            const new_pool = new_state.getPool(c);
            const new = new_pool.create() catch @panic("out of memory");
            new.* = item.?.*;

            // record that this item has been copied, and where the new one is
            slot.page.?.remap[slot.slot] = new;

            const info = @typeInfo(FieldType(c));
            switch (info) {
                .Optional => |optional| {
                    const optinfo = @typeInfo(optional.child);
                    if (optinfo == .Pointer) {
                        // finding that I needed this comptime was really hard...
                        if (comptime typeEnum(optinfo.Pointer.child)) |child| {
                            new.* = transfer(old_state, new_state, child, item.?.*);
                        }
                    }
                },
                .Struct => |_struct| {
                    inline for (_struct.fields) |field| {
                        const fieldinfo = @typeInfo(field.type);
                        if (fieldinfo == .Optional) {
                            const optinfo = @typeInfo(fieldinfo.Optional.child);
                            if (optinfo == .Pointer) {
                                if (comptime typeEnum(optinfo.Pointer.child)) |child| {
                                    @field(new, field.name) = old_state.transfer(
                                        new_state,
                                        child,
                                        @field(item.?, field.name),
                                    );
                                }
                            }
                        }
                    }
                },
                else => {},
            }

            return new;
        }

        pub fn step(state: *Self, anchors: anytype) *Self {

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
                var iter = old_pool.iterAll();
                while (iter.next()) |item| {
                    _ = state.transfer(next, anchor, item);
                }
            }

            return next;
        }

        // after a step, returns the location of an item in the new state
        pub fn update(state: *Self, ptr: anytype) ?@TypeOf(ptr) {
            std.debug.assert(state.prev != null);
            const c = comptime typeEnum(@typeInfo(@TypeOf(ptr)).Pointer.child) orelse return null;
            const old_pool = state.prev.?.getPool(c);
            const slot = old_pool.slot(ptr);

            if (slot.page) |page| {
                if (page.remap[slot.slot]) |remap| {
                    return remap;
                } else if (!page.active[slot.slot]) {
                    return null;
                }
            }

            return null;
        }

        pub fn getPool(state: *Self, comptime c: Component) *PoolType(c) {
            const i: usize = @intFromEnum(c);
            return @ptrFromInt(state.pools[i]);
        }

        pub fn create(state: *Self, comptime c: Component) *FieldType(c) {
            const pool = state.getPool(c);
            return pool.create() catch @panic("out of memory");
        }

        pub fn destroy(state: *Self, item: anytype) void {
            const c = comptime typeEnum(@typeInfo(@TypeOf(item)).Pointer.child) orelse return null;
            const pool = state.getPool(c);
            pool.destroy(item);
        }

        pub fn iterAll(state: *Self, comptime c: Component) PoolType(c).Iterator {
            const pool = state.getPool(c);
            return pool.iterAll();
        }

        pub fn iterCurrent(state: *Self, comptime c: Component) PoolType(c).Iterator {
            const pool = state.getPool(c);
            return pool.iterCurrent();
        }

        fn FieldType(comptime c: Component) type {
            return TFields[@intFromEnum(c)].type;
        }

        fn PoolType(comptime c: Component) type {
            return MemoryPool(TFields[@intFromEnum(c)].type);
        }

        fn typeEnum(comptime U: type) ?Component {
            inline for (TFields, 0..) |field, i| {
                if (field.type == U) {
                    return @enumFromInt(i);
                }
            }
            return null;
        }
    };
}

// NOTE all references to types in the state must be nullable (?*___)
// since if we destroyed what it references too, the state update returns null
const A = struct { x: u32, y: f32 };
const B = struct { a: ?*A, n: u32 };
const C = struct { n: u128, b: ?*B };
const TT1 = struct {
    uint: u32,
    float: f32,
    float_ptr: ?*f32,
    a: A,
    b: B,
    c: C,
};

test "State create & step" {
    var state = State(TT1).init(std.testing.allocator, 2);
    defer state.deinit();

    {
        const a = state.create(.uint);
        a.* = 123;

        const b = state.create(.float);
        b.* = 2.34;
        const c = state.create(.float);
        c.* = 3.45;

        const d = state.create(.float_ptr);
        d.* = b;
    }

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.uint).len);
    try std.testing.expectEqual(@as(usize, 2), state.getPool(.float).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(.float_ptr).len);

    state = state.step(.{ .uint, .float_ptr });

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.uint).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(.float).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(.float_ptr).len);

    // it's also possible to create by going through the pool
    const pool_A = state.getPool(.a);
    const pool_B = state.getPool(.b);
    const pool_C = state.getPool(.c);

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

    try std.testing.expectEqual(@as(usize, 2), state.getPool(.a).len);

    state = state.step(.{.c});

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.a).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(.b).len);
    try std.testing.expectEqual(@as(usize, 1), state.getPool(.c).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(.uint).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(.float).len);
    try std.testing.expectEqual(@as(usize, 0), state.getPool(.float_ptr).len);

    {
        const a = state.create(.uint);
        a.* = 1;
        const b = state.create(.uint);
        b.* = 1;

        var iter = state.iterAll(.uint);
        try std.testing.expectEqual(a.*, iter.next().?.*);
        try std.testing.expectEqual(b.*, iter.next().?.*);
    }

    state = state.step(.{});
}

const TT2 = struct {
    uint: u32,
    uint_ptr: ?*u32,
};

test "State multiple references" {
    // multiple references to an item should not result in it being duplicated
    // also test repeated use of long state chains
    var state = State(TT2).init(std.testing.allocator, 16);
    defer state.deinit();

    var a = state.create(.uint);
    a.* = 123;

    {
        const b = state.create(.uint_ptr);
        b.* = a;

        const c = state.create(.uint_ptr);
        c.* = a;
    }

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.uint).len);
    try std.testing.expectEqual(@as(usize, 2), state.getPool(.uint_ptr).len);

    state = state.step(.{.uint_ptr});
    a = state.update(a).?;

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.uint).len);
    try std.testing.expectEqual(@as(usize, 2), state.getPool(.uint_ptr).len);

    for (3..234) |i| {
        const d = state.create(.uint_ptr);
        d.* = a;
        try std.testing.expectEqual(@as(usize, 1), state.getPool(.uint).len);
        try std.testing.expectEqual(@as(usize, i), state.getPool(.uint_ptr).len);

        state = state.step(.{.uint_ptr});
        a = state.update(a).?;
    }
}

const D = struct { n: usize, parent: ?*D };
const TT3 = struct {
    d: D,
};

test "State multiple references 2" {
    var state = State(TT3).init(std.testing.allocator, 2);
    defer state.deinit();

    var root = state.create(.d);
    root.* = .{ .n = 0, .parent = null };

    { // tree
        var a = state.create(.d);
        a.* = .{ .n = 0, .parent = root };
        var b = state.create(.d);
        b.* = .{ .n = 0, .parent = root };

        var c = state.create(.d);
        c.* = .{ .n = 0, .parent = a };
        var d = state.create(.d);
        d.* = .{ .n = 0, .parent = a };

        var e = state.create(.d);
        e.* = .{ .n = 0, .parent = b };
        var f = state.create(.d);
        f.* = .{ .n = 0, .parent = b };
    }

    { // cycle
        var c0 = state.create(.d);
        var c1 = state.create(.d);
        var c2 = state.create(.d);
        c0.* = .{ .n = 0, .parent = c2 };
        c1.* = .{ .n = 0, .parent = c0 };
        c2.* = .{ .n = 0, .parent = c1 };
    }

    try std.testing.expectEqual(@as(usize, 10), state.getPool(.d).len);

    for (0..32) |_| {
        state = state.step(.{.d});
        try std.testing.expectEqual(@as(usize, 10), state.getPool(.d).len);
    }
}

const TT4 = struct {
    int: i32,
    int_ptr: ?*i32,
};

test "State destroy & reference invalidation" {
    var state = State(TT4).init(std.testing.allocator, 2);
    defer state.deinit();

    const a = state.create(.int);
    a.* = 123;
    const b = state.create(.int);
    b.* = 234;
    const c = state.create(.int);
    c.* = 345;

    const pa = state.create(.int_ptr);
    pa.* = a;
    const pc = state.create(.int_ptr);
    pc.* = c;

    state.destroy(a);

    try std.testing.expectEqual(@as(usize, 2), state.getPool(.int).len);
    try std.testing.expectEqual(@as(usize, 2), state.getPool(.int_ptr).len);

    var iter = state.iterAll(.int_ptr);
    try std.testing.expect(iter.next().?.* != null);
    try std.testing.expect(iter.next().?.* != null);
    try std.testing.expect(iter.next() == null);

    state = state.step(.{.int_ptr});

    // since we destroyed a, after the step the reference to it will be invalid after step
    // (but only after step)

    try std.testing.expectEqual(@as(usize, 1), state.getPool(.int).len);
    try std.testing.expectEqual(@as(usize, 2), state.getPool(.int_ptr).len);

    iter = state.iterAll(.int_ptr);
    try std.testing.expect(iter.next().?.* == null);
    try std.testing.expect(iter.next().?.* != null);
    try std.testing.expect(iter.next() == null);
}
