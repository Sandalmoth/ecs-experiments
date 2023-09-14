const std = @import("std");

// reimplementation of the tracing-cgs
// - remove the remap hashmap and instead store new locations in an array (performance)
// - remove the free list in the memorypool (redundant for my predicted use case)

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
    // not a general purpose memory pool, as it just leaks elements

    return struct {
        const Self = @This();

        const page_size = 64;
        const Page = struct {
            next: ?*Page,
            items: [page_size]Item,
            active: [page_size]bool,
            remap: [page_size]?*Item,
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

        /// return an Item to the memorypool so it's skipped in future iteration
        /// if the item does not point to an active item in the pool, this function does nothing
        pub fn destroy(pool: *Self, ptr: *Item) void {
            if (pool.page_list == null) {
                // pool is empty (NOOP)
                return;
            }

            // do pointer arithmetic to find what page, and what slot, the Item was in
            var page = pool.page_list.?;
            var slot: usize = blk: {
                while (true) {
                    const slot = getIndex(ptr, &page.items);
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

test "IterCurrent" {
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
