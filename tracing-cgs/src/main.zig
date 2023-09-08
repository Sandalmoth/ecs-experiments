const std = @import("std");

fn getIndex(ptr: anytype, slice: []@TypeOf(ptr.*)) ?usize {
    // based on https://github.com/FlorenceOS/Florence/blob/master/lib/util/pointers.zig
    const a = @intFromPtr(ptr);
    const b = @intFromPtr(slice.ptr);
    if (b > a) {
        return null;
    }
    return (a - b) / @sizeOf(@TypeOf(ptr.*));
}

pub fn MemoryPool(comptime Item: type) type {
    // this is loosely based on std.heap.MemoryPool
    // however, I think this union-based implementation fixes some issues with the std version
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

        alloc: std.mem.Allocator,

        free_list: ?*Node = null,
        page_list: ?*Page = null,

        len: usize = 0,
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
                    .next = pool.page_list,
                    .items = undefined,
                    .active = [_]bool{false} ** page_size,
                };
                pool.page_list = page;
                pool.n_pages += 1;
            }

            // TODO possible optimization:
            // if we allocated a page, we know pool.free_list is empty

            const node = if (pool.free_list) |item| blk: {
                pool.free_list = item.next;
                break :blk item;
            } else blk: {
                const i = pool.len - (pool.n_pages - 1) * page_size;
                pool.page_list.?.active[i] = true;
                break :blk &pool.page_list.?.items[i];
            };
            pool.len += 1;

            node.* = Node{ .item = undefined };
            return @ptrCast(node);
        }

        /// return an Item to the memorypool so it can be reused
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
                    page = page.next.?;
                }
            };
            page.active[slot] = false; // all that for this...
            pool.len -= 1;

            node.* = Node{ .next = pool.free_list };
            pool.free_list = node;
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

    pool.destroy(b);
    pool.destroy(a);

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

pub const State = struct {
    alloc: std.mem.Allocator,
    prev: ?*State,

    pub fn init(alloc: std.mem.Allocator, n_states: usize) !*State {
        _ = n_states;

        var state = try alloc.create(State);
        state.* = State{
            .alloc = alloc,
            .prev = null,
        };

        return state;
    }

    pub fn deinit(state: *State) void {
        if (state.prev) |prev| {
            prev.deinit();
        }
        state.alloc.destroy(state);
    }

    pub fn step(state: *State) *State {
        _ = state;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var state = try State.init(alloc, 2);
    defer state.deinit();
}
