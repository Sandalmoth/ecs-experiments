const std = @import("std");

// a simplified version of https://www.plflib.org/colony.htm in zig

pub fn Colony(comptime T: type) type {
    const MIN_PAGE_SIZE = 16;
    const MAX_PAGE_SIZE = 65536;

    return struct {
        const C = @This();

        const Skipfield = struct {
            skip: u16,
        };

        const Node = struct {
            // I don't quite understand the freelist construction in the source
            // so I'm just doing a singly linked list
            end: bool, // end within this page, but check the page.next_free also
            next: u16,
        };

        const Element = union {
            node: Node,
            data: T,
        };

        const Page = struct {
            next: ?*Page = null,
            prev: ?*Page = null,
            next_free: ?*Page = null,
            next_empty: ?*Page = null,

            skip: []Skipfield,
            data: []Element,

            head: Node = .{ .end = true, .next = 0 }, // head of free list
            len: u16 = 0, // current number of stored elements

            fn create(alloc: std.mem.Allocator, _size: usize) !*Page {
                const size: usize = @min(_size, MAX_PAGE_SIZE);

                var page = try alloc.create(Page);
                errdefer alloc.destroy(page);
                page.* = .{ .skip = undefined, .data = undefined };

                page.skip = try alloc.alloc(Skipfield, size + 1);
                errdefer alloc.free(page.skip);

                page.data = try alloc.alloc(Element, size);
                errdefer alloc.free(page.data);

                // guessing here :P
                page.skip[0].skip = @intCast(size);
                page.skip[size - 1].skip = @intCast(size);
                page.skip[size].skip = 0;

                return page;
            }

            fn destroy(page: *Page, alloc: std.mem.Allocator) void {
                alloc.free(page.skip);
                alloc.free(page.data);
                alloc.destroy(page);
            }
        };

        pub const Iterator = struct {
            page: *Page,
            element: [*]Element,
            skipfield: [*]Skipfield,

            pub fn next() ?*T {
                return null;
            }
        };

        alloc: std.mem.Allocator,

        begin: Iterator,
        end: Iterator,

        next_free: ?*Page = null,
        next_empty: ?*Page = null,

        capacity: usize = 0,
        len: usize = 0,

        pub fn init(alloc: std.mem.Allocator) C {
            return .{
                .alloc = alloc,
                // these three are always initialized before first use I think
                .begin = undefined,
                .end = undefined,
            };
        }

        pub fn deinit(colony: *C) void {
            if (colony.capacity > 0) {
                // there are pages to destroy
                var page: ?*Page = colony.begin.page;
                while (page != null) {
                    const next = page.?.next;
                    page.?.destroy(colony.alloc);
                    page = next;
                }
            }

            colony.* = undefined;
        }

        /// returns a pointer to the inserted value
        pub fn insert(colony: *C, value: T) !*T {
            std.debug.print("trying to insert {}\n", .{value});

            if (colony.capacity == 0) {
                std.debug.print("first time init\n", .{});
                // first time initialization, allocate a page and retry
                const page = try Page.create(colony.alloc, MIN_PAGE_SIZE);
                colony.begin.page = page;
                colony.begin.element = page.data.ptr;
                colony.begin.skipfield = page.skip.ptr;
                colony.end.page = page;
                colony.end.element = page.data.ptr;
                colony.end.skipfield = page.skip.ptr;
                colony.capacity += page.data.len;

                return @call(.always_tail, insert, .{ colony, value });
            }

            if (colony.next_free) |free| {
                std.debug.print("inserted {} to free list\n", .{value});
                // there are empty spots in the free list, use one of those
                std.debug.assert(!free.head.end); // there must be a valid free list in the page
                const node = free.data[free.head.next];
                free.data[free.head.next] = .{ .data = value };
                free.skip[free.head.next].skip = 0;
                const result_ptr: *T = @ptrCast(&free.data[free.head.next].data);
                free.head = node.node;
                if (free.head.end) {
                    // page has no more reusable slots so remove this page from the page free list
                    colony.next_free = free.next_free;
                    free.next_free = null;
                }
                free.len += 1;
                return result_ptr;
            }

            if (colony.end.page.len < colony.end.page.data.len) {
                std.debug.print("inserted {} to end of page\n", .{value});
                // there is room at the end of this page, insert there
                colony.end.element[0] = .{ .data = value };
                colony.end.skipfield[0].skip = 0;
                const result_ptr: *T = @ptrCast(&colony.end.element[0].data);
                std.debug.print("{} {} {}\n", .{
                    colony.end.element[0],
                    colony.end.skipfield[0],
                    colony.end.page.len,
                });
                colony.end.page.len += 1;
                colony.end.element += 1;
                colony.end.skipfield += 1;
                colony.len += 1;
                return result_ptr;
            }

            if (colony.next_empty) |empty| {
                std.debug.print("reusing an old page\n", .{});
                _ = empty;
                // there is an empty page, activate it and retry
                return @call(.always_tail, insert, .{ colony, value });
            }

            // we need a new page, allocate and retry
            std.debug.print("adding another page\n", .{});
            const page = try Page.create(colony.alloc, colony.capacity);
            page.prev = colony.end.page;
            colony.end.page.next = page;

            colony.end.page = page;
            colony.end.element = page.data.ptr;
            colony.end.skipfield = page.skip.ptr;
            colony.capacity += page.data.len;

            return @call(.always_tail, insert, .{ colony, value });
        }

        /// return whether the value was successfully erased
        /// it can only fail (return false) if ptr.* is already erased, or if it is not the colony
        pub fn erase(colony: *C, ptr: *T) bool {

            // first find where in the datastructure the pointer is
            var page: ?*Page = colony.end.page;
            var index: usize = 0;
            while (page) |p| : (page = p.prev) {
                if (@intFromPtr(ptr) < @intFromPtr(p.data.ptr)) {
                    continue;
                }
                std.debug.print("{} {}\n", .{ @sizeOf(Element), @sizeOf(T) });
                index = (@intFromPtr(ptr) - @intFromPtr(p.data.ptr)) / @sizeOf(Element);
                if (index < p.data.len) {
                    break;
                }
            }

            std.debug.print("index of deletion is {}\n", .{index});
            // std.debug.print("{} {}\n", .{ page.?.skip[index], page.?.data[index].data });

            if (page == null or page.?.skip[index].skip != 0) {
                return false;
            }

            std.debug.print("index of deletion is {}\n", .{index});

            page.?.skip[index].skip = 1; // FIXME update skipfield correctly
            // add this element to the free list
            if (page.?.head.end) {
                page.?.data[index] = .{ .node = .{ .end = true, .next = 0 } };
                page.?.head = .{ .end = false, .next = @intCast(index) };
                // since this page didn't have any free nodes previously, add to free_list
                page.?.next_free = colony.next_free;
                colony.next_free = page;
            } else {
                page.?.data[index] = .{ .node = .{ .end = false, .next = page.?.head.next } };
                page.?.head = .{ .end = false, .next = @intCast(index) };
            }
            page.?.len -= 1;
            colony.len -= 1;

            return true;
        }
    };
}

test "basics" {
    var colony = Colony(i32).init(std.testing.allocator);
    defer colony.deinit();

    _ = try colony.insert(123);
    const p1 = try colony.insert(234);
    std.debug.print("{}\n", .{p1.*});
    _ = try colony.insert(345);

    for (0..100) |i| {
        _ = try colony.insert(@intCast(i));
    }

    for (colony.begin.page.data) |e| {
        std.debug.print("{} ", .{e.data});
    }
    std.debug.print("\n", .{});

    for (colony.begin.page.skip) |s| {
        std.debug.print("{} ", .{s.skip});
    }
    std.debug.print("\n", .{});

    try std.testing.expect(colony.erase(p1));
    try std.testing.expect(!colony.erase(p1));
    try std.testing.expect(!colony.erase(@ptrCast(&colony)));

    std.debug.print("{*}\n", .{p1});

    // _ = try colony.insert(456);
    try std.testing.expectEqual(p1, try colony.insert(456));

    std.debug.print("{}\n", .{p1.*});

    for (colony.begin.page.data) |e| {
        std.debug.print("{} ", .{e.data});
    }
    std.debug.print("\n", .{});
}

// skipfield_pointer_type		skipfield;
// Skipfield storage. The element and skipfield arrays are allocated contiguously,
// in a single allocation, in this implementation, hence the skipfield pointer also functions
// as a 'one-past-end' pointer for the elements array. There will always be one additional
// skipfield node allocated compared to the number of elements. This is to ensure a faster
// ++ iterator operation (fewer checks are required when this is present). The extra node is unused
// and always zero, but checked, and not having it will result in out-of-bounds memory errors.
// This is present before elements in the group struct as it is referenced constantly by the
// ++ operator, hence having it first results in a minor performance increase.

// group_pointer_type			next_group;
// Next group in the linked list of all groups. NULL if no following group.
// 2nd in struct because it is so frequently used during iteration.

// aligned_pointer_type const  elements;
// Element storage.

// group_pointer_type			previous_group;
// Previous group in the linked list of all groups. NULL if no preceding group.

// skipfield_type 				free_list_head;
// The index of the last erased element in the group. The last erased element will, in turn,
// contain the number of the index of the next erased element, and so on. If this is == maximum
// skipfield_type value then free_list is empty ie. no erasures have occurred in the group
// (or if they have, the erased locations have subsequently been reused via insert/emplace/assign).

// const skipfield_type 		capacity;
// The element capacity of this particular group - can also be calculated from
// reinterpret_cast<aligned_pointer_type>(group->skipfield) - group->elements, however this space
// is effectively free due to struct padding and the sizeof(skipfield_type), and calculating it
// once is faster in benchmarking.

// skipfield_type 				size;
// The total number of active elements in group - changes with insert and erase commands -
// used to check for empty group in erase function, as an indication to remove the group.
// Also used in combination with capacity to check if group is full, which is used in the
// next/previous/advance/distance overloads, and range-erase.

// group_pointer_type			erasures_list_next_group, erasures_list_previous_group;
// The next and previous groups in the list of groups with erasures ie. with active
// erased-element free lists. NULL if no next or previous group.

// size_type					group_number;
// Used for comparison (> < >= <= <=>) iterator operators (used by distance function and user).
