const std = @import("std");

pub fn SegmentType(comptime T: type) type {
    return struct {
        const Self = @This();
        const Skipfield = u8;

        const Node = extern union {
            value: T,
            next: Skipfield,
        };

        const SIZE = @as(comptime_int, @min(
            std.math.floorPowerOfTwo(usize, @as(
                usize,
                @intCast(std.math.maxInt(Skipfield) * (@sizeOf(Node) + @sizeOf(Skipfield))),
            )),
            std.mem.page_size,
        ));
        const CAPACITY = @min(
            (SIZE - 64) / (@sizeOf(Node) + @sizeOf(Skipfield)),
            std.math.maxInt(Skipfield) - 1, // leave one for NIL
        );
        const NIL = std.math.maxInt(Skipfield);

        const Header = struct {
            len: usize,
            free_segment: ?*Self,
            next: ?*Self,
            free: Skipfield,
        };

        header: Header align(SIZE),
        data: [CAPACITY]Node,
        skip: [CAPACITY + 1]Skipfield,

        comptime {
            std.debug.assert(@sizeOf(Self) <= std.mem.page_size);
            std.debug.assert(@alignOf(Self) <= std.mem.page_size);
            std.debug.assert(@sizeOf(Self) <= @alignOf(Self));
        }

        fn create(alloc: std.mem.Allocator) !*Self {
            const segment = try alloc.create(Self);
            errdefer alloc.destroy(segment);

            segment.header = .{
                .len = 0,
                .free_segment = null,
                .next = null,
                .free = NIL,
            };

            // https://plflib.org/matt_bentley_-_the_high_complexity_jump-counting_pattern.pdf
            segment.skip[0] = @intCast(CAPACITY);
            for (1..CAPACITY) |i| {
                segment.skip[i] = @intCast(i + 1);
            }
            segment.skip[CAPACITY] = 0; // this field exists to removes a branch during iteration

            return segment;
        }

        fn destroy(segment: *Self, alloc: std.mem.Allocator) void {
            alloc.destroy(segment);
        }

        fn insertFree(segment: *Self) *T {
            std.debug.assert(segment.header.free != NIL);
            const result = &segment.data[segment.header.free].value;
            segment.setUnskip(segment.header.free);
            segment.header.free = segment.data[segment.header.free].next;
            segment.header.len += 1;
            return result;
        }

        fn insertEnd(segment: *Self) *T {
            std.debug.assert(segment.header.len < CAPACITY);
            const result = &segment.data[segment.header.len].value;
            segment.setUnskip(segment.header.len);
            segment.header.len += 1;
            return result;
        }

        fn erase(segment: *Self, ptr: *T) void {
            std.debug.assert(segment.header.len > 0);
            const i = segment.index(ptr);
            segment.setSkip(i);
            segment.data[i].next = segment.header.free;
            segment.header.free = @intCast(i);
            segment.header.len -= 1;
        }

        fn isFull(segment: *const Self) bool {
            std.debug.assert(segment.header.len <= CAPACITY);
            return segment.header.len == CAPACITY;
        }

        fn hasFree(segment: *const Self) bool {
            return segment.header.free != NIL;
        }

        fn index(segment: *const Self, ptr: *T) usize {
            const offset = (@intFromPtr(ptr) - @intFromPtr(&segment.data[0]));
            std.debug.assert(offset % @sizeOf(Node) == 0);
            const result = offset / @sizeOf(Node);
            std.debug.assert(result < CAPACITY);
            return result;
        }

        fn basePtr(ptr: *T) *Self {
            return @ptrFromInt(@intFromPtr(ptr) & ~(@as(usize, SIZE) - 1));
        }

        fn setSkip(segment: *Self, loc: usize) void {
            std.debug.assert(loc < CAPACITY);
            std.debug.assert(segment.skip[loc] == 0);

            const l = if (loc > 0) segment.skip[loc - 1] else 0;
            const r = segment.skip[loc + 1];

            if (l + r == 0) {
                segment.skip[loc] = 1;
            } else if (r == 0) {
                segment.skip[loc] = 1 + segment.skip[loc - 1];
                const y = loc - segment.skip[loc - 1];
                segment.skip[y] = segment.skip[loc];
            } else if (l == 0) {
                var x = segment.skip[loc + 1];
                segment.skip[loc] = x + 1;
                var j: Skipfield = 1;
                while (x > 0) {
                    segment.skip[loc + j] = j + 1;
                    j += 1;
                    x -= 1;
                }
            } else {
                var x = 1 + segment.skip[loc + 1];
                var y = segment.skip[loc - 1];
                segment.skip[loc - y] += x;
                y += 1;
                var j = loc;
                while (x > 0) {
                    segment.skip[j] = y;
                    j += 1;
                    y += 1;
                    x -= 1;
                }
            }
        }

        fn setUnskip(segment: *Self, loc: usize) void {
            std.debug.assert(loc < CAPACITY);
            std.debug.assert(segment.skip[loc] != 0);

            const l = if (loc > 0) segment.skip[loc - 1] else 0;
            const r = segment.skip[loc + 1];

            if (l + r == 0) {
                std.debug.assert(segment.skip[loc] == 1);
                segment.skip[loc] = 0;
            } else if (r == 0) {
                const x = segment.skip[loc] - 1;
                segment.skip[loc - x] = x;
                segment.skip[loc] = 0;
            } else if (l == 0) {
                var x = segment.skip[loc] - 1;
                segment.skip[loc] = 0;
                segment.skip[loc + 1] = x;
                x -= 1;
                var j: Skipfield = 2;
                while (x > 0) {
                    segment.skip[loc + j] = j;
                    j += 1;
                    x -= 1;
                }
            } else {
                // phase 1
                const y = segment.skip[loc];
                const z = loc - (y - 1);
                var x = segment.skip[z] - y;
                segment.skip[loc + 1] = x;
                // phase 2
                segment.skip[z] = y - 1;
                x -= 1;
                segment.skip[loc] = 0;
                // phase 3
                var j: Skipfield = 2;
                while (x > 0) {
                    segment.skip[loc + j] = j;
                    j += 1;
                    x -= 1;
                }
            }
        }
    };
}

pub fn Colony(comptime T: type) type {
    return struct {
        const Self = @This();
        const Segment = SegmentType(T);

        alloc: std.mem.Allocator,
        head: ?*Segment,
        free_segment: ?*Segment,
        len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .head = null,
                .free_segment = null,
                .len = 0,
            };
        }

        pub fn deinit(colony: *Self) void {
            var walk = colony.head;
            while (walk) |segment| {
                walk = segment.header.next;
                segment.destroy(colony.alloc);
            }
            colony.* = undefined;
        }

        pub fn insert(colony: *Self) !*T {
            if (colony.free_segment) |segment| {
                const result = segment.insertFree();
                colony.len += 1;
                if (!segment.hasFree()) {
                    // we used the last free slot in this segment, remove from free list
                    colony.free_segment = segment.header.free_segment;
                    segment.header.free_segment = null;
                }
                return result;
            }
            if (colony.head == null) {
                colony.head = try Segment.create(colony.alloc);
            } else if (colony.head.?.isFull()) {
                const new = try Segment.create(colony.alloc);
                new.header.next = colony.head;
                colony.head = new;
            }
            const result = colony.head.?.insertEnd();
            colony.len += 1;
            return result;
        }

        /// undefined behaviour if ptr is not in this colony or upon double free
        pub fn erase(colony: *Self, ptr: *T) void {
            const segment = Segment.basePtr(ptr);
            const has_free = segment.hasFree();
            segment.erase(ptr);
            if (!has_free) {
                // we didn't previously have any free segments, so put on free list
                segment.header.free_segment = colony.free_segment;
                colony.free_segment = segment;
            }
            colony.len -= 1;
        }

        const Iterator = struct {
            segment: ?*Segment,
            cursor: usize,

            pub fn next(it: *Iterator) ?*T {
                const segment = it.segment orelse return null;
                std.debug.assert(it.cursor <= segment.data.len);
                if (it.cursor == segment.data.len) {
                    it.segment = segment.header.next;
                    if (it.segment != null) it.cursor = it.segment.?.skip[0];
                    return it.next();
                }
                const result = &segment.data[it.cursor].value;
                it.cursor += 1;
                it.cursor += it.segment.?.skip[it.cursor];
                return result;
            }
        };

        pub fn iterator(colony: *Self) Iterator {
            return .{
                .segment = colony.head,
                .cursor = if (colony.head != null) colony.head.?.skip[0] else undefined,
            };
        }
    };
}

test "scratch" {
    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    var colony = Colony(u32).init(std.testing.allocator);
    defer colony.deinit();

    var contents = std.ArrayList(*u32).init(std.testing.allocator);
    defer contents.deinit();
    var to_delete = std.ArrayList(*u32).init(std.testing.allocator);
    defer to_delete.deinit();

    const N = 10000;
    var acc_reference: usize = 0;

    for (0..N) |i| {
        const p = try colony.insert();
        p.* = @intCast(i);
        if (rand.boolean()) {
            try to_delete.append(p);
        } else {
            try contents.append(p);
            acc_reference +%= i;
        }
    }

    for (to_delete.items) |p| colony.erase(p);
    std.debug.assert(colony.len == contents.items.len);

    {
        var acc: usize = 0;
        var it = colony.iterator();
        while (it.next()) |p| {
            acc +%= p.*;
        }
        std.debug.assert(acc == acc_reference); // 1
    }

    for (0..N) |i| {
        const p = try colony.insert();
        p.* = @intCast(i);
        try contents.append(p);
        acc_reference +%= i;
    }
    std.debug.assert(colony.len == contents.items.len);

    {
        var acc: usize = 0;
        var it = colony.iterator();
        while (it.next()) |p| {
            acc +%= p.*;
        }
        std.debug.assert(acc == acc_reference); // 2
    }
}

pub fn main() !void {
    var colony = Colony(u8).init(std.heap.page_allocator);
    defer colony.deinit();
    _ = try colony.insert();
}
