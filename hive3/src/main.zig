const builtin = @import("builtin");
const std = @import("std");

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;

pub fn Hive(comptime T: type) type {
    return struct {
        const Self = @This();
        const Skipfield = u16;

        const Node = struct {
            next: Skipfield,
            prev: Skipfield,
        };

        const Data = union { value: T, node: Node };

        const Segment = struct {
            // NOTE we could save space by storing [*] and a capacity instead of []
            data: []Data,
            skip: []Skipfield,
            len: Skipfield,
            next_segment: ?*Segment,
            prev_segment: ?*Segment,
            next_free_segment: ?*Segment,
            prev_free_segment: ?*Segment,
            first_free_data: Skipfield,

            fn create(alloc: std.mem.Allocator, capacity: Skipfield) !*Segment {
                const segment = try alloc.create(Segment);
                errdefer alloc.destroy(segment);
                segment.data = try alloc.alloc(Data, capacity);
                errdefer alloc.free(segment.data);
                segment.skip = try alloc.alloc(Skipfield, capacity + 1);
                errdefer alloc.free(segment.skip);
                segment.len = 0;
                segment.next_segment = null;
                segment.prev_segment = null;
                segment.next_free_segment = null;
                segment.prev_free_segment = null;
                segment.first_free_data = 0;

                segment.data[0] = .{ .node = .{
                    .prev = 0,
                    .next = 0,
                } };
                segment.skip[0] = capacity;
                segment.skip[capacity - 1] = capacity;
                segment.skip[capacity] = 0;

                return segment;
            }

            fn destroy(segment: *Segment, alloc: std.mem.Allocator) void {
                if (segment.prev_segment) |prev| {
                    prev.next_segment = segment.next_segment;
                }
                if (segment.next_segment) |next| {
                    next.prev_segment = segment.prev_segment;
                }
                if (segment.prev_free_segment) |prev| {
                    prev.next_free_segment = segment.next_free_segment;
                }
                if (segment.next_free_segment) |next| {
                    next.prev_free_segment = segment.prev_free_segment;
                }
                alloc.free(segment.skip);
                alloc.free(segment.data);
                alloc.destroy(segment);
            }

            fn insert(segment: *Segment, value: T) !usize {
                // std.debug.print("instert start {}\n", .{segment.first_free_data});
                // std.debug.print("instert start {any}\n", .{segment.skip});
                // free block has more than 1 element - just insert
                // free block has exactly 1 element - special case skiplist edit
                std.debug.assert(segment.len < segment.data.len);
                const ix = segment.first_free_data;
                std.debug.assert(segment.skip[ix] > 0);
                std.debug.assert(segment.skip[ix] == segment.skip[ix + segment.skip[ix] - 1]);
                const free_block = segment.data[ix].node;
                const free_block_len = segment.skip[ix];
                std.debug.assert(free_block_len > 0);
                // update skip list
                segment.skip[ix + 1] = segment.skip[ix] - 1;
                if (segment.skip[ix] > 2) segment.skip[ix + segment.skip[ix] - 1] -= 1;
                segment.skip[ix] = 0;
                std.debug.assert(segment.skip[ix + 1] < segment.data.len - ix);
                // update the erasure list
                if (free_block_len > 1) {
                    segment.data[ix + 1] = .{ .node = .{
                        .prev = ix + 1,
                        .next = if (free_block.next != ix) free_block.next else ix + 1,
                    } };
                    segment.first_free_data += 1;
                } else {
                    // we've exhausted a free block
                    std.debug.assert(segment.data[ix].node.prev == ix);
                    if (free_block.next != ix) {
                        segment.data[free_block.next].node.prev = free_block.next;
                    }
                    segment.first_free_data = segment.data[ix].node.next;
                }
                // finally insert the new value
                segment.data[ix] = .{ .value = value };
                segment.len += 1;
                return ix;
            }

            fn erase(segment: *Segment, ix: usize) void {
                // std.debug.print("erase start {}\n", .{ix});
                // std.debug.print("erase start {any}\n", .{segment.skip});
                // there are four options
                // a) both neighbours occupied, form new skipblock
                // b/c) one neighbour occupied (left/right), extend skipblock
                // d) both neighbours free, merge skipblocks to the left
                // and the way to determine the case is to look at the skipfields
                const skip_left = if (ix == 0) 0 else segment.skip[ix - 1];
                const skip_right = segment.skip[ix + 1]; // NOTE using the padding skipfield
                if (skip_left == 0 and skip_right == 0) {
                    segment.skip[ix] = 1;
                    segment.data[ix] = .{ .node = .{
                        .prev = @intCast(ix),
                        .next = if (segment.len == segment.data.len)
                            @intCast(ix)
                        else
                            segment.first_free_data,
                    } };
                    segment.len -= 1;
                    segment.first_free_data = @intCast(ix);
                } else if (skip_left > 0 and skip_right == 0) {
                    // std.debug.print(
                    //     "left merge {} {} {any}\n",
                    //     .{ ix, segment.data.len, segment.skip[ix - 1 .. ix + 2] },
                    // );
                    const free_block_len = segment.skip[ix - 1] + 1;
                    segment.skip[ix - segment.skip[ix - 1]] = free_block_len;
                    segment.skip[ix] = free_block_len;
                    segment.len -= 1;
                } else if (skip_left == 0 and skip_right > 0) {
                    // std.debug.print(
                    //     "right merge {} {} {any}\n",
                    //     .{ ix, segment.data.len, segment.skip[ix - 1 .. ix + 2] },
                    // );
                    const free_block_len = segment.skip[ix + 1] + 1;
                    segment.skip[ix + segment.skip[ix + 1]] = free_block_len;
                    segment.skip[ix] = free_block_len;
                    segment.data[ix] = .{ .node = .{
                        .prev = @intCast(ix),
                        .next = if (segment.len == segment.data.len)
                            @intCast(ix)
                        else
                            segment.first_free_data,
                    } };
                    segment.len -= 1;
                    segment.first_free_data = @intCast(ix);
                } else if (skip_left > 0 and skip_right > 0) {
                    // std.debug.print(
                    //     "center join {} {} {any}\n",
                    //     .{ ix, segment.data.len, segment.skip[ix - 1 .. ix + 2] },
                    // );
                    const free_block_len = segment.skip[ix - 1] + segment.skip[ix + 1] + 1;
                    segment.skip[ix - segment.skip[ix - 1]] = free_block_len;
                    segment.skip[ix + segment.skip[ix + 1]] = free_block_len;
                    segment.len -= 1;
                } else unreachable;
            }
        };

        alloc: std.mem.Allocator,

        len: usize,

        first_segment: ?*Segment,
        first_free_segment: ?*Segment,
        reserve_segment: ?*Segment,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .len = 0,
                .first_segment = null,
                .first_free_segment = null,
                .reserve_segment = null,
            };
        }

        pub fn deinit(hive: *Self) void {
            var walk = hive.first_segment;
            while (walk) |segment| {
                walk = segment.next_segment;
                segment.destroy(hive.alloc);
            }
            walk = hive.reserve_segment;
            while (walk) |segment| {
                std.debug.assert(segment.next_segment == null);
                walk = segment.next_segment;
                segment.destroy(hive.alloc);
            }
            hive.* = undefined;
        }

        pub fn insert(hive: *Self, value: T) !Iterator {
            const segment = hive.first_free_segment orelse try hive.expand();
            std.debug.assert(segment.prev_free_segment == null);
            const cursor = try segment.insert(value);
            if (segment.len == segment.data.len) {
                hive.first_free_segment = segment.next_free_segment;
                segment.next_free_segment = null;
            }
            hive.len += 1;
            return .{
                .hive = hive,
                .segment = segment,
                .cursor = cursor,
            };
        }

        fn expand(hive: *Self) !*Segment {
            if (hive.reserve_segment) |segment| {
                hive.reserve_segment = null;
                return segment;
            }
            const capacity = if (hive.len == 0)
                2
            else
                @min(
                    std.math.maxInt(Skipfield) - 1,
                    2 * std.math.floorPowerOfTwo(usize, hive.len),
                );
            const segment = try Segment.create(hive.alloc, capacity);
            segment.next_segment = hive.first_segment;
            if (hive.first_segment) |first| first.prev_segment = segment;
            hive.first_segment = segment;
            segment.next_free_segment = hive.first_free_segment;
            if (hive.first_free_segment) |first| first.prev_free_segment = segment;
            hive.first_free_segment = segment;
            return segment;
        }

        fn erase(hive: *Self, it: Iterator) void {
            std.debug.assert(hive == it.hive);
            const segment = it.segment orelse return;
            std.debug.assert(segment.len <= segment.data.len);
            if (segment.len == 1) {
                // segment will become empty after deletion
                // if this will be the new reserve, actually erase so it's correctly reusable
                // otherwise, we can just destroy it outright
                // (NOTE would it be more optimal to reinitialize rather than erase?)
                if (hive.reserve_segment != null and
                    hive.reserve_segment.?.data.len >= segment.data.len)
                {
                    segment.destroy(hive.alloc);
                }

                if (hive.reserve_segment) |reserve| reserve.destroy(hive.alloc);
                segment.erase(it.cursor);
                if (segment.prev_segment) |prev| {
                    prev.next_segment = segment.next_segment;
                }
                if (segment.next_segment) |next| {
                    next.prev_segment = segment.prev_segment;
                }
                if (segment.prev_free_segment) |prev| {
                    prev.next_free_segment = segment.next_free_segment;
                }
                if (segment.next_free_segment) |next| {
                    next.prev_free_segment = segment.prev_free_segment;
                }
                hive.reserve_segment = segment;
                return;
            }

            if (segment.len == segment.data.len) {
                // since the segment was full, it cannot have been in the free list, so add it
                std.debug.assert(segment.next_free_segment == null);
                std.debug.assert(segment.prev_free_segment == null);
                if (hive.first_free_segment) |first| first.prev_free_segment = segment;
                segment.next_free_segment = hive.first_free_segment;
                hive.first_free_segment = segment;
            }
            it.segment.?.erase(it.cursor);
            hive.len -= 1;
        }

        pub fn iterator(hive: *Self) Iterator {
            return .{
                .hive = hive,
                .segment = hive.first_segment,
                .cursor = if (hive.first_segment) |segment| segment.skip[0] else 0,
            };
        }

        pub const Iterator = struct {
            hive: *Self,
            segment: ?*Segment,
            cursor: usize,

            pub fn next(it: *Iterator) ?T {
                const segment = it.segment orelse return null;
                if (it.cursor >= segment.data.len) {
                    it.segment = segment.next_segment;
                    if (it.segment != null) it.cursor = it.segment.?.skip[0];
                    return it.next();
                }
                const result = segment.data[it.cursor].value;
                it.cursor += 1;
                it.cursor += segment.skip[it.cursor];
                return result;
            }
        };
    };
}

fn test_replace(_it: Hive(u32).Iterator, x: u32) !Hive(u32).Iterator {
    _it.hive.erase(_it);

    var it = _it.hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});

    const result = try _it.hive.insert(x);

    it = _it.hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});

    return result;
}

fn test_replace_2(it1: *Hive(u32).Iterator, it2: *Hive(u32).Iterator, x1: u32, x2: u32) !void {
    const hive = it1.hive;

    hive.erase(it1.*);
    hive.erase(it2.*);

    var it = hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});

    it2.* = try hive.insert(x2);
    it1.* = try hive.insert(x1);

    it = hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});
}

fn test_replace_3(
    it1: *Hive(u32).Iterator,
    it2: *Hive(u32).Iterator,
    it3: *Hive(u32).Iterator,
    x1: u32,
    x2: u32,
    x3: u32,
) !void {
    const hive = it1.hive;

    hive.erase(it1.*);
    hive.erase(it2.*);
    hive.erase(it3.*);

    var it = hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});

    it3.* = try hive.insert(x3);
    it2.* = try hive.insert(x2);
    it1.* = try hive.insert(x1);

    it = hive.iterator();
    while (it.next()) |y| {
        std.debug.print("{} ", .{y});
    }
    std.debug.print("\n", .{});
}

test "hive" {
    var hive = Hive(u32).init(std.testing.allocator);
    defer hive.deinit();

    const x0 = try hive.insert(12);
    const x1 = try hive.insert(23);
    var x2 = try hive.insert(34);
    var x3 = try hive.insert(45);
    var x4 = try hive.insert(56);
    var x5 = try hive.insert(67);
    const x6 = try hive.insert(78);
    const x7 = try hive.insert(89);

    _ = x0;
    _ = x1;
    _ = x6;
    _ = x7;

    std.debug.print("\n", .{});
    var it = hive.iterator();
    while (it.next()) |x| {
        std.debug.print("{} ", .{x});
    }
    std.debug.print("\n", .{});

    // std.debug.print("{any}\n", .{hive.first_free_segment.?.data});
    // std.debug.print("{any}\n", .{hive.first_free_segment.?.skip});

    std.debug.print("single replace\n", .{});
    x2 = try test_replace(x2, 3434);
    x3 = try test_replace(x3, 4545);
    x4 = try test_replace(x4, 5656);
    x5 = try test_replace(x5, 6767);

    std.debug.print("double replace\n", .{});
    try test_replace_2(&x2, &x3, 45, 34);
    try test_replace_2(&x3, &x2, 4545, 3434);
    try test_replace_2(&x4, &x5, 67, 56);
    try test_replace_2(&x5, &x4, 6767, 5656);
    try test_replace_2(&x2, &x5, 34, 67);
    try test_replace_2(&x5, &x2, 6767, 3434);

    std.debug.print("triple replace\n", .{});
    try test_replace_3(&x2, &x3, &x4, 54, 45, 34);
    try test_replace_3(&x2, &x4, &x3, 5454, 4545, 3434);
}

test "hive fuzz" {
    var hive = Hive(u32).init(std.testing.allocator);
    defer hive.deinit();

    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp() *% 89));
    const rand = rng.random();

    var acc: u64 = 0;
    var contents = std.ArrayList(struct {
        value: u32,
        iterator: Hive(u32).Iterator,
    }).init(std.testing.allocator);
    defer contents.deinit();

    for (0..10) |_| {
        while (hive.len < 100_000) {
            const x = rand.int(u32);
            const result = try hive.insert(x);
            try contents.append(.{
                .value = x,
                .iterator = result,
            });
            acc += x;
        }
        var it = hive.iterator();
        var total: u64 = 0;
        while (it.next()) |x| total += x;
        try std.testing.expect(total == acc);

        while (hive.len > 1000) {
            const x = rand.uintLessThan(usize, contents.items.len);
            const result = contents.swapRemove(x);
            hive.erase(result.iterator);
            acc -= result.value;
        }

        it = hive.iterator();
        total = 0;
        while (it.next()) |x| total += x;
        try std.testing.expect(total == acc);
    }
}

pub fn main() !void {}
