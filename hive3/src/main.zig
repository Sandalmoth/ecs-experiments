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
                std.debug.assert(std.math.isPowerOfTwo(capacity));
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

            fn insert(segment: *Segment, value: T) !void {
                // free block has more than 1 element - just insert
                // free block has exactly 1 element - special case skiplist edit
                std.debug.assert(segment.len < segment.data.len);
                const ix = segment.first_free_data;
                const free_block = segment.data[ix].node;
                const free_block_len = segment.skip[ix];
                std.debug.assert(free_block_len > 0);
                // update skip list
                segment.skip[ix + 1] = segment.skip[ix] - 1;
                if (segment.skip[ix] > 2) segment.skip[ix + segment.skip[ix] - 1] -= 1;
                segment.skip[ix] = 0;
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

        pub fn insert(hive: *Self, value: T) !void {
            const segment = hive.first_free_segment orelse try hive.expand();
            std.debug.assert(segment.prev_free_segment == null);
            try segment.insert(value);
            if (segment.len == segment.data.len) {
                hive.first_free_segment = segment.next_free_segment;
                segment.next_free_segment = null;
            }
            hive.len += 1;
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
                    std.math.maxInt(Skipfield),
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

        pub fn iterator(hive: *Self) Iterator {
            return .{
                .hive = hive,
                .segment = hive.first_free_segment,
                .cursor = 0,
            };
        }

        pub const Iterator = struct {
            hive: *Self,
            segment: ?*Segment,
            cursor: usize,

            pub fn next(it: *Iterator) ?T {
                const segment = it.segment orelse return null;
                if (it.cursor >= segment.len) {
                    it.cursor = 0;
                    it.segment = segment.next_segment;
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

test "hive" {
    var hive = Hive(u32).init(std.testing.allocator);
    defer hive.deinit();

    try hive.insert(12);
    try hive.insert(23);
    try hive.insert(34);
    try hive.insert(45);
    try hive.insert(56);
    try hive.insert(67);
    try hive.insert(78);
    try hive.insert(89);

    // std.debug.print("{any}\n", .{hive.first_free_segment.?.data});
    // std.debug.print("{any}\n", .{hive.first_free_segment.?.skip});

    var it = hive.iterator();
    while (it.next()) |x| {
        std.debug.print("{}\n", .{x});
    }
}

pub fn main() !void {}
