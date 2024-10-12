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
            data: []Data,
            skip: []Skipfield,
            len: usize,
            next_free_segment: ?*Segment,
            prev_free_segment: ?*Segment,
            first_free_data: Skipfield,

            fn create(alloc: std.mem.Allocator, capacity: usize) !*Segment {
                std.debug.assert(std.math.isPowerOfTwo(capacity));
                const segment = try alloc.create(Segment);
                errdefer alloc.destroy(segment);
                segment.data = try alloc.alloc(Data, capacity);
                errdefer alloc.free(segment.data);
                segment.skip = try alloc.alloc(Skipfield, capacity + 1);
                errdefer alloc.free(segment.skip);
                segment.len = 0;
                segment.next_free_segment = null;
                segment.prev_free_segment = null;
                segment.first_free_data = 0;

                segment.data[0] = .{ .node = .{
                    .prev = 0,
                    .next = 0,
                } };
                segment.skip[0] = @intCast(capacity);
                segment.skip[capacity - 1] = @intCast(capacity);
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
                std.debug.assert(segment.len < segment.data.len);
                const ix = segment.first_free_data;
                const free_block_len = segment.skip[ix];
                std.debug.assert(free_block_len > 0);
                segment.skip[ix + 1] = segment.skip[ix] - 1;
                // std.debug.print("{}\n", .{segment.skip[ix]});
                if (segment.skip[ix] > 0) segment.skip[ix + segment.skip[ix] - 1] -= 1;
                segment.skip[ix] = 0;
                segment.data[ix + 1] = .{ .node = .{
                    .prev = ix + 1,
                    .next = segment.data[ix].node.next,
                } };
                segment.data[ix] = .{ .value = value };
                segment.first_free_data += 1;
                segment.len += 1;
            }
        };

        alloc: std.mem.Allocator,

        len: usize,

        first_free_segment: ?*Segment,
        reserve_segment: ?*Segment,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .len = 0,
                .first_free_segment = null,
                .reserve_segment = null,
            };
        }

        pub fn deinit(hive: *Self) void {
            var walk = hive.first_free_segment;
            while (walk) |segment| {
                walk = segment.next_free_segment;
                segment.destroy(hive.alloc);
            }
            walk = hive.reserve_segment;
            while (walk) |segment| {
                walk = segment.next_free_segment;
                segment.destroy(hive.alloc);
            }
            hive.* = undefined;
        }

        pub fn insert(hive: *Self, value: T) !void {
            const segment = hive.first_free_segment orelse try hive.expand();
            try segment.insert(value);
            hive.len += 1;
        }

        fn expand(hive: *Self) !*Segment {
            std.debug.assert(hive.len == 0 or std.math.isPowerOfTwo(hive.len));
            if (hive.reserve_segment) |segment| {
                hive.reserve_segment = null;
                return segment;
            }
            const capacity = @max(
                16,
                @min(std.math.maxInt(Skipfield), 2 * hive.len),
            );
            const segment = try Segment.create(hive.alloc, capacity);
            segment.next_free_segment = hive.first_free_segment;
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
                if (it.cursor == segment.len) {
                    it.cursor = 0;
                    it.segment = segment.next_free_segment;
                    return it.next();
                }
                const result = segment.data[it.cursor].value;
                it.cursor += segment.skip[it.cursor] + 1;
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

    // std.debug.print("{any}\n", .{hive.first_free_segment.?.data});
    // std.debug.print("{any}\n", .{hive.first_free_segment.?.skip});

    var it = hive.iterator();
    while (it.next()) |x| {
        std.debug.print("{}\n", .{x});
    }
}

pub fn main() !void {}
