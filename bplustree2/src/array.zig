const std = @import("std");

const LINEAR_VS_BINARY_CUTOFF = 0; // 0 is most performant in practic, but worse in micro?

pub fn FixedArray(comptime SIZE: comptime_int, comptime T: type) type {
    std.debug.assert(SIZE > 0);

    return struct {
        const Array = @This();

        data: [SIZE]T align(64) = undefined,
        len: u32 = 0,

        pub inline fn at(a: Array, i: u32) T {
            std.debug.assert(i < a.len);

            return a.data[i];
        }

        pub inline fn ptr(a: *Array, i: u32) *T {
            std.debug.assert(i < a.len);

            return &a.data[i];
        }

        pub inline fn set(a: *Array, i: u32, x: T) void {
            std.debug.assert(i < a.len);

            a.data[i] = x;
        }

        pub inline fn front(a: Array) T {
            std.debug.assert(a.len > 0);

            return a.data[0];
        }

        pub inline fn back(a: Array) T {
            std.debug.assert(a.len > 0);

            return a.data[a.len - 1];
        }

        pub fn pushFront(a: *Array, x: T) void {
            std.debug.assert(a.len < SIZE);

            std.mem.copyBackwards(T, a.data[1..], a.data[0..a.len]);
            a.data[0] = x;
            a.len += 1;
        }

        pub fn pushBack(a: *Array, x: T) void {
            std.debug.assert(a.len < SIZE);

            a.data[a.len] = x;
            a.len += 1;
        }

        pub fn popFront(a: *Array) T {
            std.debug.assert(a.len > 0);

            const x = a.front();
            std.mem.copyForwards(T, a.data[0..], a.data[1..a.len]);
            a.len -= 1;
            return x;
        }

        pub fn popBack(a: *Array) T {
            std.debug.assert(a.len > 0);

            const x = a.back();
            a.len -= 1;
            return x;
        }

        pub fn insert(a: *Array, i: u32, x: T) void {
            std.debug.assert(a.len < SIZE);

            std.mem.copyBackwards(T, a.data[i + 1 ..], a.data[i..a.len]);
            a.data[i] = x;
            a.len += 1;
        }

        pub fn remove(a: *Array, i: u32) T {
            std.debug.assert(i < a.len);

            const x = a.data[i];
            std.mem.copyForwards(T, a.data[i..], a.data[i + 1 .. a.len]);
            a.len -= 1;
            return x;
        }

        /// assumes the deque is sorted
        pub fn lowerBound(a: *Array, x: T) u32 {
            if (SIZE <= LINEAR_VS_BINARY_CUTOFF) {
                //  linear counting
                var count: u32 = 0;
                for (0..a.len) |i| {
                    count += if (a.at(@intCast(i)) < x) 1 else 0;
                }
                return count;
            } else {
                // binary search
                var left: u32 = 0;
                var right: u32 = a.len;
                while (left < right) {
                    const mid = left + (right - left) / 2;
                    if (a.at(mid) >= x) {
                        right = mid;
                    } else {
                        left = mid + 1;
                    }
                }
                return left;
            }
        }

        /// assumes the deque is sorted
        pub fn upperBound(a: *Array, x: T) u32 {
            if (SIZE <= LINEAR_VS_BINARY_CUTOFF) {
                // counting
                var count: u32 = 0;
                for (0..a.len) |i| {
                    count += if (a.at(@intCast(i)) <= x) 1 else 0;
                }
                return count;
            } else {
                // binary search
                var left: u32 = 0;
                var right: u32 = a.len;
                while (left < right) {
                    const mid = left + (right - left) / 2;
                    if (a.at(mid) > x) {
                        right = mid;
                    } else {
                        left = mid + 1;
                    }
                }
                return left;
            }
        }

        /// inserts in such a way that the deque remains sorted
        pub inline fn insertSorted(a: *Array, x: T) void {
            a.insert(a.lowerBound(x), x);
        }

        /// checks that deque is sorted with no duplicates
        pub fn isSorted(a: Array) bool {
            if (a.len < 2) {
                return true;
            }

            var x = a.front();
            for (1..a.len) |i| {
                if (x >= a.at(@intCast(i))) {
                    return false;
                }
                x = a.at(@intCast(i));
            }
            return true;
        }

        pub fn format(
            a: Array,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) std.os.WriteError!void {
            try writer.print("[", .{});
            if (a.len > 0) {
                for (0..a.len - 1) |i| {
                    try writer.print("{} ", .{a.at(@intCast(i))});
                }
                try writer.print("{}]", .{a.at(a.len - 1)});
            } else {
                try writer.print("]", .{});
            }
        }
    };
}

test "sorted" {
    var a = FixedArray(8, u32){};

    a.insertSorted(4);
    a.insertSorted(6);
    a.insertSorted(2);
    a.insertSorted(10);
    a.insertSorted(8);

    var i: usize = 1;
    while (i < a.len) : (i += 1) {
        try std.testing.expect(a.at(@intCast(i - 1)) < a.at(@intCast(i)));
    }

    a.insertSorted(4);
    a.insertSorted(8);

    // std.debug.print("\n{}\n", .{a});

    try std.testing.expectEqual(1, a.lowerBound(4));
    try std.testing.expectEqual(3, a.upperBound(4));

    try std.testing.expectEqual(3, a.lowerBound(6));
    try std.testing.expectEqual(4, a.upperBound(6));

    try std.testing.expectEqual(4, a.lowerBound(8));
    try std.testing.expectEqual(6, a.upperBound(8));
}

test "insert erase" {
    var a = FixedArray(8, u32){};

    a.pushFront(12);
    a.pushFront(10);
    a.pushFront(8);
    a.pushFront(6);
    a.pushFront(4);
    a.pushFront(2);

    // std.debug.print("\n", .{});
    // std.debug.print("{} {any} {} {}\n", .{ a, a.data, a.len, a.start });

    try std.testing.expectEqual(2, a.remove(0));
    a.insert(0, 2);
    try std.testing.expectEqual(4, a.remove(1));
    a.insert(1, 4);
    try std.testing.expectEqual(6, a.remove(2));
    a.insert(2, 6);
    try std.testing.expectEqual(8, a.remove(3));
    a.insert(3, 8);
    try std.testing.expectEqual(10, a.remove(4));
    a.insert(4, 10);
    try std.testing.expectEqual(12, a.remove(5));
    a.insert(5, 12);

    // std.debug.print("{} {any} {} {}\n", .{ a, a.data, a.len, a.start });
}

test "bounds" {
    var a = FixedArray(8, u32){};

    a.pushFront(12);
    a.pushFront(10);
    a.pushFront(8);
    a.pushFront(6);
    a.pushFront(4);
    a.pushFront(2);

    try std.testing.expectEqual(0, a.lowerBound(1));
    try std.testing.expectEqual(0, a.lowerBound(2));
    try std.testing.expectEqual(1, a.lowerBound(3));
    try std.testing.expectEqual(1, a.lowerBound(4));
    try std.testing.expectEqual(2, a.lowerBound(5));
    try std.testing.expectEqual(2, a.lowerBound(6));
    try std.testing.expectEqual(3, a.lowerBound(7));
    try std.testing.expectEqual(3, a.lowerBound(8));
    try std.testing.expectEqual(4, a.lowerBound(9));
    try std.testing.expectEqual(4, a.lowerBound(10));
    try std.testing.expectEqual(5, a.lowerBound(11));
    try std.testing.expectEqual(5, a.lowerBound(12));
    try std.testing.expectEqual(6, a.lowerBound(13));

    try std.testing.expectEqual(0, a.upperBound(1));
    try std.testing.expectEqual(1, a.upperBound(2));
    try std.testing.expectEqual(1, a.upperBound(3));
    try std.testing.expectEqual(2, a.upperBound(4));
    try std.testing.expectEqual(2, a.upperBound(5));
    try std.testing.expectEqual(3, a.upperBound(6));
    try std.testing.expectEqual(3, a.upperBound(7));
    try std.testing.expectEqual(4, a.upperBound(8));
    try std.testing.expectEqual(4, a.upperBound(9));
    try std.testing.expectEqual(5, a.upperBound(10));
    try std.testing.expectEqual(5, a.upperBound(11));
    try std.testing.expectEqual(6, a.upperBound(12));
    try std.testing.expectEqual(6, a.upperBound(13));
}
