const std = @import("std");

pub fn FixedDeque(comptime SIZE: comptime_int, comptime T: type) type {
    std.debug.assert(SIZE > 0);
    std.debug.assert(SIZE < std.math.maxInt(u32) / 2); // avoid overflow in index maths
    std.debug.assert(std.math.isPowerOfTwo(SIZE)); // otherwise perf is bad

    return struct {
        const Deque = @This();

        data: [SIZE]T align(64) = undefined,
        len: u32 = 0,
        start: u32 = SIZE / 2,

        pub inline fn at(dq: Deque, i: u32) T {
            std.debug.assert(i < dq.len);

            return dq.data[(dq.start + i) % SIZE];
        }

        pub inline fn set(dq: *Deque, i: u32, x: T) void {
            std.debug.assert(i < dq.len);

            dq.data[(dq.start + i) % SIZE] = x;
        }

        pub inline fn front(dq: Deque) T {
            std.debug.assert(dq.len > 0);

            return dq.data[dq.start];
        }

        pub inline fn back(dq: Deque) T {
            std.debug.assert(dq.len > 0);

            return dq.data[(dq.start + dq.len - 1) % SIZE];
        }

        pub fn pushFront(dq: *Deque, x: T) void {
            std.debug.assert(dq.len < SIZE);

            dq.start = (dq.start + SIZE - 1) % SIZE;
            dq.data[dq.start] = x;
            dq.len += 1;
        }

        pub fn pushBack(dq: *Deque, x: T) void {
            std.debug.assert(dq.len < SIZE);

            const i = (dq.start + dq.len) % SIZE;
            dq.data[i] = x;
            dq.len += 1;
        }

        pub fn popFront(dq: *Deque) T {
            std.debug.assert(dq.len > 0);

            const x = dq.front();
            dq.start = (dq.start + 1) % SIZE;
            dq.len -= 1;
            return x;
        }

        pub fn popBack(dq: *Deque) T {
            std.debug.assert(dq.len > 0);

            const x = dq.back();
            dq.len -= 1;
            return x;
        }

        pub fn insert(dq: *Deque, i: u32, x: T) void {
            std.debug.assert(dq.len < SIZE);

            if (dq.len == 0 or i == 0) {
                dq.pushFront(x);
                return;
            } else if (i == dq.len) {
                dq.pushBack(x);
                return;
            }

            // var j = dq.len;
            // while (j >= i) : (j -= 1) {
            //     dq.data[(dq.start + j) % SIZE] = dq.data[(dq.start + j - 1) % SIZE];
            // }

            if (i > SIZE / 2) {
                var j = dq.len;
                while (j >= i) : (j -= 1) {
                    dq.data[(dq.start + j) % SIZE] = dq.data[(dq.start + j - 1) % SIZE];
                }
            } else {
                var j: u32 = 0;
                while (j < i) : (j += 1) {
                    dq.data[(dq.start + j + SIZE - 1) % SIZE] = dq.data[(dq.start + j) % SIZE];
                }
                dq.start = (dq.start + SIZE - 1) % SIZE;
            }

            dq.data[(dq.start + i) % SIZE] = x;
            dq.len += 1;
        }

        pub fn remove(dq: *Deque, i: u32) T {
            std.debug.assert(i < dq.len);

            if (i == 0) {
                return dq.popFront();
            } else if (i == dq.len) {
                return dq.popBack();
            }

            const x = dq.at(i);

            // var j = i;
            // while (j < dq.len - 1) : (j += 1) {
            //     dq.data[(dq.start + j) % SIZE] = dq.data[(dq.start + j + 1) % SIZE];
            // }

            if (i > SIZE / 2) {
                var j = i;
                while (j < dq.len - 1) : (j += 1) {
                    dq.data[(dq.start + j) % SIZE] = dq.data[(dq.start + j + 1) % SIZE];
                }
            } else {
                var j = i;
                while (j > 0) : (j -= 1) {
                    dq.data[(dq.start + j) % SIZE] = dq.data[(dq.start + j + SIZE - 1) % SIZE];
                }
                dq.start = (dq.start + 1) % SIZE;
            }

            dq.len -= 1;
            return x;
        }

        /// assumes the deque is sorted
        pub fn lowerBound(dq: *Deque, x: u32) T {
            var left: u32 = 0;
            var right: u32 = dq.len;
            while (left < right) {
                const mid = left + (right - left) / 2;
                if (dq.at(mid) >= x) {
                    right = mid;
                } else {
                    left = mid + 1;
                }
            }

            return left;
        }

        /// assumes the deque is sorted
        pub fn upperBound(dq: *Deque, x: u32) T {
            var left: u32 = 0;
            var right: u32 = dq.len;
            while (left < right) {
                const mid = left + (right - left) / 2;
                if (dq.at(mid) > x) {
                    right = mid;
                } else {
                    left = mid + 1;
                }
            }

            return left;
        }

        /// inserts in such a way that the deque remains sorted
        pub inline fn insertSorted(dq: *Deque, x: u32) void {
            dq.insert(dq.lowerBound(x), x);
        }

        pub fn format(
            dq: Deque,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) std.os.WriteError!void {
            try writer.print("[", .{});
            if (dq.len > 0) {
                for (0..dq.len - 1) |i| {
                    try writer.print("{} ", .{dq.at(@intCast(i))});
                }
                try writer.print("{}]", .{dq.at(dq.len - 1)});
            } else {
                try writer.print("]", .{});
            }
        }
    };
}

test "sorted" {
    var dq = FixedDeque(8, u32){};

    dq.insertSorted(4);
    dq.insertSorted(6);
    dq.insertSorted(2);
    dq.insertSorted(10);
    dq.insertSorted(8);

    var i: usize = 1;
    while (i < dq.len) : (i += 1) {
        try std.testing.expect(dq.at(@intCast(i - 1)) < dq.at(@intCast(i)));
    }

    dq.insertSorted(4);
    dq.insertSorted(8);

    // std.debug.print("\n{}\n", .{dq});

    try std.testing.expectEqual(1, dq.lowerBound(4));
    try std.testing.expectEqual(3, dq.upperBound(4));

    try std.testing.expectEqual(3, dq.lowerBound(6));
    try std.testing.expectEqual(4, dq.upperBound(6));

    try std.testing.expectEqual(4, dq.lowerBound(8));
    try std.testing.expectEqual(6, dq.upperBound(8));
}

test "insert erase" {
    var dq = FixedDeque(8, u32){};

    dq.pushFront(12);
    dq.pushFront(10);
    dq.pushFront(8);
    dq.pushFront(6);
    dq.pushFront(4);
    dq.pushFront(2);

    // std.debug.print("\n", .{});
    // std.debug.print("{} {any} {} {}\n", .{ dq, dq.data, dq.len, dq.start });

    try std.testing.expectEqual(2, dq.remove(0));
    dq.insert(0, 2);
    try std.testing.expectEqual(4, dq.remove(1));
    dq.insert(1, 4);
    try std.testing.expectEqual(6, dq.remove(2));
    dq.insert(2, 6);
    try std.testing.expectEqual(8, dq.remove(3));
    dq.insert(3, 8);
    try std.testing.expectEqual(10, dq.remove(4));
    dq.insert(4, 10);
    try std.testing.expectEqual(12, dq.remove(5));
    dq.insert(5, 12);

    // std.debug.print("{} {any} {} {}\n", .{ dq, dq.data, dq.len, dq.start });
}
