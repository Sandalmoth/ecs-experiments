const std = @import("std");

// TODO benchmark implementing copyForwards/Backwards without slicing

pub fn front(comptime T: type, a: [*]T, len: u32) T {
    std.debug.assert(len > 0);
    return a[0];
}

pub fn back(comptime T: type, a: [*]T, len: u32) T {
    std.debug.assert(len > 0);
    return a[len - 1];
}

pub fn pushBack(comptime T: type, a: [*]T, len: u32, x: T) void {
    a[len] = x;
}

pub fn popBack(comptime T: type, a: [*]T, len: u32) T {
    std.debug.assert(len > 0);
    return a[len - 1];
}

pub fn pushFront(comptime T: type, a: [*]T, len: u32, x: T) void {
    std.mem.copyBackwards(T, a[1 .. len + 1], a[0..len]);
    a[0] = x;
}

pub fn popFront(comptime T: type, a: [*]T, len: u32) T {
    std.debug.assert(len > 0);
    const result = a[0];
    std.mem.copyForwards(T, a[0 .. len - 1], a[1..len]);
    return result;
}

pub fn insert(comptime T: type, a: [*]T, len: u32, i: u32, x: T) void {
    std.mem.copyBackwards(T, a[i + 1 .. len + 1], a[i..len]);
    a[i] = x;
}

pub fn remove(comptime T: type, a: [*]T, len: u32, i: u32) T {
    std.debug.assert(len > 0);
    const result = a[i];
    std.mem.copyForwards(T, a[i .. len - 1], a[i + 1 .. len]);
    return result;
}

// https://orlp.net/blog/bitwise-binary-search/
// It lower_bound(It begin, It end, const T& value, Cmp comp) {
//     size_t n = end - begin;
//     size_t b = 0;
//     for (size_t bit = std::bit_floor(n); bit != 0; bit >>= 1) {
//         size_t i = (b | bit) - 1;
//         if (i < n && comp(begin[i], value)) b |= bit;
//     }
//     return begin + b;
// }

pub fn lowerBound(comptime T: type, a: [*]T, len: u32, x: T) u32 {
    var count: u32 = 0;
    for (0..len) |i| {
        count += if (a[i] < x) 1 else 0;
    }
    return count;

    // var left: u32 = 0;
    // var right: u32 = len;
    // while (left < right) {
    //     const mid = (left +% right) / 2;
    //     if (a[mid] >= x) {
    //         right = mid;
    //     } else {
    //         left = mid +% 1;
    //     }
    // }
    // return left;

    // var b: u32 = 0;
    // var bit = std.math.floorPowerOfTwo(u32, len);
    // while (bit != 0) : (bit >>= 1) {
    //     const i: u32 = (b | bit) -% 1;
    //     if (i < len and a[i] < x) {
    //         b |= bit;
    //     }
    // }
    // return b;
}

pub fn upperBound(comptime T: type, a: [*]T, len: u32, x: T) u32 {
    var count: u32 = 0;
    for (0..len) |i| {
        count += if (a[i] <= x) 1 else 0;
    }
    return count;

    // var left: u32 = 0;
    // var right: u32 = len;
    // while (left < right) {
    //     const mid = left + (right - left) / 2;
    //     if (a[mid] > x) {
    //         right = mid;
    //     } else {
    //         left = mid + 1;
    //     }
    // }
    // return left;
}

test "bounds" {
    var a: [6]u32 = .{ 2, 4, 6, 8, 10, 12 };

    try std.testing.expectEqual(0, lowerBound(u32, &a, 6, 1));
    try std.testing.expectEqual(0, lowerBound(u32, &a, 6, 2));
    try std.testing.expectEqual(1, lowerBound(u32, &a, 6, 3));
    try std.testing.expectEqual(1, lowerBound(u32, &a, 6, 4));
    try std.testing.expectEqual(2, lowerBound(u32, &a, 6, 5));
    try std.testing.expectEqual(2, lowerBound(u32, &a, 6, 6));
    try std.testing.expectEqual(3, lowerBound(u32, &a, 6, 7));
    try std.testing.expectEqual(3, lowerBound(u32, &a, 6, 8));
    try std.testing.expectEqual(4, lowerBound(u32, &a, 6, 9));
    try std.testing.expectEqual(4, lowerBound(u32, &a, 6, 10));
    try std.testing.expectEqual(5, lowerBound(u32, &a, 6, 11));
    try std.testing.expectEqual(5, lowerBound(u32, &a, 6, 12));
    try std.testing.expectEqual(6, lowerBound(u32, &a, 6, 13));

    try std.testing.expectEqual(0, upperBound(u32, &a, 6, 1));
    try std.testing.expectEqual(1, upperBound(u32, &a, 6, 2));
    try std.testing.expectEqual(1, upperBound(u32, &a, 6, 3));
    try std.testing.expectEqual(2, upperBound(u32, &a, 6, 4));
    try std.testing.expectEqual(2, upperBound(u32, &a, 6, 5));
    try std.testing.expectEqual(3, upperBound(u32, &a, 6, 6));
    try std.testing.expectEqual(3, upperBound(u32, &a, 6, 7));
    try std.testing.expectEqual(4, upperBound(u32, &a, 6, 8));
    try std.testing.expectEqual(4, upperBound(u32, &a, 6, 9));
    try std.testing.expectEqual(5, upperBound(u32, &a, 6, 10));
    try std.testing.expectEqual(5, upperBound(u32, &a, 6, 11));
    try std.testing.expectEqual(6, upperBound(u32, &a, 6, 12));
    try std.testing.expectEqual(6, upperBound(u32, &a, 6, 13));
}
