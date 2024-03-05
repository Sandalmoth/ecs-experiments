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

pub fn lowerBound(comptime T: type, a: [*]T, len: u32, x: T) u32 {
    var count: u32 = 0;
    for (0..len) |i| {
        count += if (a[i] < x) 1 else 0;
    }
    return count;
}

pub fn upperBound(comptime T: type, a: [*]T, len: u32, x: T) u32 {
    var count: u32 = 0;
    for (0..len) |i| {
        count += if (a[i] <= x) 1 else 0;
    }
    return count;
}
