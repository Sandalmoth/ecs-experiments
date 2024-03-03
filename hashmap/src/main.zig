const std = @import("std");

// yet another comparison point
// just wrap std.arrayhashmap in the same interface

pub fn Storage(comptime T: type) type {
    return struct {
        const Self = @This();

        map: std.AutoArrayHashMap(u32, T),
        len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .map = std.AutoArrayHashMap(u32, T).init(alloc),
                .len = 0,
            };
        }

        pub fn deinit(storage: *Self) void {
            storage.map.deinit();
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: u32) ?*T {
            return storage.map.getPtr(key);
        }

        pub fn add(storage: *Self, key: u32, val: T) !void {
            std.debug.assert(!storage.map.contains(key));
            try storage.map.putNoClobber(key, val);
            storage.len += 1;
        }

        pub fn del(storage: *Self, key: u32) void {
            std.debug.assert(storage.map.contains(key));
            _ = storage.map.swapRemove(key);
            storage.len -= 1;
        }

        const Iterator = struct {
            const KV = struct { key: u32, val: T };

            it: std.AutoArrayHashMap(u32, T).Iterator,

            pub fn next(it: *Iterator) ?KV {
                if (it.it.next()) |kv| {
                    return .{ .key = kv.key_ptr.*, .val = kv.value_ptr.* };
                }
                return null;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            return .{ .it = storage.map.iterator() };
        }
    };
}

pub fn main() void {}
