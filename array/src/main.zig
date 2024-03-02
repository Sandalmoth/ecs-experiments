const std = @import("std");

// the point here is not to make something good, but to get a comparison point for benchmarking

pub fn Storage(comptime T: type) type {
    const nil = std.math.maxInt(u32);

    return struct {
        const Self = @This();

        keys: []u32 align(64),
        vals: []T align(64),

        len: usize,

        alloc: std.mem.Allocator,

        pub fn init(alloc: std.mem.Allocator, capacity: usize) !Self {
            var storage = Self{
                .alloc = alloc,
                .keys = undefined,
                .vals = undefined,
                .len = 0,
            };
            storage.keys = try alloc.alloc(u32, capacity);
            errdefer alloc.free(storage.keys);

            storage.vals = try alloc.alloc(T, capacity);
            errdefer alloc.free(storage.vals);

            for (0..capacity) |i| {
                storage.keys[i] = nil;
            }

            return storage;
        }

        pub fn deinit(storage: *Self) void {
            storage.alloc.free(storage.keys);
            storage.alloc.free(storage.vals);
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: u32) ?*T {
            std.debug.assert(key < storage.keys.len);

            if (storage.keys[key] == nil) {
                return null;
            } else {
                return &storage.vals[key];
            }
        }

        pub fn add(storage: *Self, key: u32, val: T) void {
            std.debug.assert(key < storage.keys.len);
            std.debug.assert(storage.keys[key] == nil);

            storage.keys[key] = key;
            storage.vals[key] = val;
            storage.len += 1;
        }

        pub fn del(storage: *Self, key: u32) void {
            std.debug.assert(key < storage.keys.len);
            std.debug.assert(storage.keys[key] != nil);

            storage.keys[key] = nil;
            storage.len -= 1;
        }

        const Iterator = struct {
            const KV = struct { key: u32, val: T };

            storage: *Self,
            cursor: usize,

            pub fn next(it: *Iterator) ?KV {
                while (it.cursor < it.storage.keys.len and
                    it.storage.keys[it.cursor] == nil) : (it.cursor += 1)
                {}

                if (it.cursor >= it.storage.keys.len) {
                    return null;
                } else {
                    const result = KV{
                        .key = @intCast(it.cursor),
                        .val = it.storage.vals[it.cursor],
                    };
                    it.cursor += 1;
                    return result;
                }
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            return .{
                .storage = storage,
                .cursor = 0,
            };
        }
    };
}

pub fn main() void {}
