const std = @import("std");

// what if it's a tree
// where each node is a hashmap
// but upon hash collision we traverse to a sub-hashmap

// memory use is far worse than I expected
// we probably have to use some other collision resolution as a first step

pub fn Storage(comptime K: type, comptime V: type, comptime SIZE: comptime_int) type {
    const nil = std.math.maxInt(K);

    return struct {
        const Self = @This();

        const Page = struct {
            keys: [SIZE]K,
            vals: [SIZE]V,
            // skip: [SIZE]u32, // for faster iteeation
            left: ?*Page,
            right: ?*Page,
            parent: ?*Page, // for stackless iterator

            fn create(alloc: std.mem.Allocator) !*Page {
                const page = try alloc.create(Page);
                page.keys = .{nil} ** SIZE;
                page.left = null;
                page.right = null;
                page.parent = null;
                return page;
            }

            fn destroy(page: *Page, alloc: std.mem.Allocator) void {
                if (page.left) |left| {
                    left.destroy(alloc);
                }
                if (page.right) |right| {
                    right.destroy(alloc);
                }
                alloc.destroy(page);
            }

            fn add(page: *Page, alloc: std.mem.Allocator, key: K, val: V) !void {
                const loc = std.hash.uint32(key) % SIZE;
                if (page.keys[loc] == nil) {
                    page.keys[loc] = key;
                    page.vals[loc] = val;
                } else {
                    std.debug.assert(page.keys[loc] != key);

                    if (key < page.keys[loc]) {
                        if (page.left == null) {
                            page.left = try Page.create(alloc);
                            page.left.?.parent = page;
                        }
                        try page.left.?.add(alloc, key, val);
                    } else {
                        if (page.right == null) {
                            page.right = try Page.create(alloc);
                            page.right.?.parent = page;
                        }
                        try page.right.?.add(alloc, key, val);
                    }
                }
            }

            fn get(page: *Page, key: K) ?*K {
                const loc = std.hash.uint32(key) % SIZE;
                if (page.keys[loc] == nil) {
                    return null;
                } else if (page.keys[loc] == key) {
                    return &page.vals[loc];
                } else {
                    if (key < page.keys[loc]) {
                        if (page.left) |left| {
                            return @call(.always_tail, Page.get, .{ left, key });
                        }
                    } else {
                        if (page.right) |right| {
                            return @call(.always_tail, Page.get, .{ right, key });
                        }
                    }
                }
                return null;
            }

            fn debugPrint(page: *Page, depth: usize) void {
                for (0..depth) |_| std.debug.print("  ", .{});
                std.debug.print("[ ", .{});
                for (0..SIZE) |i| {
                    if (page.keys[i] == nil) continue;
                    std.debug.print("{} ", .{page.keys[i]});
                }
                std.debug.print("]\n", .{});

                if (page.left) |left| left.debugPrint(depth + 1);
                if (page.right) |right| right.debugPrint(depth + 1);
            }
        };

        alloc: std.mem.Allocator,
        len: usize,
        root: ?*Page,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .len = 0,
                .root = null,
            };
        }

        pub fn deinit(storage: *Self) void {
            if (storage.root) |root| {
                root.destroy(storage.alloc);
            }
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: K) ?*V {
            if (key == nil) return null;
            if (storage.root) |root| return root.get(key);
            return null;
        }

        pub fn add(storage: *Self, key: K, val: V) !void {
            if (key == nil) return;

            if (storage.root == null) {
                storage.root = try Page.create(storage.alloc);
            }

            try storage.root.?.add(storage.alloc, key, val);
            storage.len += 1;
        }

        const Iterator = struct {
            const KV = struct { key: K, val: V };

            page: ?*Page,
            cursor: usize = 0,

            fn succ(page: *Page) ?*Page {
                if (page.right != null) {
                    var current = page.right;
                    while (current.?.left != null) : (current = current.?.left) {}
                    return current;
                }

                var n = page;
                var parent: ?*Page = page.parent;
                while (parent != null and parent.?.right == n) {
                    n = parent.?;
                    parent = parent.?.parent;
                }
                return parent;
            }

            pub fn next(it: *Iterator) ?KV {
                if (it.page == null) return null;

                while (it.cursor < SIZE and it.page.?.keys[it.cursor] == nil) : (it.cursor += 1) {}

                if (it.cursor == SIZE) {
                    it.page = succ(it.page.?);
                    it.cursor = 0;
                    return @call(.always_tail, Iterator.next, .{it});
                }

                it.cursor += 1;
                return .{
                    .key = it.page.?.keys[it.cursor - 1],
                    .val = it.page.?.vals[it.cursor - 1],
                };
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            var walk = storage.root;
            while (walk != null and walk.?.left != null) : (walk = walk.?.left) {}
            return .{ .page = walk };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s} {s}>\n", .{ @typeName(K), @typeName(V) });
            if (storage.root) |root| root.debugPrint(0);
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(u32, u32, 4).init(alloc);
    defer s.deinit();

    for (0..32) |i| {
        const x: u32 = @intCast(i * 51 % 32);
        try s.add(x, x);
        s.debugPrint();
    }

    var it = s.iterator();
    while (it.next()) |kv| {
        std.debug.print("{}\n", .{kv.key});
    }
}
