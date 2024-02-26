const std = @import("std");

// another database index structure is the b+tree
// and it's pretty neat and a lot simpler than the cst-tree
// in particular the balancing kinda comes natural (i skipped it in the t-trees for simplicity)
// however, the b+tree does not play nice with a single page size
// as the internal nodes are ultimately just small sets of pointers
// however, depending on the performance characteristics of changing the leaf node size
// we might get away with having two pools (small and large) for the entire ecs

// for the internal node size, i see two options
// either 5, fitting both size, keys and child pointers into one cache line
// which has less fanout -> more depth, but less cache misses in search
// or 15, fitting the size and all the keys into one cache line
// and then we incur one more miss to get the child pointer
const ISIZE = 5;

// for the leaf node, it's ultimately dependent on tree size vs data shuffling during update
// though, since I plan to store the data together with the keys in the node, instead of via ptrs
// the size of the data could also affect the optimal size
const LSIZE = 15;

fn Internal(comptime T: type) type {
    // const MAX = ISIZE / 2;

    return struct {
        const Self = @This();

        keys: [ISIZE]u32 align(64),
        children: [ISIZE]usize,
        len: u32,

        fn create(alloc: std.mem.Allocator) !*Self {
            var node = try alloc.create(Self);
            node.keys = .{0} ** ISIZE;
            node.len = 0;
            node.children = .{null} ** ISIZE;
            return node;
        }

        fn destroy(node: *Self, alloc: std.mem.Allocator, height: i32) void {
            std.debug.assert(height > 0);
            for (node.children) |child| {
                if (child == 0) continue;
                if (height == 2) {
                    lptr(child).?.destroy(alloc);
                } else {
                    iptr(child).?.destroy(alloc, height - 1);
                }
            }
            alloc.destroy(node);
        }

        fn iptr(ptr: usize) ?*Self {
            return @ptrFromInt(ptr);
        }

        fn lptr(ptr: usize) ?*Leaf(T) {
            return @ptrFromInt(ptr);
        }
    };
}

comptime {
    if (ISIZE == 5) {
        std.debug.assert(@sizeOf(Internal(void)) == 0);
    }
}

fn Leaf(comptime T: type) type {
    // const MAX = LSIZE / 2;

    return struct {
        const Self = @This();

        keys: [LSIZE]u32 align(64),
        vals: [LSIZE]T,
        len: u32,

        prev: ?*Self,
        next: ?*Self,

        fn create(alloc: std.mem.Allocator) !*Self {
            var node = try alloc.create(Self);
            node.keys = .{0} ** LSIZE;
            node.len = 0;
            node.prev = null;
            node.next = null;
            return node;
        }

        fn destroy(node: *Self, alloc: std.mem.Allocator) void {
            alloc.destroy(node);
        }

        fn add(node: *Self, alloc: std.mem.Allocator, key: u32, val: T) !i32 {
            // find where into this node our data would fit
            var i: u32 = 0;
            while (i < node.len) : (i += 1) {
                if (node.keys[i] >= key) break;
            }

            if (node.len + 1 < LSIZE) {
                std.mem.copyBackwards(
                    u32,
                    node.keys[i + 1 .. node.len],
                    node.keys[i .. node.len - 1],
                );
                std.mem.copyBackwards(
                    u32,
                    node.keys[i + 1 .. node.len],
                    node.keys[i .. node.len - 1],
                );

                node.keys[i] = key;
                node.vals[i] = val;
                node.len += 1;
            }

            _ = alloc;
        }

        fn iptr(ptr: usize) ?*Internal(T) {
            return @ptrFromInt(ptr);
        }

        fn lptr(ptr: usize) ?*Self {
            return @ptrFromInt(ptr);
        }

        fn debugPrint(node: *Self) void {
            std.debug.print("{{ ", .{});
        }
    };
}

// NOTE I think add, del, get and and Iterator is all that we really need
fn Storage(comptime T: type) type {
    return struct {
        const Self = @This();

        const Iterator = struct {
            const KV = struct { key: u32, val: T };

            pub fn next() ?*KV {
                @compileError("TODO");
            }
        };

        alloc: std.mem.Allocator,
        root: usize,
        height: i32,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .root = 0,
                .height = 0,
            };
        }

        pub fn deinit(storage: *Self) void {
            if (storage.root != 0) {
                if (storage.height == 0) {
                    lptr(storage.root).?.destroy(storage.alloc);
                } else {
                    iptr(storage.root).?.destroy(storage.alloc, storage.height);
                }
            }
            storage.* = undefined;
        }

        fn add(storage: *Self, key: u32, val: T) !void {
            std.debug.assert(storage.height >= 0);

            if (storage.root == 0) {
                storage.root = @intFromPtr(try Leaf(T).create(storage.alloc));
                storage.height += 1;
            }

            if (storage.height == 0) {
                storage.height += try lptr(storage.root).?.add(std.mem.Allocator, key, val);
            } else {
                //
            }

            std.debug.assert(storage.height > 0);
        }

        fn del(storage: *Self, key: u32) void {
            _ = storage;
            _ = key;
            @compileError("TODO");
        }

        fn get(storage: *Self, key: u32) ?*T {
            _ = storage;
            _ = key;
            @compileError("TODO");
        }

        fn iterator(storage: *Self) Iterator {
            _ = storage;
            @compileError("TODO");
        }

        fn iptr(ptr: usize) ?*Internal(T) {
            return @ptrFromInt(ptr);
        }

        fn lptr(ptr: usize) ?*Leaf(T) {
            return @ptrFromInt(ptr);
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(usize).init(alloc);
    defer s.deinit();

    var i: u32 = 0;
    for (0..100) |j| {
        try s.add(i, j);
        i = (i + 40507) % 65536;
    }
}
