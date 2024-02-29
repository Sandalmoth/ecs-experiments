const std = @import("std");

const Deque = @import("deque.zig").FixedDeque;

// i like the b+tree design, so i'm gonna try to rewrite it better
// it may or may not be a good idea, but I'm gonna try using deques in all the nodes/leaves

pub fn Storage(
    comptime K: type,
    comptime V: type,
    comptime NODE_SIZE: comptime_int, // max number of children for an internal node
    comptime LEAF_SIZE: comptime_int, // max number of entries in a leaf node
) type {
    std.debug.assert(NODE_SIZE >= 4);
    std.debug.assert(LEAF_SIZE >= 4);
    // circular array deques are much faster for power-of-two sizes
    // so we'll just make that a restriction
    std.debug.assert(std.math.isPowerOfTwo(NODE_SIZE));
    std.debug.assert(std.math.isPowerOfTwo(LEAF_SIZE));

    return struct {
        const Self = @This();

        const Node = struct {
            keys: Deque(NODE_SIZE, K),
            children: Deque(NODE_SIZE + 1, usize),

            fn create(alloc: std.mem.Allocator) !*Node {
                var node = try alloc.create(Node);
                node.keys = Deque(NODE_SIZE, K){};
                node.children = Deque(NODE_SIZE + 1, usize){};
                return node;
            }

            fn destroy(node: *Node, alloc: std.mem.Allocator, height: usize) void {
                if (height == 1) {
                    for (0..node.children.len) |i| {
                        std.debug.assert(node.children.at(@intCast(i)) != 0);
                        lp(node.children.at(@intCast(i))).destroy(alloc);
                    }
                } else {
                    for (0..node.children.len) |i| {
                        std.debug.assert(node.children.at(@intCast(i)) != 0);
                        np(node.children.at(@intCast(i))).destroy(alloc, height - 1);
                    }
                }
                alloc.destroy(node);
            }

            fn add(node: *Node, alloc: std.mem.Allocator, height: usize, key: K, val: V) !bool {
                const loc = node.keys.lowerBound(key);
                if (height == 1) {
                    const overflow = lp(node.children.at(loc)).add(key, val);
                    if (overflow) {
                        const next = try lp(node.children.at(loc)).split(alloc);
                        node.keys.insert(loc, next.keys.front());
                        node.children.insert(loc + 1, @intFromPtr(next));
                    }
                } else {
                    const overflow = try np(node.children.at(loc)).add(alloc, height - 1, key, val);
                    if (overflow) {
                        const result = try np(node.children.at(loc)).split(alloc);
                        node.keys.insert(loc, result.pivot);
                        node.children.insert(loc + 1, @intFromPtr(result.next));
                    }
                }

                return node.children.len > NODE_SIZE;
            }

            const Split = struct {
                next: *Node,
                pivot: K,
            };
            fn split(node: *Node, alloc: std.mem.Allocator) !Split {
                const half = NODE_SIZE / 2 + 1;
                var result = Split{
                    .next = try Node.create(alloc),
                    .pivot = undefined,
                };

                for (0..half - 1) |_| {
                    result.next.keys.pushFront(node.keys.popBack());
                }
                result.pivot = node.keys.popBack();
                for (0..half) |_| {
                    result.next.children.pushFront(node.children.popBack());
                }

                return result;
            }

            fn _debugPrint(node: *Node, height: usize, depth: usize) void {
                std.debug.assert(height > 0);
                for (0..depth) |_| std.debug.print("  ", .{});
                std.debug.print("{}\n", .{node.keys});

                if (height == 1) {
                    for (0..node.children.len) |i| {
                        lp(node.children.at(@intCast(i)))._debugPrint(depth + 1);
                    }
                } else {
                    for (0..node.children.len) |i| {
                        np(node.children.at(@intCast(i)))._debugPrint(height - 1, depth + 1);
                    }
                }
            }
        };

        const Leaf = struct {
            keys: Deque(LEAF_SIZE + 1, K),
            vals: Deque(LEAF_SIZE + 1, V),

            prev: ?*Leaf,
            next: ?*Leaf,

            fn create(alloc: std.mem.Allocator) !*Leaf {
                var leaf = try alloc.create(Leaf);
                leaf.keys = Deque(LEAF_SIZE + 1, K){};
                leaf.vals = Deque(LEAF_SIZE + 1, V){};
                leaf.prev = null;
                leaf.next = null;
                return leaf;
            }

            fn destroy(leaf: *Leaf, alloc: std.mem.Allocator) void {
                alloc.destroy(leaf);
            }

            fn add(leaf: *Leaf, key: K, val: V) bool {
                const loc = leaf.keys.lowerBound(key);
                if (leaf.keys.len > 0 and loc < leaf.keys.len) {
                    std.debug.assert(leaf.keys.at(loc) != key);
                }

                leaf.keys.insert(loc, key);
                leaf.vals.insert(loc, val);

                return leaf.keys.len > LEAF_SIZE;
            }

            fn split(leaf: *Leaf, alloc: std.mem.Allocator) !*Leaf {
                const next = try Leaf.create(alloc);

                const half = LEAF_SIZE / 2 + 1;
                for (0..half) |_| {
                    next.keys.pushFront(leaf.keys.popBack());
                    next.vals.pushFront(leaf.vals.popBack());
                }

                // stitch neighbours
                next.next = leaf.next;
                next.prev = leaf;
                leaf.next = next;
                if (next.next != null) {
                    next.next.?.prev = next;
                }

                return next;
            }

            fn _debugPrint(leaf: *Leaf, depth: usize) void {
                for (0..depth) |_| std.debug.print("  ", .{});
                std.debug.print("{}\n", .{leaf.keys});
            }
        };

        const KeyValIterator = struct {};

        alloc: std.mem.Allocator,
        root: usize,
        height: usize,

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
                    lp(storage.root).destroy(storage.alloc);
                } else {
                    np(storage.root).destroy(storage.alloc, storage.height);
                }
            }
            storage.* = undefined;
        }

        /// asserts that the key is not in the storage
        pub fn add(storage: *Self, key: K, val: V) !void {
            if (storage.root == 0) {
                const root = try Leaf.create(storage.alloc);
                storage.root = @intFromPtr(root);
            }

            if (storage.height == 0) {
                const overflow = lp(storage.root).add(key, val);
                if (overflow) {
                    const root = try Node.create(storage.alloc);
                    errdefer root.destroy(storage.alloc, 1);
                    const next = try lp(storage.root).split(storage.alloc);

                    root.children.pushBack(storage.root);
                    root.children.pushBack(@intFromPtr(next));
                    root.keys.pushBack(next.keys.front());

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            } else {
                const overflow = try np(storage.root).add(storage.alloc, storage.height, key, val);
                if (overflow) {
                    const root = try Node.create(storage.alloc);
                    errdefer root.destroy(storage.alloc, 1);
                    const result = try np(storage.root).split(storage.alloc);

                    root.children.pushBack(storage.root);
                    root.children.pushBack(@intFromPtr(result.next));
                    root.keys.pushBack(result.pivot);

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            }
        }

        fn np(ptr: usize) *Node {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn lp(ptr: usize) *Leaf {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s} {s}> \n", .{ @typeName(K), @typeName(V) });
            if (storage.root == 0) return;

            if (storage.height == 0) {
                lp(storage.root)._debugPrint(0);
            } else {
                np(storage.root)._debugPrint(storage.height, 0);
            }
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(u32, f32, 4, 4).init(alloc);
    defer s.deinit();

    for (0..100) |i| {
        std.debug.print("\ninserting {}\n", .{i});
        try s.add(@intCast(i), @floatFromInt(i));
        s.debugPrint();
    }
}
