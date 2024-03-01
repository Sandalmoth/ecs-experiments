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
    std.debug.assert(NODE_SIZE >= 3);
    std.debug.assert(LEAF_SIZE >= 3);
    // circular array deques are much faster for power-of-two sizes
    // so we'll just make that a restriction
    std.debug.assert(std.math.isPowerOfTwo(NODE_SIZE + 1));
    std.debug.assert(std.math.isPowerOfTwo(LEAF_SIZE + 1));

    return struct {
        const Self = @This();

        const Node = struct {
            keys: Deque(NODE_SIZE + 1, K), // one unused
            children: Deque(NODE_SIZE + 1, usize),

            fn create(alloc: std.mem.Allocator) !*Node {
                var node = try alloc.create(Node);
                node.keys = Deque(NODE_SIZE + 1, K){};
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

            fn del(node: *Node, alloc: std.mem.Allocator, height: usize, key: K) bool {
                std.debug.assert(node.children.len >= 2);
                std.debug.assert(node.children.len == node.keys.len + 1);
                // NOTE the use of upperBound for searching in nodes
                // as if our element is equal, we want to go right, not left
                // so equal elements should return the next index, rather than their own
                const loc = node.keys.upperBound(key);
                // node._debugPrint(height, 0);

                if (height == 1) {
                    const underflow = lp(node.children.at(loc)).del(key);
                    if (underflow) {
                        if (lp(node.children.at(loc)).keys.len == 0) {
                            unreachable; // stealing should prevent this from ever happening
                        }
                        // node._debugPrint(height, 0);

                        // strategy is
                        // 1. steal left
                        // 2. merge left
                        // 3. steal right
                        // 4. merge right
                        // and the deques should make stealing nice and fast

                        if (loc > 0 and
                            lp(node.children.at(loc - 1)).keys.len > LEAF_SIZE / 2 + 1)
                        {
                            // steal left
                            std.debug.print("leaf steal left\n", .{});
                            lp(node.children.at(loc)).keys.pushFront(
                                lp(node.children.at(loc - 1)).keys.popBack(),
                            );
                            lp(node.children.at(loc)).vals.pushFront(
                                lp(node.children.at(loc - 1)).vals.popBack(),
                            );
                            node.keys.set(loc - 1, lp(node.children.at(loc)).keys.front());
                        } else if (loc > 0) {
                            // merge left
                            std.debug.print("leaf merge left\n", .{});
                            // if we cannot steal left, but left exists, there is room to merge
                            std.debug.assert(lp(node.children.at(loc)).keys.len +
                                lp(node.children.at(loc - 1)).keys.len <= LEAF_SIZE);

                            lp(node.children.at(loc - 1)).merge(alloc, lp(node.children.at(loc)));
                            _ = node.keys.remove(loc - 1);
                            _ = node.children.remove(loc);
                        } else if (loc < node.keys.len and
                            lp(node.children.at(loc + 1)).keys.len > LEAF_SIZE / 2 + 1)
                        {
                            // steal right
                            std.debug.assert(loc == 0);
                            std.debug.print("leaf steal right\n", .{});
                            // NOTE in the condition: node.keys.len == node.children.len - 1
                            lp(node.children.at(loc)).keys.pushBack(
                                lp(node.children.at(loc + 1)).keys.popFront(),
                            );
                            lp(node.children.at(loc)).vals.pushBack(
                                lp(node.children.at(loc + 1)).vals.popFront(),
                            );
                            node.keys.set(loc, lp(node.children.at(loc + 1)).keys.front());
                        } else if (loc < node.keys.len) {
                            // merge right
                            std.debug.assert(loc == 0);
                            std.debug.print("leaf merge right\n", .{});
                            std.debug.assert(lp(node.children.at(loc)).keys.len +
                                lp(node.children.at(loc + 1)).keys.len <= LEAF_SIZE);

                            lp(node.children.at(loc)).merge(alloc, lp(node.children.at(loc + 1)));
                            _ = node.keys.remove(loc);
                            _ = node.children.remove(loc + 1);
                        }
                        // node._debugPrint(height, 0);
                    }
                } else {
                    const underflow = np(node.children.at(loc)).del(alloc, height - 1, key);
                    if (underflow) {
                        if (np(node.children.at(loc)).children.len == 0) {
                            unreachable; // stealing should prevent this from ever happening
                        }
                        // node._debugPrint(height, 0);

                        // strategy is the same as for the leaf underflow

                        if (loc > 0 and
                            np(node.children.at(loc - 1)).children.len > NODE_SIZE / 2 + 1)
                        {
                            // steal left
                            std.debug.print("node merge left\n", .{});
                            _ = np(node.children.at(loc - 1)).keys.popBack();
                            if (height == 2) {
                                np(node.children.at(loc)).keys.pushFront(
                                    lp(np(node.children.at(loc)).children.front()).keys.front(),
                                );
                            } else {
                                np(node.children.at(loc)).keys.pushFront(
                                    np(np(node.children.at(loc)).children.front()).keys.front(),
                                );
                            }
                            np(node.children.at(loc)).children.pushFront(
                                np(node.children.at(loc - 1)).children.popBack(),
                            );
                            const new_lb = blk: {
                                var h = height - 1;
                                var walk = node.children.at(loc);
                                while (h > 0) : (h -= 1) {
                                    walk = np(walk).children.front();
                                }
                                break :blk lp(walk).keys.front();
                            };
                            node.keys.set(loc - 1, new_lb);
                        } else if (loc > 0) {
                            // merge left
                            std.debug.print("node merge left\n", .{});
                            std.debug.assert(np(node.children.at(loc)).keys.len +
                                np(node.children.at(loc - 1)).keys.len <= NODE_SIZE);

                            np(node.children.at(loc - 1)).merge(
                                alloc,
                                height - 1,
                                np(node.children.at(loc)),
                            );
                            _ = node.keys.remove(loc - 1);
                            _ = node.children.remove(loc);
                        } else if (loc < node.keys.len and
                            np(node.children.at(loc + 1)).children.len > NODE_SIZE / 2 + 1)
                        {
                            // steal right
                            std.debug.assert(loc == 0);
                            std.debug.print("node steal right\n", .{});
                            std.debug.print("{}\n", .{loc});
                            if (height == 2) {
                                np(node.children.at(loc)).keys.pushBack(
                                    lp(np(node.children.at(loc + 1)).children.front()).keys.front(),
                                );
                            } else {
                                np(node.children.at(loc)).keys.pushBack(
                                    np(np(node.children.at(loc + 1)).children.front()).keys.front(),
                                );
                            }
                            _ = np(node.children.at(loc + 1)).keys.popFront();
                            np(node.children.at(loc)).children.pushBack(
                                np(node.children.at(loc + 1)).children.popFront(),
                            );
                            const new_lb = blk: {
                                var h = height - 1;
                                var walk = node.children.at(loc + 1);
                                while (h > 0) : (h -= 1) {
                                    walk = np(walk).children.front();
                                }
                                break :blk lp(walk).keys.front();
                            };
                            node.keys.set(loc, new_lb);
                        } else if (loc < node.keys.len) {
                            // merge right
                            std.debug.assert(loc == 0);
                            std.debug.print("node merge right\n", .{});
                            std.debug.assert(np(node.children.at(loc)).keys.len +
                                np(node.children.at(loc + 1)).keys.len <= NODE_SIZE);

                            np(node.children.at(loc)).merge(
                                alloc,
                                height - 1,
                                np(node.children.at(loc + 1)),
                            );
                            _ = node.keys.remove(loc);
                            _ = node.children.remove(loc + 1);
                        }

                        // node._debugPrint(height, 0);
                    }
                }

                return node.children.len <= NODE_SIZE / 2;
            }

            fn merge(node: *Node, alloc: std.mem.Allocator, height: usize, next: *Node) void {
                const new_lb = blk: {
                    var h = height;
                    var walk = @intFromPtr(next);
                    while (h > 0) : (h -= 1) {
                        walk = np(walk).children.front();
                    }
                    break :blk lp(walk).keys.front();
                };

                node.keys.pushBack(new_lb);
                node.children.pushBack(next.children.popFront());
                while (next.children.len > 0) {
                    node.keys.pushBack(next.keys.popFront());
                    node.children.pushBack(next.children.popFront());
                }

                next.destroy(alloc, height);
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

                std.debug.assert(leaf.keys.len == leaf.vals.len);
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

            fn del(leaf: *Leaf, key: K) bool {
                const loc = leaf.keys.lowerBound(key);
                std.debug.assert(leaf.keys.at(loc) == key);

                _ = leaf.keys.remove(loc);
                _ = leaf.vals.remove(loc);

                std.debug.assert(leaf.keys.len == leaf.vals.len);
                return leaf.keys.len <= LEAF_SIZE / 2;
            }

            fn merge(leaf: *Leaf, alloc: std.mem.Allocator, next: *Leaf) void {
                std.debug.assert(leaf.keys.back() < next.keys.front());
                while (next.keys.len > 0) {
                    leaf.keys.pushBack(next.keys.popFront());
                    leaf.vals.pushBack(next.vals.popFront());
                }
                // stitch the neighbors, as next will be destroyed
                leaf.next = next.next;
                if (next.next != null) {
                    next.next.?.prev = leaf;
                }

                next.destroy(alloc);
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
        len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .root = 0,
                .height = 0,
                .len = 0,
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
        /// on error, the storage may be left in an irreparable invalid state
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

            storage.len += 1;
        }

        /// asserts that the key is in the storage
        pub fn del(storage: *Self, key: K) void {
            std.debug.assert(storage.root != 0);
            std.debug.assert(storage.len > 0);

            if (storage.height == 0) {
                const underflow = lp(storage.root).del(key);
                if (underflow and lp(storage.root).keys.len == 0) {
                    lp(storage.root).destroy(storage.alloc);
                    storage.root = 0;
                    std.debug.assert(storage.height == 0);
                    std.debug.assert(storage.len == 1); // decremented at end of this function
                }
            } else {
                const underflow = np(storage.root).del(storage.alloc, storage.height, key);
                if (underflow and np(storage.root).keys.len < 2) {
                    // special case for root node allows size all the way down to 2
                    std.debug.assert(np(storage.root).children.len == 1);

                    const old = np(storage.root);
                    storage.root = np(storage.root).children.front();
                    old.children.len = 0;
                    old.destroy(storage.alloc, storage.height);
                    storage.height -= 1;
                }
            }

            storage.len -= 1;
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

    var s = Storage(u32, f32, 3, 3).init(alloc);
    defer s.deinit();

    const n = 32;
    for (0..n) |i| {
        // std.debug.print("\ninserting {}\n", .{i});
        // try s.add(@intCast(i), @floatFromInt(i));
        // std.debug.print("\ninserting {}\n", .{i * 89 % n});
        try s.add(@intCast(i * 89 % n), @floatFromInt(i));
        // s.debugPrint();
    }
    s.debugPrint();

    for (0..n) |i| {
        // std.debug.print("\ndeleting {}\n", .{i});
        // s.del(@intCast(i));
        // std.debug.print("\ndeleting {}\n", .{n - i - 1});
        // s.del(@intCast(n - i - 1));
        std.debug.print("\ndeleting {}\n", .{i * 87 % n});
        s.del(@intCast(i * 87 % n));
        s.debugPrint();
    }

    // std.debug.print("{}\n", .{@sizeOf(@TypeOf(s).Node)});
    // std.debug.print("{}\n", .{@sizeOf(@TypeOf(s).Leaf)});

    // some fuzz tests just to see that we don't hit a crash/assert
    std.debug.print("\ncommencing fuzz tests!\n", .{});

    const fuzz_lim = 4 * 1024; // must be power of two for the weyl sequence
    {
        var _s = Storage(u32, f32, 3, 3).init(alloc);
        defer _s.deinit();

        for (0..fuzz_lim) |i| {
            try _s.add(@intCast(i), @floatFromInt(i));
        }

        for (0..fuzz_lim) |i| {
            _ = _s.del(@intCast(i));
        }
    }

    {
        var _s = Storage(u32, f32, 3, 3).init(alloc);
        defer _s.deinit();

        for (0..fuzz_lim) |i| {
            try _s.add(fuzz_lim - @as(u32, @intCast(i)), @floatFromInt(i));
        }

        for (0..fuzz_lim) |i| {
            _ = _s.del(fuzz_lim - @as(u32, @intCast(i)));
        }
    }

    {
        var _s = Storage(u32, f32, 3, 3).init(alloc);
        defer _s.deinit();

        var x: u32 = 0;
        for (0..fuzz_lim) |i| {
            try _s.add(x % fuzz_lim, @floatFromInt(i));
            x +%= 2_654_435_761; // prime
        }

        x = 0;
        for (0..fuzz_lim) |_| {
            _ = _s.del(x % fuzz_lim);
            x +%= 2_654_435_761;
        }
    }

    std.debug.print("fuzz tests finished!\n", .{});
}
