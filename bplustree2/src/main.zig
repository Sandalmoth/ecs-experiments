const std = @import("std");

const Deque = @import("deque.zig").FixedDeque;

// i like the b+tree design, so i'm gonna try to rewrite it better
// it may or may not be a good idea, but I'm gonna try using deques in all the nodes/leaves

// we end up having a spare slot in the node keys deques
// so, for simplicity, track also the lower bound in the first child
// which makes deleting much simpler

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
            keys: Deque(NODE_SIZE + 1, K), // first slot is (unlike textbook b+tree) used for lb
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
                const loc = blk: {
                    if (key < node.keys.front()) {
                        break :blk 0;
                    }
                    break :blk node.keys.upperBound(key) - 1;
                };
                std.debug.assert(node.keys.at(loc) != key);

                if (height == 1) {
                    const overflow = lp(node.children.at(loc)).add(key, val);
                    if (!overflow) {
                        node.keys.set(loc, lp(node.children.at(loc)).keys.front());
                    } else {
                        const next = try lp(node.children.at(loc)).split(alloc);
                        node.keys.insert(loc + 1, next.keys.front());
                        node.children.insert(loc + 1, @intFromPtr(next));
                    }
                } else {
                    const overflow = try np(node.children.at(loc)).add(alloc, height - 1, key, val);
                    if (!overflow) {
                        node.keys.set(loc, np(node.children.at(loc)).keys.front());
                    } else {
                        const next = try np(node.children.at(loc)).split(alloc);
                        node.keys.insert(loc + 1, next.keys.front());
                        node.children.insert(loc + 1, @intFromPtr(next));
                    }
                }

                std.debug.assert(node.keys.isSorted());
                return node.children.len > NODE_SIZE;
            }

            fn split(node: *Node, alloc: std.mem.Allocator) !*Node {
                const next = try Node.create(alloc);

                const half = NODE_SIZE / 2 + 1;
                for (0..half) |_| {
                    next.keys.pushFront(node.keys.popBack());
                    next.children.pushFront(node.children.popBack());
                }

                std.debug.assert(node.keys.isSorted());
                std.debug.assert(next.keys.isSorted());
                return next;
            }

            fn del(node: *Node, alloc: std.mem.Allocator, height: usize, key: K) bool {
                std.debug.assert(node.children.len >= 2);
                // NOTE the use of upperBound for searching in nodes
                // as if our element is equal, we want to go right, not left
                // so equal elements should return the next index, rather than their own
                const loc = node.keys.upperBound(key) - 1;

                if (height == 1) {
                    const underflow = lp(node.children.at(loc)).del(key);
                    if (!underflow) {
                        // update the lower bound
                        node.keys.set(loc, lp(node.children.at(loc)).keys.front());
                    } else {
                        if (lp(node.children.at(loc)).keys.len == 0) {
                            unreachable; // stealing should prevent this from ever happening
                        }

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

                            lp(node.children.at(loc)).keys.pushFront(
                                lp(node.children.at(loc - 1)).keys.popBack(),
                            );
                            lp(node.children.at(loc)).vals.pushFront(
                                lp(node.children.at(loc - 1)).vals.popBack(),
                            );
                            node.keys.set(loc, lp(node.children.at(loc)).keys.front());
                        } else if (loc > 0) {
                            // merge left
                            // if we cannot steal left, but left exists, there is room to merge
                            std.debug.assert(lp(node.children.at(loc)).keys.len +
                                lp(node.children.at(loc - 1)).keys.len <= LEAF_SIZE);

                            lp(node.children.at(loc - 1)).merge(alloc, lp(node.children.at(loc)));
                            _ = node.keys.remove(loc);
                            _ = node.children.remove(loc);
                            node.keys.set(loc - 1, lp(node.children.at(loc - 1)).keys.front());
                        } else if (lp(node.children.at(loc + 1)).keys.len > LEAF_SIZE / 2 + 1) {
                            // steal right
                            std.debug.assert(loc == 0);
                            // NOTE in the condition: node.keys.len == node.children.len - 1
                            lp(node.children.at(loc)).keys.pushBack(
                                lp(node.children.at(loc + 1)).keys.popFront(),
                            );
                            lp(node.children.at(loc)).vals.pushBack(
                                lp(node.children.at(loc + 1)).vals.popFront(),
                            );
                            node.keys.set(loc, lp(node.children.at(loc)).keys.front());
                            node.keys.set(loc + 1, lp(node.children.at(loc + 1)).keys.front());
                        } else {
                            // merge right
                            std.debug.assert(loc == 0);
                            std.debug.assert(lp(node.children.at(loc)).keys.len +
                                lp(node.children.at(loc + 1)).keys.len <= LEAF_SIZE);

                            lp(node.children.at(loc)).merge(alloc, lp(node.children.at(loc + 1)));
                            _ = node.keys.remove(loc + 1);
                            _ = node.children.remove(loc + 1);
                            node.keys.set(loc, lp(node.children.at(loc)).keys.front());
                        }
                    }

                    //

                } else {
                    const underflow = np(node.children.at(loc)).del(alloc, height - 1, key);
                    if (!underflow) {
                        node.keys.set(loc, np(node.children.at(loc)).keys.front());
                    } else {
                        if (np(node.children.at(loc)).children.len == 0) {
                            unreachable; // stealing should prevent this from ever happening
                        }

                        // strategy is the same as for the leaf underflow

                        if (loc > 0 and
                            np(node.children.at(loc - 1)).children.len > NODE_SIZE / 2 + 1)
                        {
                            // steal left
                            np(node.children.at(loc)).keys.pushFront(
                                np(node.children.at(loc - 1)).keys.popBack(),
                            );
                            np(node.children.at(loc)).children.pushFront(
                                np(node.children.at(loc - 1)).children.popBack(),
                            );
                            node.keys.set(loc, np(node.children.at(loc)).keys.front());
                        } else if (loc > 0) {
                            // merge left
                            std.debug.assert(np(node.children.at(loc)).keys.len +
                                np(node.children.at(loc - 1)).keys.len <= NODE_SIZE);

                            np(node.children.at(loc - 1)).merge(
                                alloc,
                                height - 1,
                                np(node.children.at(loc)),
                            );
                            _ = node.keys.remove(loc);
                            _ = node.children.remove(loc);
                            node.keys.set(loc - 1, np(node.children.at(loc - 1)).keys.front());
                        } else if (np(node.children.at(loc + 1)).children.len > NODE_SIZE / 2 + 1) {
                            // steal right
                            std.debug.assert(loc == 0);

                            np(node.children.at(loc)).keys.pushBack(
                                np(node.children.at(loc + 1)).keys.popFront(),
                            );
                            np(node.children.at(loc)).children.pushBack(
                                np(node.children.at(loc + 1)).children.popFront(),
                            );
                            node.keys.set(loc, np(node.children.at(loc)).keys.front());
                            node.keys.set(loc + 1, np(node.children.at(loc + 1)).keys.front());
                        } else {
                            // merge right
                            std.debug.assert(loc == 0);
                            std.debug.assert(np(node.children.at(loc)).keys.len +
                                np(node.children.at(loc + 1)).keys.len <= NODE_SIZE);

                            np(node.children.at(loc)).merge(
                                alloc,
                                height - 1,
                                np(node.children.at(loc + 1)),
                            );
                            _ = node.keys.remove(loc + 1);
                            _ = node.children.remove(loc + 1);
                            node.keys.set(loc, np(node.children.at(loc)).keys.front());
                        }
                    }
                }

                std.debug.assert(node.keys.isSorted());
                return node.children.len <= NODE_SIZE / 2;
            }

            fn merge(node: *Node, alloc: std.mem.Allocator, height: usize, next: *Node) void {
                while (next.children.len > 0) {
                    node.keys.pushBack(next.keys.popFront());
                    node.children.pushBack(next.children.popFront());
                }

                std.debug.assert(node.keys.isSorted());
                next.destroy(alloc, height);
            }

            fn get(node: *Node, height: usize, key: K) ?*V {
                const loc = blk: {
                    if (key < node.keys.front()) {
                        break :blk 0;
                    }
                    break :blk node.keys.upperBound(key) - 1;
                };

                if (height == 1) {
                    return lp(node.children.at(loc)).get(key);
                } else {
                    return @call(
                        .always_tail,
                        Node.get,
                        .{ np(node.children.at(loc)), height - 1, key },
                    );
                    // return np(node.children.at(loc)).get(height - 1, key);
                }
            }

            fn getLeaf(node: *Node, height: usize, key: K) ?*Leaf {
                const loc = blk: {
                    if (key < node.keys.front()) {
                        break :blk 0;
                    }
                    break :blk node.keys.upperBound(key) - 1;
                };

                if (height == 1) {
                    return lp(node.children.at(loc));
                } else {
                    return @call(
                        .always_tail,
                        Node.getLeaf,
                        .{ np(node.children.at(loc)), height - 1, key },
                    );
                }
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
                std.debug.assert(leaf.keys.isSorted());
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

                std.debug.assert(leaf.keys.isSorted());
                std.debug.assert(next.keys.isSorted());
                return next;
            }

            fn del(leaf: *Leaf, key: K) bool {
                const loc = leaf.keys.lowerBound(key);
                std.debug.assert(leaf.keys.at(loc) == key);

                _ = leaf.keys.remove(loc);
                _ = leaf.vals.remove(loc);

                std.debug.assert(leaf.keys.len == leaf.vals.len);
                std.debug.assert(leaf.keys.isSorted());
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

                std.debug.assert(leaf.keys.isSorted());
                next.destroy(alloc);
            }

            fn get(leaf: *Leaf, key: K) ?*V {
                if (leaf.keys.len == 0) {
                    return null;
                }

                const loc = leaf.keys.lowerBound(key);
                if (loc < leaf.keys.len and leaf.keys.at(loc) == key) {
                    return leaf.vals.ptr(loc);
                }

                return null;
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
                    root.keys.pushBack(lp(storage.root).keys.front());
                    root.keys.pushBack(next.keys.front());

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            } else {
                const overflow = try np(storage.root).add(storage.alloc, storage.height, key, val);
                if (overflow) {
                    const root = try Node.create(storage.alloc);
                    errdefer root.destroy(storage.alloc, 1);
                    const next = try np(storage.root).split(storage.alloc);

                    root.children.pushBack(storage.root);
                    root.children.pushBack(@intFromPtr(next));
                    root.keys.pushBack(np(storage.root).keys.front());
                    root.keys.pushBack(next.keys.front());

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

        pub fn get(storage: *Self, key: K) ?*V {
            if (storage.root == 0) {
                return null;
            }

            if (storage.height == 0) {
                return lp(storage.root).get(key);
            } else {
                return np(storage.root).get(storage.height, key);
            }
        }

        fn getLeaf(storage: *Self, key: K) ?*Leaf {
            if (storage.root == 0) {
                return null;
            }

            if (storage.height == 0) {
                return lp(storage.root);
            } else {
                return np(storage.root).getLeaf(storage.height, key);
            }
        }

        pub inline fn has(storage: *Self, key: u32) bool {
            return storage.get(key) != null;
        }

        const Iterator = struct {
            const KV = struct { key: K, val: V };

            leaf: ?*Leaf,
            cursor: usize = 0,

            pub fn next(it: *Iterator) ?KV {
                if (it.leaf == null) {
                    return null;
                }

                const result = KV{
                    .key = it.leaf.?.keys.at(@intCast(it.cursor)),
                    .val = it.leaf.?.vals.at(@intCast(it.cursor)),
                };

                it.cursor += 1;
                while (it.leaf != null and it.cursor == it.leaf.?.keys.len) {
                    it.cursor = 0;
                    it.leaf = it.leaf.?.next;
                }

                return result;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            if (storage.height == 0) {
                return .{ .leaf = lp(storage.root) };
            }

            var walk = storage.root;
            var h = storage.height;
            while (h > 0) : (h -= 1) {
                walk = np(walk).children.front();
            }
            return .{ .leaf = lp(walk) };
        }

        const Query = struct {
            storage: *Self,
            leaf: ?*Leaf,

            pub fn get(q: *Query, key: K) ?*V {
                if (q.leaf) |leaf| {
                    const result = leaf.get(key);
                    if (result != null) {
                        return result;
                    }
                    q.leaf = null;
                }

                q.leaf = q.storage.getLeaf(key);
                if (q.leaf) |leaf| {
                    return leaf.get(key);
                }
                return null;
            }
        };

        // returns a struct that can be used for more efficient get operations
        // assuming spatio-temporal locality
        pub fn query(storage: *Self) Query {
            return .{ .storage = storage, .leaf = null };
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

    std.debug.print("{}\n", .{@sizeOf(@TypeOf(s).Node)});
    std.debug.print("{}\n", .{@sizeOf(@TypeOf(s).Leaf)});

    const n = 16;
    for (0..n) |i| {
        try s.add(@intCast(i * 2_654_435_761 % n), @floatFromInt(i));
    }
    s.debugPrint();

    for (0..n) |i| {
        std.debug.assert(s.get(@intCast(i * 2_654_435_761 % n)).?.* == @as(f32, @floatFromInt(i)));
    }

    for (0..n) |i| {
        if (i % 2 == 0) continue;
        s.del(@intCast(i * 2_654_435_761 % n));
    }

    for (0..n) |i| {
        if (i % 2 == 0) {
            std.debug.assert(s.get(@intCast(i * 2_654_435_761 % n)).?.* == @as(f32, @floatFromInt(i)));
        } else {
            std.debug.assert(s.get(@intCast(i * 2_654_435_761 % n)) == null);
        }
    }
    s.debugPrint();

    // some fuzz tests just to see that we don't hit a crash/assert
    std.debug.print("\ncommencing fuzz tests!\n", .{});

    const fuzz_lim = 16 * 1024; // must be power of two for the weyl sequence
    const fuzz_n = 31;
    const fuzz_m = 15;
    {
        var _s = Storage(u32, f32, fuzz_n, fuzz_m).init(alloc);
        defer _s.deinit();

        for (0..fuzz_lim) |i| {
            try _s.add(@intCast(i), @floatFromInt(i));
        }

        for (0..fuzz_lim) |i| {
            std.debug.assert(_s.get(@intCast(i)).?.* == @as(f32, @floatFromInt(i)));
        }

        for (0..fuzz_lim) |i| {
            _ = _s.del(@intCast(i));
        }
    }

    {
        var _s = Storage(u32, f32, fuzz_n, fuzz_m).init(alloc);
        defer _s.deinit();

        for (0..fuzz_lim) |i| {
            try _s.add(fuzz_lim - @as(u32, @intCast(i)), @floatFromInt(i));
        }

        for (0..fuzz_lim) |i| {
            std.debug.assert(_s.get(@intCast(fuzz_lim - i)).?.* == @as(f32, @floatFromInt(i)));
        }

        for (0..fuzz_lim) |i| {
            _ = _s.del(fuzz_lim - @as(u32, @intCast(i)));
        }
    }

    {
        var _s = Storage(u32, f32, fuzz_n, fuzz_m).init(alloc);
        defer _s.deinit();

        var x: u32 = 0;
        for (0..fuzz_lim) |i| {
            try _s.add(x % fuzz_lim, @floatFromInt(i));
            x +%= 2_654_435_761; // prime
        }

        x = 0;
        for (0..fuzz_lim) |i| {
            std.debug.assert(_s.get(x % fuzz_lim).?.* == @as(f32, @floatFromInt(i)));
            x +%= 2_654_435_761;
        }

        x = 0;
        for (0..fuzz_lim) |_| {
            _ = _s.del(x % fuzz_lim);
            x +%= 2_654_435_761;
        }
    }

    std.debug.print("fuzz tests finished!\n", .{});
}

test "fuzz" {
    var s = Storage(u32, u32, 31, 15).init(std.testing.allocator);
    defer s.deinit();

    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    for (0..100_000) |_| {
        const k = rng.random().int(u16);
        if (s.get(k)) |v| {
            try std.testing.expectEqual(k, v.*);
            s.del(k);
        } else {
            try s.add(k, k);
        }
    }

    if (s.len > 1) {
        var it = s.iterator();
        var x = it.next().?.key;
        var j: usize = 1;
        while (it.next()) |y| {
            try std.testing.expect(x < y.key);
            x = y.key;
            j += 1;
        }
        try std.testing.expectEqual(s.len, j);
    }

    for (0..100_000) |_| {
        const k = rng.random().int(u16);
        if (s.get(k)) |v| {
            try std.testing.expectEqual(k, v.*);
            s.del(k);
        } else {
            try s.add(k, k);
        }
    }

    if (s.len > 1) {
        var it = s.iterator();
        var x = it.next().?.key;
        var j: usize = 1;
        while (it.next()) |y| {
            try std.testing.expect(x < y.key);
            x = y.key;
            j += 1;
        }
        try std.testing.expectEqual(s.len, j);
    }
}
