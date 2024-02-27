const std = @import("std");

// another database index structure is the b+tree
// and it's pretty neat and a lot simpler than the cst-tree
// in particular the balancing kinda comes natural (i skipped it in the t-trees for simplicity)

// this is not going to be very space efficient
// because it's just so much easier if there's space preallocated for any splits during insertion

/// assumes that the list doesn't contain duplicates
fn lowerBound(data: []const u32, key: u32) u32 {
    var left: u32 = 0;
    var right: u32 = @intCast(data.len);
    while (left < right) {
        const mid = left + (right - left) / 2;
        if (key == data[mid]) {
            return mid;
        } else if (key < data[mid]) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    return left;

    // below is faster, but less safe
    // TODO benchmark once data structure is finished
    // var left: u32 = 0;
    // var right: u32 = @as(u32, @intCast(data.len));
    // while (left < right) {
    //     const mid = (left +% right) / 2;
    //     if (data[mid] >= key) {
    //         right = mid;
    //     } else {
    //         left = mid +% 1;
    //     }
    // }
    // return left;
}

test "lowerBound" {
    const a: [10]u32 = .{ 0, 1, 3, 4, 5, 65, 78, 81, 910, 1000 };

    try std.testing.expectEqual(0, lowerBound(&a, 0));
    try std.testing.expectEqual(1, lowerBound(&a, 1));
    try std.testing.expectEqual(2, lowerBound(&a, 2));
    try std.testing.expectEqual(2, lowerBound(&a, 3));
    try std.testing.expectEqual(3, lowerBound(&a, 4));
    try std.testing.expectEqual(4, lowerBound(&a, 5));
    try std.testing.expectEqual(5, lowerBound(&a, 6));

    try std.testing.expectEqual(5, lowerBound(&a, 64));
    try std.testing.expectEqual(5, lowerBound(&a, 65));
    try std.testing.expectEqual(6, lowerBound(&a, 66));

    try std.testing.expectEqual(6, lowerBound(&a, 77));
    try std.testing.expectEqual(6, lowerBound(&a, 78));
    try std.testing.expectEqual(7, lowerBound(&a, 79));

    try std.testing.expectEqual(7, lowerBound(&a, 80));
    try std.testing.expectEqual(7, lowerBound(&a, 81));
    try std.testing.expectEqual(8, lowerBound(&a, 82));

    try std.testing.expectEqual(8, lowerBound(&a, 909));
    try std.testing.expectEqual(8, lowerBound(&a, 910));
    try std.testing.expectEqual(9, lowerBound(&a, 911));

    try std.testing.expectEqual(9, lowerBound(&a, 999));
    try std.testing.expectEqual(9, lowerBound(&a, 1000));
    try std.testing.expectEqual(10, lowerBound(&a, 1001));
}

pub fn Storage(
    comptime T: type,
    comptime NODE_SIZE: comptime_int, // max number of children for an internal node
    comptime LEAF_SIZE: comptime_int, // max number of entries in a leaf node
) type {
    return struct {
        const Self = @This();

        // message to caller for dealing with the consequences of inserting into a node or leaf
        const InsEff = enum {
            none,
            move_prev,
            move_next,
            split,
        };

        const Node = struct {
            // leave space for one extra entry for easy inserting
            keys: [NODE_SIZE]u32 align(64),
            children: [NODE_SIZE + 1]usize, // ?*Node or ?*Leaf

            prev: ?*Node,
            next: ?*Node,

            len: u32,

            fn create(alloc: std.mem.Allocator) !*Node {
                var node = try alloc.create(Node);
                node.keys = undefined;
                node.children = undefined;
                node.prev = null;
                node.next = null;
                node.len = 0;
                return node;
            }

            fn destroy(node: *Node, alloc: std.mem.Allocator, height: u32) void {
                _ = node;
                _ = alloc;
                _ = height;
            }

            fn add(node: *Node, alloc: std.mem.Allocator, height: u32, key: u32, val: T) !InsEff {
                const i = lowerBound(node.keys[0 .. node.len - 1], key);

                if (height == 1) {
                    const effect = lp(node.children[i]).add(key, val);
                    switch (effect) {
                        .none => {
                            if (i > 0) {
                                node.keys[i - 1] = lp(node.children[i]).keys[0];
                            }
                            return .none; // early exit as insertion only affected this leaf
                        },
                        .move_prev => {
                            @panic("TODO node add move prev");
                        },
                        .move_next => {
                            // move right can trigger more move rights
                            for (i..node.len - 1) |j| {
                                node.keys[j] = lp(node.children[j + 1]).keys[0];
                            }
                            return .none;
                        },
                        .split => {
                            // make space for the split
                            std.mem.copyBackwards(
                                u32,
                                node.keys[i..node.len],
                                node.keys[i - 1 .. node.len - 1],
                            );
                            std.mem.copyBackwards(
                                usize,
                                node.children[i + 1 .. node.len + 1],
                                node.children[i..node.len],
                            );

                            const next = try lp(node.children[i]).split(alloc);
                            node.children[i + 1] = @intFromPtr(next);
                            node.keys[i] = next.keys[0];
                            node.len += 1;
                        },
                    }
                } else {
                    @panic("TODO add to a node with node children");
                }

                if (node.len <= NODE_SIZE) {
                    return .none;
                }

                // out of space, do something about it!
                // try to move our lowest element to the left
                if (node.prev != null and node.prev.?.len < NODE_SIZE) {
                    @panic("TODO node add move next");
                    // const displaced_child = node.children[0];
                    // std.mem.copyForwards(u32, node.keys[0..], node.keys[1..node.len]);
                    // std.mem.copyForwards(T, node.children[0..], node.children[1..node.len]);
                    // node.len -= 1;

                    // // node.prev.?.keys[node.prev.?.len - 1] = node.children[0];
                    // node.prev.?.children[node.prev.?.len] = displaced_child;
                    // node.prev.?.len += 1;

                    // return .move_prev;
                }
                // try to move our highest element to the right
                if (node.next != null and node.next.?.len < NODE_SIZE) {
                    @panic("TODO node add move next");
                    // const displaced_child = node.children[node.len - 1];
                    // node.len -= 1;

                    // return .move_next;
                }

                return .split;
            }

            /// splits node into two, and returns a pointer to next one in order (new one)
            fn split(node: *Node, alloc: std.mem.Allocator) !*Node {
                const next = try Node.create(alloc);

                const keyhalf = NODE_SIZE / 2;
                const half = (NODE_SIZE + 1) / 2;
                @memcpy(
                    next.keys[0..keyhalf],
                    node.keys[keyhalf .. 2 * keyhalf],
                );
                @memcpy(
                    next.children[0..half],
                    node.children[half .. NODE_SIZE + 1],
                );
                node.len = half;
                next.len = NODE_SIZE + 1 - half;
                node.next = next;
                next.prev = node;

                return next;
            }

            fn _debugPrint(node: *Node, height: u32) void {
                std.debug.assert(height > 0);
                std.debug.print(" {}   [ ", .{node.len});
                for (0..node.len - 1) |i| {
                    std.debug.print("{} ", .{node.keys[i]});
                }
                std.debug.print("] at height {} \n", .{height});

                if (height == 1) {
                    for (0..node.len) |i| {
                        lp(node.children[i])._debugPrint();
                    }
                } else {
                    for (0..node.len) |i| {
                        np(node.children[i])._debugPrint(height - 1);
                    }
                }
            }
        };

        const Leaf = struct {
            // leave space for one extra entry for easy inserting
            keys: [LEAF_SIZE + 1]u32 align(64),
            vals: [LEAF_SIZE + 1]T,

            prev: ?*Leaf,
            next: ?*Leaf,

            len: u32,

            fn create(alloc: std.mem.Allocator) !*Leaf {
                var leaf = try alloc.create(Leaf);
                leaf.keys = undefined;
                leaf.vals = undefined;
                leaf.prev = null;
                leaf.next = null;
                leaf.len = 0;
                return leaf;
            }

            fn destroy(leaf: *Leaf, alloc: std.mem.Allocator) void {
                _ = leaf;
                _ = alloc;
            }

            fn add(leaf: *Leaf, key: u32, val: T) InsEff {
                const i = lowerBound(leaf.keys[0..leaf.len], key);

                if (i < leaf.len) {
                    std.mem.copyBackwards(
                        u32,
                        leaf.keys[i + 1 .. leaf.len + 1],
                        leaf.keys[i..leaf.len],
                    );
                    std.mem.copyBackwards(
                        T,
                        leaf.vals[i + 1 .. leaf.len + 1],
                        leaf.vals[i..leaf.len],
                    );
                }
                leaf.keys[i] = key;
                leaf.vals[i] = val;
                leaf.len += 1;

                if (leaf.len <= LEAF_SIZE) {
                    return .none;
                }

                // out of space, do something about it!
                // try to move our lowest element to the left
                if (leaf.prev != null and leaf.prev.?.len < LEAF_SIZE) {
                    const displaced_key = leaf.keys[0];
                    const displaced_val = leaf.vals[0];
                    std.mem.copyForwards(u32, leaf.keys[0..], leaf.keys[1..leaf.len]);
                    std.mem.copyForwards(T, leaf.vals[0..], leaf.vals[1..leaf.len]);
                    leaf.len -= 1;
                    const effect = leaf.prev.?.add(displaced_key, displaced_val);
                    std.debug.assert(effect == .none);
                    return .move_prev;
                }
                // try to move our highest element to the right
                if (leaf.next != null and leaf.next.?.len < LEAF_SIZE) {
                    leaf.len -= 1;
                    const displaced_key = leaf.keys[leaf.len];
                    const displaced_val = leaf.vals[leaf.len];
                    const effect = leaf.next.?.add(displaced_key, displaced_val);
                    std.debug.assert(effect == .none);
                    return .move_next;
                }

                return .split;
            }

            /// splits leaf into two, and returns a pointer to next one in order (new one)
            fn split(leaf: *Leaf, alloc: std.mem.Allocator) !*Leaf {
                const next = try Leaf.create(alloc);

                const half = (LEAF_SIZE + 1) / 2;
                @memcpy(
                    next.keys[0 .. half + 1],
                    leaf.keys[half .. LEAF_SIZE + 1],
                );
                @memcpy(
                    next.vals[0 .. half + 1],
                    leaf.vals[half .. LEAF_SIZE + 1],
                );
                leaf.len = half;
                next.len = LEAF_SIZE + 1 - half;
                leaf.next = next;
                next.prev = leaf;

                return next;
            }

            fn _debugPrint(leaf: *Leaf) void {
                std.debug.print("   {} {{ ", .{leaf.len});
                for (0..leaf.len) |i| {
                    std.debug.print("{}: {}, ", .{ leaf.keys[i], leaf.vals[i] });
                }
                std.debug.print("}}\n", .{});
            }
        };

        const Iterator = struct {
            const KV = struct { key: u32, val: T };

            pub fn next() ?*KV {
                @compileError("TODO");
            }
        };

        alloc: std.mem.Allocator,
        root: usize, // ?*Node or ?*Leaf
        height: u32, // a height of 0 -> only a leaf

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

        fn add(storage: *Self, key: u32, val: T) !void {
            if (storage.root == 0) {
                const root = try Leaf.create(storage.alloc);
                storage.root = @intFromPtr(root);
            }

            if (storage.height == 0) {
                const effect = lp(storage.root).add(key, val);
                switch (effect) {
                    .none => {}, // no response needed
                    .split => {
                        const next = try lp(storage.root).split(storage.alloc);
                        const root = try Node.create(storage.alloc);
                        root.children[0] = storage.root;
                        root.children[1] = @intFromPtr(next);
                        root.keys[0] = next.keys[0];
                        root.len = 2;

                        storage.root = @intFromPtr(root);
                        storage.height += 1;
                    },
                    else => unreachable,
                }
            } else {
                const effect = try np(storage.root).add(storage.alloc, storage.height, key, val);
                switch (effect) {
                    .none => {},
                    .split => {
                        const next = try np(storage.root).split(storage.alloc);
                        const root = try Node.create(storage.alloc);
                        root.children[0] = storage.root;
                        root.children[1] = @intFromPtr(next);
                        root.keys[0] = next.keys[0];
                        root.len = 2;

                        storage.root = @intFromPtr(root);
                        storage.height += 1;
                    },
                    else => unreachable,
                }
            }
        }

        fn del(storage: *Self, key: u32) void {
            _ = key;
            std.debug.assert(storage.height >= 0);
            @compileError("TODO");
        }

        fn get(storage: *Self, key: u32) ?*T {
            _ = storage;
            _ = key;
            @compileError("TODO");
        }

        inline fn has(storage: *Self, key: u32) bool {
            return storage.get(key) != null;
        }

        fn iterator(storage: *Self) Iterator {
            _ = storage;
            @compileError("TODO");
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
            std.debug.print("Storage <{s}> \n", .{@typeName(T)});
            if (storage.root == 0) return;

            if (storage.height == 0) {
                lp(storage.root)._debugPrint();
            } else {
                np(storage.root)._debugPrint(storage.height);
            }
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(usize, 3, 4).init(alloc);
    defer s.deinit();

    var i: u32 = 0;
    for (0..20) |j| {
        std.debug.print("\ninserting {}: {}\n", .{ i, j });
        try s.add(i, j);
        s.debugPrint();
        i = (i + 40507) % 256;
    }
}

// legacy

pub fn OLDStorage(
    comptime T: type,
    comptime NODE_SIZE: comptime_int, // max number of children for an internal node
    comptime LEAF_SIZE: comptime_int, // max number of entries in a leaf node
) type {
    return struct {
        const Self = @This();

        const Node = struct {
            keys: [NODE_SIZE]u32 align(64),
            children: usize, // ?*NodePage or ?*LeafPage
            len: u32,

            fn init() Node {
                return .{
                    .keys = undefined,
                    .children = 0,
                    .len = 0,
                };
            }

            fn destroy(node: *Node, alloc: std.mem.Allocator, height: u32) void {
                std.debug.assert(height > 0);
                std.debug.assert(node.len > 0);
                if (height == 1) {
                    alloc.destroy(lp(node.children));
                } else {
                    for (0..node.len) |i| {
                        np(node.children).nodes[i].destroy(alloc, height - 1);
                    }
                    alloc.destroy(np(node.children));
                }
            }

            fn add(node: *Node, alloc: std.mem.Allocator, height: u32, key: u32, val: T) !bool {
                std.debug.assert(height > 0);
                // find where into this node our data would fit
                const i = lowerBound(node.keys[0 .. node.len - 1], key);

                if (height == 1) {
                    const split = lp(node.children).leaves[i].add(key, val);
                    if (split) {
                        // we need to make room in the LeafNode child array for the expansion
                        // then split the full node into that new space
                        // reconnect all the pointers
                        // and finally, update our array of keys

                        std.mem.copyBackwards(
                            Leaf,
                            lp(node.children).leaves[i + 1 .. node.len + 1],
                            lp(node.children).leaves[i..node.len],
                        );
                        std.mem.copyBackwards(
                            u32,
                            node.keys[i..node.len],
                            node.keys[i - 1 .. node.len - 1],
                        );

                        lp(node.children).leaves[i + 1] = Leaf.init();
                        const half = (LEAF_SIZE + 1) / 2;
                        @memcpy(
                            lp(node.children).leaves[i + 1].keys[0 .. half + 1],
                            lp(node.children).leaves[i].keys[half .. LEAF_SIZE + 1],
                        );
                        @memcpy(
                            lp(node.children).leaves[i + 1].vals[0 .. half + 1],
                            lp(node.children).leaves[i].vals[half .. LEAF_SIZE + 1],
                        );
                        lp(node.children).leaves[i].len = half;
                        lp(node.children).leaves[i + 1].len = LEAF_SIZE + 1 - half;
                        lp(node.children).leaves[i].next = &lp(node.children).leaves[i + 1];
                        lp(node.children).leaves[i + 1].prev = &lp(node.children).leaves[i];

                        if (i > 0) {
                            lp(node.children).leaves[i].prev = &lp(node.children).leaves[i - 1];
                            lp(node.children).leaves[i - 1].next = &lp(node.children).leaves[i];
                        }
                        if (i < NODE_SIZE - 1) {
                            lp(node.children).leaves[i + 1].next = &lp(node.children).leaves[i + 2];
                            lp(node.children).leaves[i + 2].prev = &lp(node.children).leaves[i + 1];
                        }

                        if (i > 0) {
                            node.keys[i - 1] = lp(node.children).leaves[i].keys[0];
                        }
                        node.keys[i] = lp(node.children).leaves[i + 1].keys[0];
                        node.len += 1;
                    } else {
                        if (i > 0) {
                            node.keys[i - 1] = lp(node.children).leaves[i].keys[0];
                        }
                        node.keys[i] = lp(node.children).leaves[i + 1].keys[0];
                    }
                } else {
                    @panic("TODO: add to a node with node children");
                }
                _ = alloc;

                return node.len > NODE_SIZE;
            }

            fn _debugPrint(node: *Node, height: u32) void {
                std.debug.assert(height > 0);
                std.debug.print("  {} [ ", .{node.len});
                for (0..node.len - 1) |i| {
                    std.debug.print("{} ", .{node.keys[i]});
                }
                std.debug.print("] at height {} \n", .{height});

                if (height == 1) {
                    for (0..node.len) |i| {
                        lp(node.children).leaves[i]._debugPrint();
                    }
                } else {
                    for (0..node.len) |i| {
                        np(node.children).nodes[i]._debugPrint(height - 1);
                    }
                }
            }
        };

        // leave space for one extra node if we need to split during insertion
        const NodePage = struct { nodes: [NODE_SIZE + 1]Node };

        const Leaf = struct {
            // leave space for one extra entry for easy inserting
            keys: [LEAF_SIZE + 1]u32 align(64),
            vals: [LEAF_SIZE + 1]T,
            len: u32,

            prev: ?*@This(),
            next: ?*@This(),

            fn init() Leaf {
                return .{
                    .keys = undefined,
                    .vals = undefined,
                    .len = 0,
                    .prev = null,
                    .next = null,
                };
            }

            fn add(leaf: *Leaf, key: u32, val: T) bool {
                // find where into this node our data would fit
                const i = lowerBound(leaf.keys[0..leaf.len], key);

                // there is always space to insert
                // (and then we deal with the consequences later)
                if (i < leaf.len) {
                    std.mem.copyBackwards(
                        u32,
                        leaf.keys[i + 1 .. leaf.len + 1],
                        leaf.keys[i..leaf.len],
                    );
                    std.mem.copyBackwards(
                        T,
                        leaf.vals[i + 1 .. leaf.len + 1],
                        leaf.vals[i..leaf.len],
                    );
                }
                leaf.keys[i] = key;
                leaf.vals[i] = val;
                leaf.len += 1;

                if (leaf.len <= LEAF_SIZE) {
                    return false;
                }

                // out of space, do something about it!
                // try to move our lowest element to the left
                if (leaf.prev != null and leaf.prev.?.len < LEAF_SIZE) {
                    const displaced_key = leaf.keys[0];
                    const displaced_val = leaf.vals[0];
                    std.mem.copyForwards(u32, leaf.keys[0..], leaf.keys[1..leaf.len]);
                    std.mem.copyForwards(T, leaf.vals[0..], leaf.vals[1..leaf.len]);
                    leaf.len -= 1;
                    return @call(
                        .always_tail,
                        Leaf.add,
                        .{ leaf.prev.?, displaced_key, displaced_val },
                    );
                }
                // try to move our highest element to the right
                if (leaf.next != null and leaf.next.?.len < LEAF_SIZE) {
                    leaf.len -= 1;
                    const displaced_key = leaf.keys[leaf.len];
                    const displaced_val = leaf.vals[leaf.len];
                    return @call(
                        .always_tail,
                        Leaf.add,
                        .{ leaf.next.?, displaced_key, displaced_val },
                    );
                }

                // we need to split, but we can't do it ourselves
                // so return 1 and let the caller deal with it
                return true;
            }

            fn _debugPrint(leaf: *Leaf) void {
                std.debug.print("  {} {{ ", .{leaf.len});
                for (0..leaf.len) |i| {
                    std.debug.print("{}: {}, ", .{ leaf.keys[i], leaf.vals[i] });
                }
                std.debug.print("}}\n", .{});
            }
        };

        // leave space for one extra leaf if we need to split during insertion
        const LeafPage = struct { leaves: [NODE_SIZE + 1]Leaf };

        const Iterator = struct {
            const KV = struct { key: u32, val: T };

            pub fn next() ?*KV {
                @compileError("TODO");
            }
        };

        alloc: std.mem.Allocator,
        root: usize, // ?*NodePage or ?*LeafPage
        height: u32, // a height of 0 -> only a leaf

        // Whether the root is a NodePage or a LeafPage depends on the height
        // we waste stome space by using a page for the root
        // since it means that when we need to grow the tree there's no need to move anything

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
                    storage.alloc.destroy(lp(storage.root));
                } else {
                    np(storage.root).nodes[0].destroy(storage.alloc, storage.height);
                    storage.alloc.destroy(np(storage.root));
                }
            }
            storage.* = undefined;
        }

        fn add(storage: *Self, key: u32, val: T) !void {
            if (storage.root == 0) {
                const root = try storage.alloc.create(LeafPage);
                root.leaves[0] = Leaf.init();
                storage.root = @intFromPtr(root);
            }

            if (storage.height == 0) {
                const split = lp(storage.root).leaves[0].add(key, val);
                if (split) {
                    // note the hardcoded (0, 1) split, since this only happens in one case
                    // split the leaves
                    lp(storage.root).leaves[1] = Leaf.init();
                    const half = (LEAF_SIZE + 1) / 2;
                    @memcpy(
                        lp(storage.root).leaves[1].keys[0 .. half + 1],
                        lp(storage.root).leaves[0].keys[half .. LEAF_SIZE + 1],
                    );
                    @memcpy(
                        lp(storage.root).leaves[1].vals[0 .. half + 1],
                        lp(storage.root).leaves[0].vals[half .. LEAF_SIZE + 1],
                    );
                    lp(storage.root).leaves[0].len = half;
                    lp(storage.root).leaves[1].len = LEAF_SIZE + 1 - half;
                    lp(storage.root).leaves[0].next = &lp(storage.root).leaves[1];
                    lp(storage.root).leaves[1].prev = &lp(storage.root).leaves[0];

                    // and then make a new node that gets to be the root
                    const root = try storage.alloc.create(NodePage);
                    root.nodes[0] = Node.init();
                    root.nodes[0].children = storage.root;
                    root.nodes[0].keys[0] = lp(storage.root).leaves[1].keys[0];
                    root.nodes[0].len = 2;

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            } else {
                const split = try np(
                    storage.root,
                ).nodes[0].add(storage.alloc, storage.height, key, val);
                if (split) {
                    // note the hardcoded (0, 1) split, since this only happens in one case
                    np(storage.root).nodes[1] = Node.init();
                    const half = (NODE_SIZE + 1) / 2;
                    @memcpy( // MIGHT BE WRONG?
                        np(storage.root).nodes[1].keys[0 .. half - 1],
                        np(storage.root).nodes[0].keys[half..NODE_SIZE],
                    );
                    np(storage.root).nodes[0].len = half;
                    np(storage.root).nodes[1].len = NODE_SIZE + 1 - half;

                    const root = try storage.alloc.create(NodePage);
                    root.nodes[0] = Node.init();
                    root.nodes[0].children = storage.root;
                    root.nodes[0].keys[0] = np(storage.root).nodes[1].keys[0];
                    root.nodes[0].len = 2;

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            }
        }

        fn del(storage: *Self, key: u32) void {
            _ = key;
            std.debug.assert(storage.height >= 0);
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

        fn np(ptr: usize) *NodePage {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn lp(ptr: usize) *LeafPage {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s}> \n", .{@typeName(T)});
            if (storage.root == 0) return;

            if (storage.height == 0) {
                lp(storage.root).leaves[0]._debugPrint();
            } else {
                np(storage.root).nodes[0]._debugPrint(storage.height);
            }
        }
    };
}
