const std = @import("std");
const utl = @import("utils.zig");

pub fn Storage(comptime V: type, comptime LEAF_SIZE: comptime_int) type {
    const K = u32;
    const NODE_SIZE = 48;

    return struct {
        const Self = @This();

        const Node = struct {
            keys: [NODE_SIZE + 1]K = undefined,
            indirect: [NODE_SIZE + 1]u8 = undefined, // child of key i is at children[indirect[i]]
            len: u8 = 0,
            children: usize = 0, // ?* to NodePage or LeafPage

            fn add(node: *Node, alloc: std.mem.Allocator, height: usize, key: K, val: V) !bool {
                const this: u32 = blk: {
                    const loc = utl.upperBound(K, &node.keys, node.len, key);
                    break :blk if (loc > 0) loc - 1 else loc;
                };
                std.debug.assert(node.keys[this] != key);

                if (height == 1) {
                    const this_ptr = &lp(node.children).leaves[node.indirect[this]];
                    const overflow = this_ptr.add(key, val);
                    node.keys[this] = utl.front(K, &this_ptr.keys, this_ptr.len);
                    if (overflow) {
                        const next = node.emptyChild();
                        const next_ptr = &lp(node.children).leaves[next];
                        next_ptr.* = Leaf{};
                        Leaf.split(this_ptr, next_ptr);
                        utl.insert(
                            K,
                            &node.keys,
                            node.len,
                            this + 1,
                            utl.front(K, &next_ptr.keys, next_ptr.len),
                        );
                        utl.insert(u8, &node.indirect, node.len, this + 1, @intCast(next));
                        node.len += 1;
                    }
                } else {
                    const this_ptr = &np(node.children).nodes[node.indirect[this]];
                    const overflow = try this_ptr.add(alloc, height - 1, key, val);
                    node.keys[this] = utl.front(K, &this_ptr.keys, this_ptr.len);
                    if (overflow) {
                        const next = node.emptyChild();
                        const next_ptr = &np(node.children).nodes[next];
                        next_ptr.* = Node{};
                        try Node.split(this_ptr, next_ptr, alloc, height - 1);
                        utl.insert(
                            K,
                            &node.keys,
                            node.len,
                            this + 1,
                            utl.front(K, &next_ptr.keys, next_ptr.len),
                        );
                        utl.insert(u8, &node.indirect, node.len, this + 1, @intCast(next));
                        node.len += 1;
                    }
                }

                return node.len > NODE_SIZE;
            }

            fn split(node: *Node, next: *Node, alloc: std.mem.Allocator, height: usize) !void {
                std.debug.assert(node.len == NODE_SIZE + 1);
                next.* = Node{};

                const half = (NODE_SIZE + 1) / 2;
                @memcpy(next.keys[0 .. node.len - half], node.keys[half..node.len]);

                if (height == 1) {
                    const new_page = try alloc.create(LeafPage);
                    next.children = @intFromPtr(new_page);
                    for (half..node.len) |i| {
                        new_page.leaves[next.len] = lp(node.children).leaves[node.indirect[i]];

                        if (new_page.leaves[next.len].prev != null) {
                            new_page.leaves[next.len].prev.?.next = &new_page.leaves[next.len];
                        }
                        if (new_page.leaves[next.len].next != null) {
                            new_page.leaves[next.len].next.?.prev = &new_page.leaves[next.len];
                        }

                        next.indirect[next.len] = next.len; // splitting sorts, how nice
                        next.len += 1;
                    }
                } else {
                    const new_page = try alloc.create(NodePage);
                    next.children = @intFromPtr(new_page);
                    for (half..node.len) |i| {
                        new_page.nodes[next.len] = np(node.children).nodes[node.indirect[i]];
                        next.indirect[next.len] = next.len;
                        next.len += 1;
                    }
                }
                node.len = half;
            }

            fn get(node: *Node, height: usize, key: K) ?*V {
                const this: u32 = blk: {
                    const loc = utl.upperBound(K, &node.keys, node.len, key);
                    break :blk if (loc > 0) loc - 1 else loc;
                };

                if (height == 1) {
                    return lp(node.children).leaves[node.indirect[this]].get(key);
                } else {
                    const next: usize = @intFromPtr(&np(node.children).nodes[node.indirect[this]]);
                    @prefetch(@as(*usize, @ptrFromInt(next)), .{});
                    @prefetch(@as(*usize, @ptrFromInt(next + 64)), .{});
                    @prefetch(@as(*usize, @ptrFromInt(next + 128)), .{});
                    @prefetch(@as(*usize, @ptrFromInt(next + 192)), .{});
                    return @call(
                        .always_tail,
                        Node.get,
                        .{ &np(node.children).nodes[node.indirect[this]], height - 1, key },
                    );
                }
            }

            fn freeChildren(node: Node, alloc: std.mem.Allocator, height: usize) void {
                if (node.children != 0) {
                    if (height == 1) {
                        alloc.destroy(lp(node.children));
                    } else {
                        for (0..node.len) |i| {
                            np(node.children).nodes[node.indirect[i]].freeChildren(
                                alloc,
                                height - 1,
                            );
                        }
                        alloc.destroy(np(node.children));
                    }
                }
            }

            /// find a slot in the children that is yet occupied (going by the indirection array)
            fn emptyChild(node: Node) u32 {
                var mask = std.StaticBitSet(49).initFull();
                for (node.indirect[0..node.len]) |indirect| {
                    mask.unset(indirect);
                }
                return @intCast(mask.findFirstSet().?);
            }

            fn _debugPrint(node: Node, height: usize, depth: usize) void {
                // FIXME indirection
                std.debug.assert(height > 0);
                std.debug.assert(node.children != 0);
                for (0..depth) |_| std.debug.print("  ", .{});
                std.debug.print("{any} - {any}\n", .{
                    node.keys[0..node.len],
                    node.indirect[0..node.len],
                });

                if (height == 1) {
                    for (node.indirect[0..node.len]) |i| {
                        lp(node.children).leaves[i]._debugPrint(depth + 1);
                    }
                } else {
                    for (node.indirect[0..node.len]) |i| {
                        np(node.children).nodes[i]._debugPrint(height - 1, depth + 1);
                    }
                }
            }
        };

        const NodePage = struct {
            nodes: [NODE_SIZE + 1]Node,
        };

        const Leaf = struct {
            keys: [LEAF_SIZE + 1]K = undefined,
            vals: [LEAF_SIZE + 1]V = undefined,
            len: u32 = 0,
            prev: ?*Leaf = null,
            next: ?*Leaf = null,

            fn add(leaf: *Leaf, key: K, val: V) bool {
                std.debug.assert(leaf.len <= LEAF_SIZE);

                const loc = utl.lowerBound(K, &leaf.keys, leaf.len, key);
                utl.insert(K, &leaf.keys, leaf.len, loc, key);
                utl.insert(V, &leaf.vals, leaf.len, loc, val);
                leaf.len += 1;

                return leaf.len > LEAF_SIZE;
            }

            fn split(leaf: *Leaf, next: *Leaf) void {
                std.debug.assert(leaf.len == LEAF_SIZE + 1);
                next.* = Leaf{};

                const half = (LEAF_SIZE + 1) / 2;
                @memcpy(next.keys[0 .. leaf.len - half], leaf.keys[half..leaf.len]);
                @memcpy(next.vals[0 .. leaf.len - half], leaf.vals[half..leaf.len]);
                next.len = leaf.len - half;
                leaf.len = half;

                // stitch neighbours
                next.next = leaf.next;
                next.prev = leaf;
                leaf.next = next;
                if (next.next != null) {
                    next.next.?.prev = next;
                }
            }

            fn get(leaf: *Leaf, key: K) ?*V {
                if (leaf.len == 0) {
                    return null;
                }

                const loc = utl.lowerBound(K, &leaf.keys, leaf.len, key);
                if (loc < leaf.len and leaf.keys[loc] == key) {
                    return &leaf.vals[loc];
                }

                return null;
            }

            fn _debugPrint(leaf: Leaf, depth: usize) void {
                for (0..depth) |_| std.debug.print("  ", .{});
                std.debug.print("{any}", .{leaf.keys[0..leaf.len]});
                if (leaf.next) |next| {
                    std.debug.print(" -> {any}\n", .{next.keys[0..next.len]});
                } else {
                    std.debug.print(" -> null\n", .{});
                }
            }
        };

        const LeafPage = struct {
            leaves: [NODE_SIZE + 1]Leaf,
        };

        comptime {
            std.debug.assert(@sizeOf(Node) <= 256);
        }

        alloc: std.mem.Allocator,
        // we could compact these a bit, but I don't think it matters much
        indirect: usize, // what slot in the root page is the root node/leaf at
        root: usize,
        height: usize,
        len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .indirect = 0,
                .root = 0,
                .height = 0,
                .len = 0,
            };
        }

        pub fn deinit(storage: *Self) void {
            if (storage.root != 0) {
                if (storage.height == 0) {
                    storage.alloc.destroy(lp(storage.root));
                } else {
                    np(storage.root).nodes[storage.indirect].freeChildren(
                        storage.alloc,
                        storage.height,
                    );
                    storage.alloc.destroy(np(storage.root));
                }
            }
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: K) ?*V {
            if (storage.root == 0) {
                return null;
            }

            if (storage.height == 0) {
                return lp(storage.root).leaves[storage.indirect].get(key);
            } else {
                return np(storage.root).nodes[storage.indirect].get(storage.height, key);
            }
        }

        pub fn add(storage: *Self, key: K, val: V) !void {
            if (storage.root == 0) {
                const root = try storage.alloc.create(LeafPage);
                root.leaves[storage.indirect] = Leaf{};
                storage.root = @intFromPtr(root);
            }

            if (storage.height == 0) {
                const overflow = lp(storage.root).leaves[storage.indirect].add(key, val);
                if (overflow) {
                    const root = try storage.alloc.create(NodePage);

                    // in this instance, since we just created the node page, it's a bit simpler
                    // as we can mostly ignore indirection

                    const next: usize = (storage.indirect + 1) % 2;
                    Leaf.split(
                        &lp(storage.root).leaves[storage.indirect],
                        &lp(storage.root).leaves[next],
                    );

                    root.nodes[0] = Node{};
                    root.nodes[0].children = storage.root;
                    root.nodes[0].indirect[0] = @intCast(storage.indirect);
                    root.nodes[0].indirect[1] = @intCast(next);
                    root.nodes[0].len = 2;
                    root.nodes[0].keys[0] = utl.front(
                        K,
                        &lp(storage.root).leaves[storage.indirect].keys,
                        lp(storage.root).leaves[storage.indirect].len,
                    );
                    root.nodes[0].keys[1] = utl.front(
                        K,
                        &lp(storage.root).leaves[next].keys,
                        lp(storage.root).leaves[next].len,
                    );

                    storage.root = @intFromPtr(root);
                    storage.indirect = 0;
                    storage.height += 1;
                }
            } else {
                const overflow = try np(storage.root).nodes[storage.indirect].add(
                    storage.alloc,
                    storage.height,
                    key,
                    val,
                );
                if (overflow) {
                    const root = try storage.alloc.create(NodePage);
                    const next: usize = (storage.indirect + 1) % 2;
                    try Node.split(
                        &np(storage.root).nodes[storage.indirect],
                        &np(storage.root).nodes[next],
                        storage.alloc,
                        storage.height,
                    );

                    root.nodes[0] = Node{};
                    root.nodes[0].children = storage.root;
                    root.nodes[0].indirect[0] = @intCast(storage.indirect);
                    root.nodes[0].indirect[1] = @intCast(next);
                    root.nodes[0].len = 2;
                    root.nodes[0].keys[0] = utl.front(
                        K,
                        &np(storage.root).nodes[storage.indirect].keys,
                        np(storage.root).nodes[storage.indirect].len,
                    );
                    root.nodes[0].keys[1] = utl.front(
                        K,
                        &np(storage.root).nodes[next].keys,
                        np(storage.root).nodes[next].len,
                    );

                    storage.root = @intFromPtr(root);
                    storage.indirect = 0;
                    storage.height += 1;
                }
            }

            storage.len += 1;
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
                    .key = it.leaf.?.keys[it.cursor],
                    .val = it.leaf.?.vals[it.cursor],
                };

                it.cursor += 1;
                while (it.leaf != null and it.cursor == it.leaf.?.len) {
                    it.cursor = 0;
                    it.leaf = it.leaf.?.next;
                }

                return result;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            if (storage.root == 0) {
                return .{ .leaf = null };
            }
            if (storage.height == 0) {
                return .{ .leaf = &lp(storage.root).leaves[storage.indirect] };
            }

            var walk = &np(storage.root).nodes[storage.indirect];
            var h = storage.height - 1;
            while (h > 0) : (h -= 1) {
                walk = &np(walk.children).nodes[walk.indirect[0]];
            }
            return .{ .leaf = &lp(walk.children).leaves[walk.indirect[0]] };
        }

        fn np(ptr: usize) *NodePage {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn lp(ptr: usize) *LeafPage {
            std.debug.assert(ptr != 0);
            return @ptrFromInt(ptr);
        }

        fn debugPrint(storage: Self) void {
            std.debug.print("Storage <{s} {s}> \n", .{ @typeName(K), @typeName(V) });
            if (storage.root == 0) return;

            if (storage.height == 0) {
                lp(storage.root).leaves[0]._debugPrint(0);
            } else {
                np(storage.root).nodes[0]._debugPrint(storage.height, 0);
            }
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(u32, 3).init(alloc);
    defer s.deinit();

    s.debugPrint();

    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    const n = 32;
    for (0..n) |_| {
        const k = rng.random().int(u32) % n;
        if (s.get(k)) |v| {
            std.debug.print("{} -> {}\n", .{ k, v });
            std.debug.assert(k == v.*);
        } else {
            std.debug.print("\nInserting {}\n", .{k});
            try s.add(k, k);
            _ = s.debugPrint();
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
