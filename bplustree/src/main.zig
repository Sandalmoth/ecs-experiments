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

        const Node = struct {
            // leave space for one extra entry for easy inserting
            keys: [NODE_SIZE]u32 align(64),
            children: [NODE_SIZE + 1]usize, // ?*Node or ?*Leaf

            len: u32,

            fn create(alloc: std.mem.Allocator) !*Node {
                var node = try alloc.create(Node);
                node.keys = undefined;
                node.children = undefined;
                node.len = 0;
                return node;
            }

            fn destroy(node: *Node, alloc: std.mem.Allocator, height: u32) void {
                if (height == 1) {
                    for (node.children[0..node.len]) |child| {
                        std.debug.assert(child != 0);
                        lp(child).destroy(alloc);
                    }
                } else {
                    for (node.children[0..node.len]) |child| {
                        std.debug.assert(child != 0);
                        np(child).destroy(alloc, height - 1);
                    }
                }
                alloc.destroy(node);
            }

            fn add(node: *Node, alloc: std.mem.Allocator, height: u32, key: u32, val: T) !bool {
                const i = lowerBound(node.keys[0 .. node.len - 1], key);

                if (height == 1) {
                    const overflow = lp(node.children[i]).add(key, val);
                    if (overflow) {
                        // make space for the split
                        if (i == 0) {
                            std.mem.copyBackwards(
                                u32,
                                node.keys[i + 1 .. node.len],
                                node.keys[i .. node.len - 1],
                            );
                        } else {
                            std.mem.copyBackwards(
                                u32,
                                node.keys[i..node.len],
                                node.keys[i - 1 .. node.len - 1],
                            );
                        }
                        std.mem.copyBackwards(
                            usize,
                            node.children[i + 1 .. node.len + 1],
                            node.children[i..node.len],
                        );

                        const next = try lp(node.children[i]).split(alloc);
                        node.children[i + 1] = @intFromPtr(next);
                        node.keys[i] = next.keys[0];
                        node.len += 1;
                    }
                } else {
                    const overflow = try np(node.children[i]).add(alloc, height - 1, key, val);
                    if (overflow) {
                        // make space for the split
                        if (i == 0) {
                            std.mem.copyBackwards(
                                u32,
                                node.keys[i + 1 .. node.len],
                                node.keys[i .. node.len - 1],
                            );
                        } else {
                            std.mem.copyBackwards(
                                u32,
                                node.keys[i..node.len],
                                node.keys[i - 1 .. node.len - 1],
                            );
                        }
                        std.mem.copyBackwards(
                            usize,
                            node.children[i + 1 .. node.len + 1],
                            node.children[i..node.len],
                        );

                        const result = try np(node.children[i]).split(alloc);
                        node.children[i + 1] = @intFromPtr(result.next);
                        node.keys[i] = result.pivot;
                        node.len += 1;
                    }
                }

                return (node.len > NODE_SIZE);
            }

            // we need to return both the new node, but also the key that we eliminate
            // as the new node doesnt' track the key for it's lowest child
            // it would otherwise be lost
            const SplitResult = struct {
                next: *Node,
                pivot: u32,
            };
            fn split(node: *Node, alloc: std.mem.Allocator) !SplitResult {
                const half = (NODE_SIZE + 1) / 2;

                const result = .{
                    .next = try Node.create(alloc),
                    .pivot = node.keys[half - 1],
                };

                @memcpy(
                    result.next.keys[0 .. half - 1],
                    node.keys[half..NODE_SIZE],
                );
                @memcpy(
                    result.next.children[0..half],
                    node.children[half .. NODE_SIZE + 1],
                );
                node.len = half;
                result.next.len = NODE_SIZE + 1 - half;

                return result;
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
                alloc.destroy(leaf);
            }

            fn add(leaf: *Leaf, key: u32, val: T) bool {
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

                return (leaf.len > LEAF_SIZE);
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
                const overflow = lp(storage.root).add(key, val);
                if (overflow) {
                    const next = try lp(storage.root).split(storage.alloc);
                    const root = try Node.create(storage.alloc);
                    root.children[0] = storage.root;
                    root.children[1] = @intFromPtr(next);
                    root.keys[0] = next.keys[0];
                    root.len = 2;

                    storage.root = @intFromPtr(root);
                    storage.height += 1;
                }
            } else {
                const overflow = try np(storage.root).add(storage.alloc, storage.height, key, val);
                if (overflow) {
                    const result = try np(storage.root).split(storage.alloc);
                    const root = try Node.create(storage.alloc);
                    root.children[0] = storage.root;
                    root.children[1] = @intFromPtr(result.next);
                    root.keys[0] = result.pivot;
                    root.len = 2;

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
