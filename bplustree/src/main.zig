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

            fn _debugPrint(node: *Node, height: u32) void {
                std.debug.assert(height > 0);
                std.debug.print("  [ ", .{});
                for (0..node.len) |i| {
                    std.debug.print("{}, ", .{node.keys[i]});
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

            fn add(leaf: *Leaf, key: u32, val: T) !bool {
                // find where into this node our data would fit
                const i = lowerBound(leaf.keys[0..leaf.len], key);

                // there is always space to insert
                // (and then we deal with the consequences later)
                if (i < leaf.len) {
                    std.mem.copyBackwards(
                        u32,
                        leaf.keys[i + 1 .. leaf.len],
                        leaf.keys[i .. leaf.len - 1],
                    );
                    std.mem.copyBackwards(
                        T,
                        leaf.vals[i + 1 .. leaf.len],
                        leaf.vals[i .. leaf.len - 1],
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
                std.debug.print("  {{ ", .{});
                for (0..leaf.len) |i| {
                    std.debug.print("{}: {}, ", .{ leaf.keys[i], leaf.vals[0] });
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
                const split = try lp(storage.root).leaves[0].add(key, val);
                if (split) {
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
                    lp(storage.root).leaves[0].vals[0] = half;
                    lp(storage.root).leaves[1].vals[0] = LEAF_SIZE + 1 - half;
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
                //
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
            std.debug.print("Storage: \n", .{});
            if (storage.root == 0) return;

            if (storage.height == 0) {
                lp(storage.root).leaves[0]._debugPrint();
            } else {
                np(storage.root).nodes[0]._debugPrint(storage.height);
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
    for (0..10) |j| {
        try s.add(i, j);
        s.debugPrint();
        i = (i + 40507) % 256;
    }
}

// fn Internal(comptime T: type) type {
//     // const MAX = ISIZE / 2;

//     return struct {
//         const Self = @This();

//         keys: [ISIZE]u32 align(64),
//         children: [ISIZE]usize,
//         len: u32,

//         fn create(alloc: std.mem.Allocator) !*Self {
//             var node = try alloc.create(Self);
//             node.keys = .{0} ** ISIZE;
//             node.len = 0;
//             node.children = .{null} ** ISIZE;
//             return node;
//         }

//         fn destroy(node: *Self, alloc: std.mem.Allocator, height: i32) void {
//             std.debug.assert(height > 0);
//             for (node.children) |child| {
//                 if (child == 0) continue;
//                 if (height == 2) {
//                     lptr(child).?.destroy(alloc);
//                 } else {
//                     iptr(child).?.destroy(alloc, height - 1);
//                 }
//             }
//             alloc.destroy(node);
//         }

//         fn iptr(ptr: usize) ?*Self {
//             return @ptrFromInt(ptr);
//         }

//         fn lptr(ptr: usize) ?*Leaf(T) {
//             return @ptrFromInt(ptr);
//         }
//     };
// }

// comptime {
//     if (ISIZE == 5) {
//         std.debug.assert(@sizeOf(Internal(void)) == 0);
//     }
// }

// fn Leaf(comptime T: type) type {
//     // const MAX = LSIZE / 2;

//     return struct {
//         const Self = @This();

//         keys: [LSIZE]u32 align(64),
//         vals: [LSIZE]T,
//         len: u32,

//         prev: ?*Self,
//         next: ?*Self,

//         fn create(alloc: std.mem.Allocator) !*Self {
//             var node = try alloc.create(Self);
//             node.keys = .{0} ** LSIZE;
//             node.len = 0;
//             node.prev = null;
//             node.next = null;
//             return node;
//         }

//         fn destroy(node: *Self, alloc: std.mem.Allocator) void {
//             alloc.destroy(node);
//         }

//         fn add(node: *Self, alloc: std.mem.Allocator, key: u32, val: T) !i32 {
//             // find where into this node our data would fit
//             var i: u32 = 0;
//             while (i < node.len) : (i += 1) {
//                 if (node.keys[i] >= key) break;
//             }

//             if (node.len + 1 < LSIZE) {
//                 std.mem.copyBackwards(
//                     u32,
//                     node.keys[i + 1 .. node.len],
//                     node.keys[i .. node.len - 1],
//                 );
//                 std.mem.copyBackwards(
//                     u32,
//                     node.keys[i + 1 .. node.len],
//                     node.keys[i .. node.len - 1],
//                 );

//                 node.keys[i] = key;
//                 node.vals[i] = val;
//                 node.len += 1;
//             }

//             _ = alloc;
//         }

//         fn iptr(ptr: usize) ?*Internal(T) {
//             return @ptrFromInt(ptr);
//         }

//         fn lptr(ptr: usize) ?*Self {
//             return @ptrFromInt(ptr);
//         }

//         fn debugPrint(node: *Self) void {
//             std.debug.print("{{ ", .{});
//         }
//     };
// }

// // NOTE I think add, del, get and and Iterator is all that we really need
// fn Storage(comptime T: type) type {
//     return struct {
//         const Self = @This();

//         const Iterator = struct {
//             const KV = struct { key: u32, val: T };

//             pub fn next() ?*KV {
//                 @compileError("TODO");
//             }
//         };

//         alloc: std.mem.Allocator,
//         root: usize,
//         height: i32,

//         pub fn init(alloc: std.mem.Allocator) Self {
//             return .{
//                 .alloc = alloc,
//                 .root = 0,
//                 .height = 0,
//             };
//         }

//         pub fn deinit(storage: *Self) void {
//             if (storage.root != 0) {
//                 if (storage.height == 0) {
//                     lptr(storage.root).?.destroy(storage.alloc);
//                 } else {
//                     iptr(storage.root).?.destroy(storage.alloc, storage.height);
//                 }
//             }
//             storage.* = undefined;
//         }

//         fn add(storage: *Self, key: u32, val: T) !void {
//             std.debug.assert(storage.height >= 0);

//             if (storage.root == 0) {
//                 storage.root = @intFromPtr(try Leaf(T).create(storage.alloc));
//                 storage.height += 1;
//             }

//             if (storage.height == 0) {
//                 storage.height += try lptr(storage.root).?.add(std.mem.Allocator, key, val);
//             } else {
//                 //
//             }

//             std.debug.assert(storage.height > 0);
//         }

//         fn del(storage: *Self, key: u32) void {
//             _ = storage;
//             _ = key;
//             @compileError("TODO");
//         }

//         fn get(storage: *Self, key: u32) ?*T {
//             _ = storage;
//             _ = key;
//             @compileError("TODO");
//         }

//         fn iterator(storage: *Self) Iterator {
//             _ = storage;
//             @compileError("TODO");
//         }

//         fn iptr(ptr: usize) ?*Internal(T) {
//             return @ptrFromInt(ptr);
//         }

//         fn lptr(ptr: usize) ?*Leaf(T) {
//             return @ptrFromInt(ptr);
//         }
//     };
// }

// pub fn main() !void {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     defer _ = gpa.deinit();
//     const alloc = gpa.allocator();

//     var s = Storage(usize).init(alloc);
//     defer s.deinit();

//     var i: u32 = 0;
//     for (0..100) |j| {
//         try s.add(i, j);
//         i = (i + 40507) % 65536;
//     }
// }
