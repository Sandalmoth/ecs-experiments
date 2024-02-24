const std = @import("std");

// design thoughts
// separate component type storage
// usize entities, never reuse an identifier (possible, but no protection)
// build a T-tree out of pages with entities stored in sorted order
// each page also has the storage for that components
// sorted storage order should improve locality of reference for multi-component iteration
// also designed for a fixed page size to enable use of a pool allocator

// i'll go for an avl tree for the page order
// and each page has an array of values that we insert into
// but there are many other possibilities for the implementation
// like each page could hold and array or a deque or maybe some kind of flat tree
// and the overall tree could be an a red-black, or scapegoat, or any other self-balancing tree

const MEMORY_PAGE_SIZE = 16 * 1024;

pub const Entity = usize;

fn Page(comptime T: type) type {
    comptime {
        const HEADER_SIZE = 48;
        var capacity = (MEMORY_PAGE_SIZE - HEADER_SIZE) / (@sizeOf(Entity) + @sizeOf(T)) + 1;
        var P: type = [MEMORY_PAGE_SIZE + 1]u8; // guaranteed not to fit

        while (@sizeOf(P) > MEMORY_PAGE_SIZE) : (capacity -= 1) {
            P = struct {
                const Self = @This();

                const Header = struct {
                    len: usize,
                    min: Entity,
                    max: Entity,
                    left: ?*Self,
                    right: ?*Self,
                    balance: i32, // for AVL tree balancing?
                    // parent: ?*@This(), // not sure this is needed
                };

                header: Header,
                entities: [capacity]Entity,
                data: [capacity]T,

                /// a t-tree node is invalid without any contents (no min/max)
                /// hence, we require them on init
                fn create(alloc: std.mem.Allocator, entity: Entity, value: T) !*Self {
                    // std.debug.print("create\n", .{});
                    var page = try alloc.create(Self);
                    page.header.len = 1;
                    page.header.min = entity;
                    page.header.max = entity;
                    page.header.left = null;
                    page.header.right = null;
                    page.header.balance = 0; // FIXME TODO how does an AVL tree work?
                    page.entities[0] = entity;
                    page.data[0] = value;
                    return page;
                }

                /// recursively destroys all children
                fn destroy(page: *Self, alloc: std.mem.Allocator) void {
                    if (page.header.left) |left| {
                        left.destroy(alloc);
                    }
                    if (page.header.right) |right| {
                        right.destroy(alloc);
                    }
                    alloc.destroy(page);
                }

                fn isBounding(page: *Self, entity: Entity) bool {
                    return page.header.min <= entity and page.header.max >= entity;
                }

                /// returns either the index of entity
                /// or the index where insertion of entity would preserve sorted order
                fn find(page: *Self, entity: Entity) usize {
                    // just a linear search for now
                    var i: usize = 0;
                    while (i < page.header.len) : (i += 1) {
                        if (page.entities[i] >= entity) {
                            return i;
                        }
                    }
                    return @min(page.header.len, page.entities.len - 1);
                }

                fn isFull(page: *Self) bool {
                    return page.header.len >= page.entities.len;
                }

                /// if entity is already on the page
                /// just overwrites the value
                fn insert(page: *Self, entity: Entity, value: T) void {
                    // std.debug.print("insert\n", .{});
                    std.debug.assert(!page.isFull());

                    const i = page.find(entity);
                    if (page.entities[i] == entity) {
                        page.data[i] = value;
                        return;
                    }

                    // std.debug.print("{}\n", .{i});

                    std.mem.copyBackwards(Entity, page.entities[i + 1 ..], page.entities[i..page.header.len]);
                    std.mem.copyBackwards(T, page.data[i + 1 ..], page.data[i..page.header.len]);
                    page.entities[i] = entity;
                    page.data[i] = value;

                    page.header.len += 1;
                    page.header.min = page.entities[0];
                    page.header.max = page.entities[page.header.len - 1];

                    // std.debug.print("{any}\n", .{page.entities[0..page.header.len]});
                    std.debug.assert(std.sort.isSorted(
                        Entity,
                        page.entities[0..page.header.len],
                        {},
                        std.sort.asc(Entity),
                    ));
                }

                fn erase(page: *Self, entity: Entity) void {
                    std.debug.assert(page.header.len > 2);

                    const i = page.find(entity);
                    if (page.entities[i] != entity) {
                        return;
                    }

                    std.mem.copyForwards(Entity, page.entities[i..], page.entities[i + 1 .. page.header.len]);
                    std.mem.copyForwards(T, page.data[i..], page.data[i + 1 .. page.header.len]);

                    page.header.len -= 1;
                    page.header.min = page.entities[0];
                    page.header.max = page.entities[page.header.len - 1];

                    // std.debug.print("{any}\n", .{page.entities[0..page.header.len]});
                    std.debug.assert(std.sort.isSorted(
                        Entity,
                        page.entities[0..page.header.len],
                        {},
                        std.sort.asc(Entity),
                    ));
                }
            };
        }

        std.debug.assert(@sizeOf(P) <= MEMORY_PAGE_SIZE);
        std.debug.assert(@sizeOf(P.Header) == HEADER_SIZE);

        return P;
    }
}

fn Storage(comptime T: type) type {
    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,
        root: ?*Page(T) = null,

        fn init(alloc: std.mem.Allocator) Self {
            return .{ .alloc = alloc };
        }

        fn deinit(storage: *Self) void {
            if (storage.root) |root| {
                root.destroy(storage.alloc);
            }
            storage.* = undefined;
        }

        /// may need to allocate, hence can fail
        fn add(storage: *Self, entity: Entity, value: T) !void {
            // std.debug.print("[{}]\n", .{entity});

            // if we don't have a root, make one
            if (storage.root == null) {
                storage.root = try Page(T).create(storage.alloc, entity, value);
                return;
            }

            var page = storage.root.?;
            while (true) {
                if (page.isBounding(entity)) {
                    if (page.isFull()) {
                        // std.debug.print("full bounding insert\n", .{});
                        // this is the bounding page, so this is where the value should go
                        // hence, displace another entity from it
                        const lowest_entity = page.entities[0];
                        const lowest_value = page.data[0];

                        // finds the index if we push the rest backwards
                        // but since we remove at from here, we insert at i-1
                        const i = page.find(entity);
                        //         std.debug.print("{}\n", .{i});
                        std.mem.copyForwards(Entity, page.entities[0..], page.entities[1..i]);
                        std.mem.copyForwards(T, page.data[0..], page.data[1..i]);
                        page.entities[i - 1] = entity;
                        page.data[i - 1] = value;
                        page.header.min = page.entities[0];
                        page.header.max = page.entities[page.header.len - 1];

                        // std.debug.print("{any}\n", .{page.entities[0..page.header.len]});
                        std.debug.assert(std.sort.isSorted(
                            Entity,
                            page.entities[0..page.header.len],
                            {},
                            std.sort.asc(Entity),
                        ));

                        // now reinsert the displaced entity
                        try @call(.always_tail, add, .{ storage, lowest_entity, lowest_value });
                    } else {
                        page.insert(entity, value);
                        return;
                    }
                } else {
                    if (page.isFull()) {
                        if (entity < page.header.min) {
                            if (page.header.left) |left| {
                                page = left;
                            } else {
                                page.header.left = try Page(T).create(storage.alloc, entity, value);
                                // TODO rebalance
                                return;
                            }
                        } else {
                            std.debug.assert(entity > page.header.max);
                            if (page.header.right) |right| {
                                page = right;
                            } else {
                                page.header.right = try Page(T).create(storage.alloc, entity, value);
                                // TODO rebalance
                                return;
                            }
                        }
                    } else {
                        page.insert(entity, value);
                        return;
                    }
                }
            }
        }

        fn del(storage: *Self, _entity: Entity) void {
            if (storage.root == null) {
                return;
            }

            var entity = _entity;

            var page = storage.root.?;
            var parent: ?*?*Page(T) = null;
            while (true) {
                // std.debug.print("{} [ {} ] {}\n", .{ page.header.min, entity, page.header.max });
                if (page.isBounding(entity)) {
                    if (page.entities[page.find(entity)] != entity) {
                        std.debug.print("1\n", .{});
                        return;
                    }

                    // we have found our entity, now delete it
                    if (page.header.len == 1) {
                        std.debug.assert(page.header.left == null);
                        std.debug.assert(page.header.right == null);
                        // we're deleting the only element, so just remove the whole node
                        // making sure we also null-out this page in it's parent
                        if (page == storage.root.?) {
                            storage.root = null;
                            page.destroy(storage.alloc);
                        } else {
                            std.debug.assert(parent != null);
                            std.debug.assert(parent.?.* != null);
                            parent.?.* = null;
                            page.destroy(storage.alloc);
                        }
                        return;
                    }

                    if (page.header.left == null and page.header.right == null) {
                        page.erase(entity);
                        return;
                    }

                    if ((page.header.left == null and page.header.right != null) or
                        (page.header.left != null and page.header.right == null))
                    {
                        const child = page.header.left orelse page.header.right.?;
                        page.erase(entity);

                        // now merge with subtree if possible
                        if (page.header.len + child.header.len > page.entities.len) {
                            return;
                        }
                        for (0..child.header.len) |i| {
                            page.insert(child.entities[i], child.data[i]);
                        }
                        page.header.left = null;
                        page.header.right = null;
                        child.destroy(storage.alloc);

                        return;
                    }

                    std.debug.assert(page.header.left != null);
                    std.debug.assert(page.header.right != null);

                    page.erase(entity);
                    if (page.header.len > page.entities.len / 2) {
                        return;
                    }

                    var glb = page.header.left.?;
                    while (glb.header.right) |right| {
                        glb = right;
                    }

                    const glb_entity = glb.entities[glb.header.len - 1];
                    const glb_value = glb.data[glb.header.len - 1];
                    page.insert(glb_entity, glb_value);

                    // essential "recur" on the value we removed
                    entity = glb_entity;
                    page = glb;
                } else {
                    if (entity < page.header.min) {
                        if (page.header.left) |left| {
                            parent = &page.header.left;
                            page = left;
                        } else {
                            std.debug.print("2\n", .{});
                            return;
                        }
                    } else {
                        std.debug.assert(entity > page.header.max);
                        if (page.header.right) |right| {
                            parent = &page.header.right;
                            page = right;
                        } else {
                            std.debug.print("3\n", .{});
                            return;
                        }
                    }
                }
            }
        }

        fn get(storage: *Self, entity: Entity) ?*T {
            _ = storage;
            _ = entity;
        }
    };
}

test "scratch" {
    std.debug.print("\n", .{});

    var s = Storage(f32).init(std.testing.allocator);
    defer s.deinit();

    // try s.add(0, 1.0);
    // std.debug.print("{}\n", .{s.root.?.entities.len});

    // add a bunch of entries
    var i: usize = 0;
    for (0..100_000) |_| {
        try s.add(i, @floatFromInt(i));
        i = (i + 2_654_435_761) % (65536 * 65536); // weyl sequence
    }

    // remove some
    i = 0;
    for (0..20_000) |_| {
        s.del(i);
        i = (i + 2_654_435_761) % (65536 * 65536);
        i = (i + 2_654_435_761) % (65536 * 65536);
        s.del(i);
        i = (i + 2_654_435_761) % (65536 * 65536);
        i = (i + 2_654_435_761) % (65536 * 65536);
        i = (i + 2_654_435_761) % (65536 * 65536);
    }
}
