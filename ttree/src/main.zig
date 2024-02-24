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
                    return page.header.min < entity and page.header.max > entity;
                }

                /// returns either the index of entity
                /// or the index where insertion of entity would preserve sorted order
                fn find(page: *Self, entity: Entity) usize {
                    // just a linear search for now
                    var i: usize = 0;
                    while (i < page.header.len) : (i += 1) {
                        if (entity >= page.entities[i]) {
                            return i;
                        }
                    }
                    return page.header.len;
                }

                fn isFull(page: *Self) bool {
                    return page.header.len >= page.entities.len;
                }

                /// if entity is already on the page
                /// just overwrites the value
                fn insert(page: *Self, entity: Entity, value: T) void {
                    std.debug.assert(!page.isFull());

                    const i = page.find(entity);
                    if (page.entities[i] == entity) {
                        page.data[i] = value;
                        return;
                    }

                    std.mem.copyBackwards(Entity, page.entities[i + 1 ..], page.entities[i..page.header.len]);
                    std.mem.copyBackwards(T, page.data[i + 1 ..], page.data[i..page.header.len]);
                    page.entities[i] = entity;
                    page.data[i] = value;
                    page.header.len += 1;
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

            // if we don't have a root, make one
            if (storage.root == null) {
                storage.root = try Page(T).create(storage.alloc, entity, value);
                return;
            }

            var page = storage.root.?;
            while (true) {
                if (page.isBounding(entity)) {
                    if (page.isFull()) {
                        // this is the bounding page, so this is where the value should go
                        // hence, displace another entity from it
                        const lowest_entity = page.entities[0];
                        const lowest_value = page.data[0];

                        const i = page.find(entity);
                        std.mem.copyForwards(Entity, page.entities[0..], page.entities[1..i]);
                        std.mem.copyForwards(T, page.data[0..], page.data[1..i]);
                        page.entities[i] = entity;
                        page.data[i] = value;
                        page.header.min = page.entities[0];
                        page.header.max = page.entities[page.header.len - 1];

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
                                return;
                            }
                        } else {
                            std.debug.assert(entity > page.header.max);
                            if (page.header.right) |right| {
                                page = right;
                            } else {
                                page.header.right = try Page(T).create(storage.alloc, entity, value);
                                return;
                            }
                        }
                    } else {
                        page.insert(entity, value);
                        page.header.min = page.entities[0];
                        page.header.max = page.entities[page.header.len - 1];
                        return;
                    }
                }
            }
        }

        fn del(storage: *Self, entity: Entity) void {
            _ = storage;
            _ = entity;
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

    var i: usize = 0;
    for (0..100_000_000) |j| {
        if (j % 1_000_000 == 0) {
            std.debug.print("{} {}\n", .{ j, i });
        }
        try s.add(i, @floatFromInt(i));
        i = (i + 2_499_999_997) % (65536 * 65536); // weyl sequence
    }
}
