const std = @import("std");

const Entity = u32;
const EntityVersion = u16;
const EntityIndex = u16;

const Detail = packed struct {
    index: EntityIndex,
    version: EntityVersion,
};

fn detail(entity: Entity) Detail {
    return @bitCast(entity);
}

fn newEntity(old: Entity) Entity {
    var d = detail(old);
    d.version += 1;
    return @bitCast(d);
}

// fn tomb(entity: Entity) Entity {
//     var d = detail(entity);
//     d.index = std.max.maxInt(EntityIndex);
//     return @bitCast(d);
// }

fn isTomb(entity: Entity) bool {
    const d = detail(entity);
    return d.index == std.math.maxInt(EntityIndex);
}

fn Storage(comptime T: type) type {
    const TOMB = std.math.maxInt(EntityIndex);

    return struct {
        const Self = @This();

        const Iterator = struct {
            const KV = struct { entity: Entity, value: T };

            storage: *Self,
            cursor: usize,

            pub fn next(iter: *Iterator) ?KV {
                if (iter.cursor > 0) {
                    iter.cursor -= 1;
                    return .{
                        .entity = iter.storage.dense[iter.cursor],
                        .value = iter.storage.data[iter.cursor],
                    };
                }
                return null;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            return Iterator{
                .storage = storage,
                .cursor = storage.len,
            };
        }

        sparse: []EntityIndex,
        dense: []Entity,
        data: []T,

        len: usize,

        pub fn init() Self {
            return Self{
                .sparse = &.{},
                .dense = &.{},
                .data = &.{},
                .len = 0,
            };
        }

        // pub fn initCopy(alloc: std.mem.Allocator, other: *Self) Self {}

        pub inline fn contains(storage: *Self, entity: Entity) bool {
            std.debug.assert(!isTomb(entity));
            const d = detail(entity);
            // std.debug.print("{}\n", .{d});
            // std.debug.print("{}\n", .{storage.sparse.len});
            // if (storage.sparse.len > d.index) std.debug.print("{}\n", .{storage.sparse[d.index]});
            // if (storage.sparse.len > d.index and storage.sparse[d.index] != TOMB) std.debug.print("{}\n", .{storage.dense[storage.sparse[d.index]]});
            return storage.sparse.len > d.index and storage.sparse[d.index] != TOMB and storage.dense[storage.sparse[d.index]] == entity;
        }

        /// entity must not be in storage
        pub fn add(storage: *Self, alloc: std.mem.Allocator, entity: Entity, value: T) void {
            std.debug.assert(!storage.contains(entity));

            const d = detail(entity);
            if (d.index >= storage.sparse.len) {
                expandFill(EntityIndex, &storage.sparse, d.index, alloc, TOMB);
            }

            if (storage.len == storage.dense.len) {
                expand(Entity, &storage.dense, 2 * storage.len, alloc);
                expand(T, &storage.data, 2 * storage.len, alloc);
            }

            storage.dense[storage.len] = entity;
            if (@sizeOf(T) > 0) {
                storage.data[storage.len] = value;
            }
            storage.sparse[d.index] = @intCast(storage.len);
            storage.len += 1;
        }

        /// entity must be in storage
        pub fn get(storage: *Self, entity: Entity) T {
            std.debug.assert(storage.contains(entity));

            const d = detail(entity);
            const i = storage.sparse[d.index];
            storage.dense[i] = entity;
            if (@sizeOf(T) > 0) {
                return storage.data[i];
            } else {
                return .{}; // dunno how this works with strange empty types...
            }
        }

        /// entity must be in storage
        pub fn set(storage: *Self, entity: Entity, value: T) void {
            std.debug.assert(storage.contains(entity));

            const d = detail(entity);
            const i = storage.sparse[d.index];
            storage.dense[i] = entity;
            if (@sizeOf(T) > 0) {
                storage.data[i] = value;
            }
        }

        /// returns whether an element was deleted
        pub fn del(storage: *Self, entity: Entity) bool {
            if (!storage.contains(entity)) {
                return false;
            }

            std.debug.assert(storage.len > 0);
            const d = detail(entity);
            const i = storage.sparse[d.index]; // storage location of what we are deleting
            const j = storage.len - 1; // storage location of what we are repacing it with
            const d2 = detail(storage.dense[j]);

            storage.dense[i] = storage.dense[j];
            if (@sizeOf(T) > 0) {
                storage.data[i] = storage.data[j];
            }
            storage.sparse[d2.index] = i;
            storage.sparse[d.index] = TOMB;

            storage.len -= 1;

            return true;
        }

        fn expand(comptime U: type, arr: *[]U, min: usize, alloc: std.mem.Allocator) void {
            if (@sizeOf(U) == 0) return;

            const len = std.math.ceilPowerOfTwoAssert(usize, @max(16, min));
            const new = alloc.alloc(U, len) catch @panic("out of memory");
            std.mem.copy(U, new, arr.*);
            arr.* = new;
        }

        fn expandFill(comptime U: type, arr: *[]U, min: usize, alloc: std.mem.Allocator, value: U) void {
            if (@sizeOf(U) == 0) return;

            const len = std.math.ceilPowerOfTwoAssert(usize, @max(16, min));
            const new = alloc.alloc(U, len) catch @panic("out of memory");

            // i wonder why there is no std.mem.fill or something...
            for (arr.len..new.len) |i| {
                new[i] = value;
            }

            std.mem.copy(U, new, arr.*);
            arr.* = new;
        }
    };
}

test "Storage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer _ = arena.deinit();
    const alloc = arena.allocator();

    var storage = Storage(f32).init();
    storage.add(alloc, 1, 1.23);
    storage.add(alloc, 2, 2.34);
    storage.add(alloc, 3, 3.45);

    var iter = storage.iterator();
    try std.testing.expectEqual(@as(Entity, 3), iter.next().?.entity);
    try std.testing.expectEqual(@as(Entity, 2), iter.next().?.entity);
    try std.testing.expectEqual(@as(Entity, 1), iter.next().?.entity);

    try std.testing.expect(storage.del(2));

    iter = storage.iterator();
    try std.testing.expectEqual(@as(f32, 3.45), iter.next().?.value);
    try std.testing.expectEqual(@as(f32, 1.23), iter.next().?.value);

    iter = storage.iterator();
    storage.add(alloc, 4, 4.56);
    try std.testing.expect(storage.del(iter.next().?.entity));
    storage.add(alloc, 5, 5.67);
    try std.testing.expect(storage.del(iter.next().?.entity));
}

pub fn State(comptime Base: type) type {
    const Component = std.meta.FieldEnum(Base);

    return struct {

        // generate a type with pointers to those fields in Base specified by the fields enum
        // the fields enum should contain entries from std.meta.FieldEnum(Base)
        fn Item(comptime fields: anytype) type {
            const n_fields = blk: {
                var i: usize = 0;
                for (fields) |_| {
                    i += 1;
                }
                break :blk i;
            };

            var item_fields: [n_fields]std.builtin.Type.StructField = undefined;

            // iterate over all the
            inline for (fields, 0..) |field, i| {
                const j = @intFromEnum(@as(Component, field));

                item_fields[i] = std.builtin.Type.StructField{
                    .name = std.meta.fieldNames(Component)[j],
                    .type = *std.meta.fields(Base)[j].type,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(usize),
                };
            }

            return @Type(std.builtin.Type{ .Struct = std.builtin.Type.Struct{
                .layout = .Auto,
                .is_tuple = false,
                .backing_integer = null,
                .fields = &item_fields,
                .decls = &.{},
            } });
        }
    };
}

const TT1 = struct {
    int: i32,
    float: f32,
};
test "Item" {
    // just make sure stuff compiles
    const a: State(TT1).Item(.{}) = undefined;
    const b: State(TT1).Item(.{.int}) = undefined;
    const c: State(TT1).Item(.{.float}) = undefined;
    const d: State(TT1).Item(.{ .int, .float }) = undefined;

    // std.debug.print("{}\n", .{a});
    // std.debug.print("{}\n", .{b});
    // std.debug.print("{}\n", .{c});
    // std.debug.print("{}\n", .{d});

    _ = a;
    _ = b;
    _ = c;
    _ = d;
}
