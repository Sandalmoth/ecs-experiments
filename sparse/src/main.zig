const std = @import("std");

const Entity = u64;
const EntityVersion = u32;
const EntityIndex = u32;

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

        pub fn initCopy(alloc: std.mem.Allocator, other: *Self) Self {
            // note, we shrink the storage to the nearest power of two
            // so that we can reclaim memory if this component is now rare
            const len = std.math.ceilPowerOfTwoAssert(usize, @max(16, other.len));
            const storage = Self{
                .sparse = alloc.dupe(EntityIndex, other.sparse) catch @panic("out of memory"),
                .dense = alloc.alloc(Entity, len) catch @panic("out of memory"),
                .data = if (@sizeOf(T) == 0) &.{} else alloc.alloc(T, len) catch @panic("out of memory"),
                .len = other.len,
            };
            @memcpy(storage.dense.ptr, other.dense);
            if (@sizeOf(T) > 0) {
                @memcpy(storage.data.ptr, other.data);
            }
            return storage;
        }

        pub inline fn contains(storage: *Self, entity: Entity) bool {
            std.debug.assert(!isTomb(entity));
            const d = detail(entity);
            return storage.sparse.len > d.index and storage.sparse[d.index] != TOMB and storage.dense[storage.sparse[d.index]] == entity;
        }

        /// entity must not be in storage
        pub fn add(storage: *Self, alloc: std.mem.Allocator, entity: Entity, value: T) void {
            std.debug.assert(!storage.contains(entity));

            const d = detail(entity);
            if (d.index >= storage.sparse.len) {
                expandFill(EntityIndex, &storage.sparse, d.index + 1, alloc, TOMB);
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
        pub fn getPtr(storage: *Self, entity: Entity) *T {
            std.debug.assert(storage.contains(entity));

            const d = detail(entity);
            const i = storage.sparse[d.index];
            storage.dense[i] = entity;
            if (@sizeOf(T) > 0) {
                return &storage.data[i];
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
            @memcpy(new.ptr, arr.*);
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

            @memcpy(new.ptr, arr.*);
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
    const n_components = std.meta.fields(Component).len;

    return struct {
        const Self = @This();
        arena: std.heap.ArenaAllocator,
        prev: ?*Self,

        storage: [n_components]usize, // type erased pointers

        entities: Storage(void), // for keeping track of what entities exist
        reusable_entities: std.ArrayList(Entity),
        entity_counter: EntityIndex = 0,

        fn initStorage(state: *Self) void {
            state.entities = Storage(void).init();
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                const ST = StorageType(c);
                const storage = state.arena.allocator().create(ST) catch @panic("out of memory");
                storage.* = ST.init();
                state.storage[i] = @intFromPtr(storage);
            }
            state.reusable_entities = std.ArrayList(Entity).init(state.arena.allocator());
        }

        pub fn init(alloc: std.mem.Allocator, chain: usize) *Self {
            var state = alloc.create(Self) catch @panic("out of memory");
            state.* = .{
                .arena = std.heap.ArenaAllocator.init(alloc),
                .prev = null,
                .storage = undefined,
                .entities = undefined,
                .reusable_entities = undefined,
            };
            state.initStorage();

            for (1..chain) |_| {
                var next = alloc.create(Self) catch @panic("out of memory");
                next.* = .{
                    .arena = std.heap.ArenaAllocator.init(alloc),
                    .prev = state,
                    .storage = undefined,
                    .entities = undefined,
                    .reusable_entities = undefined,
                };
                next.initStorage();
                state = next;
            }

            return state;
        }

        pub fn deinit(state: *Self, alloc: std.mem.Allocator) void {
            if (state.prev) |prev| {
                prev.deinit(alloc);
            }
            state.arena.deinit();
            alloc.destroy(state);
        }

        pub fn create(state: *Self) Entity {
            if (state.reusable_entities.items.len == 0) {
                const entity: Entity = @bitCast(Detail{ .index = state.entity_counter, .version = 0 });
                state.entity_counter += 1;
                state.entities.add(state.arena.allocator(), entity, {});
                return entity;
            }
            return newEntity(state.reusable_entities.pop());
        }

        pub fn destroy(state: *Self, entity: Entity) void {
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                const storage = state.getStorage(c);
                _ = storage.del(entity);
            }
            state.reusable_entities.append(entity) catch @panic("out of memory");
            _ = state.entities.del(entity);
        }

        pub fn exists(state: *Self, entity: Entity) bool {
            return state.entities.contains(entity);
        }

        pub fn has(state: *Self, entity: Entity, comptime c: Component) bool {
            return state.getStorage(c).contains(entity);
        }

        pub fn add(state: *Self, entity: Entity, comptime c: Component, value: ComponentType(c)) void {
            state.getStorage(c).add(state.arena.allocator(), entity, value);
        }

        pub fn del(state: *Self, entity: Entity, comptime c: Component) void {
            _ = state.getStorage(c).del(entity);
        }

        pub fn get(state: *Self, entity: Entity, comptime c: Component) ComponentType(c) {
            state.getStorage(c).get(entity);
        }

        pub fn getPtr(state: *Self, entity: Entity, comptime c: Component) *ComponentType(c) {
            state.getStorage(c).getPtr(entity);
        }

        pub fn set(state: *Self, entity: Entity, comptime c: Component, value: ComponentType(c)) void {
            state.getStorage(c).set(entity, value);
        }

        pub fn step(state: *Self) *Self {
            // repurpose the oldest step as the new one
            var next = state;
            while (next.prev) |prev| {
                if (prev.prev == null) {
                    next.prev = null;
                }
                next = prev;
            }
            std.debug.assert(next.prev == null);
            next.prev = state;
            std.debug.assert(next != state);

            // then copy all the data to the new storage.
            _ = next.arena.reset(.retain_capacity);
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                const ST = StorageType(c);
                const old_storage = state.getStorage(c);
                const new_storage = next.arena.allocator().create(ST) catch @panic("out of memory");
                new_storage.* = ST.initCopy(next.arena.allocator(), old_storage);
                next.storage[i] = @intFromPtr(new_storage);
            }
            next.entities = Storage(void).initCopy(next.arena.allocator(), &state.entities);
            next.reusable_entities = std.ArrayList(Entity).initCapacity(
                next.arena.allocator(),
                state.reusable_entities.capacity,
            ) catch @panic("out of memory");
            @memcpy(
                next.reusable_entities.items[0..next.reusable_entities.capacity],
                state.reusable_entities.items,
            );
            next.entity_counter = state.entity_counter;

            return next;
        }

        pub fn iterator(state: *Self, comptime fields: anytype) Iterator(fields) {
            return Iterator(fields).init(state);
        }

        fn ComponentType(comptime c: Component) type {
            return std.meta.fields(Base)[@intFromEnum(c)].type;
        }

        fn StorageType(comptime c: Component) type {
            return Storage(ComponentType(c));
        }

        fn getStorage(state: *Self, comptime c: Component) *StorageType(c) {
            const i: usize = @intFromEnum(c);
            return @ptrFromInt(state.storage[i]);
        }

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

            var item_fields: [n_fields + 1]std.builtin.Type.StructField = undefined;

            // iterate over all parts of Base
            // and generate a pointer type if they are in fields
            inline for (fields, 0..) |field, i| {
                const j = @intFromEnum(@as(Component, field));

                item_fields[i] = std.builtin.Type.StructField{
                    .name = std.meta.fieldNames(Component)[j],
                    .type = *std.meta.fields(Base)[j].type,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(*std.meta.fields(Base)[j].type),
                };
            }

            // Entity id
            item_fields[n_fields] = std.builtin.Type.StructField{
                .name = "entity",
                .type = Entity,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Entity),
            };

            return @Type(std.builtin.Type{ .Struct = std.builtin.Type.Struct{
                .layout = .Auto,
                .is_tuple = false,
                .backing_integer = null,
                .fields = &item_fields,
                .decls = &.{},
            } });
        }

        fn Iterator(comptime fields: anytype) type {
            const n_fields = blk: {
                var i: usize = 0;
                for (fields) |_| {
                    i += 1;
                }
                break :blk i;
            };
            std.debug.assert(n_fields > 0); // disallow iterating over no fields

            return struct {
                const Iter = @This();

                storage: [n_fields]usize,
                anchor: usize, // index into storage and components
                cursor: usize,

                // NOTE this is not so elegant
                // because we have to reimplement the Storage Iterator
                // rather than reusing it for the anchor type...

                fn init(state: *Self) Iter {
                    var iter: Iter = undefined;

                    // find the pool with the fewest items
                    var len: usize = std.math.maxInt(usize);
                    inline for (fields, 0..) |field, i| {
                        const storage = state.getStorage(field);
                        iter.storage[i] = @intFromPtr(storage);
                        if (storage.len < len) {
                            iter.anchor = i;
                            iter.cursor = storage.len;
                            len = storage.len;
                        }
                    }

                    return iter;
                }

                pub fn next(iter: *Iter) ?Item(fields) {
                    // iterate the pool with the fewest items
                    find_element: while (iter.cursor > 0) {
                        iter.cursor -= 1;

                        var item: Item(fields) = undefined;

                        // could be (inline) switch since we only access one?
                        inline for (fields, 0..) |field, i| {
                            const storage: *StorageType(field) = @ptrFromInt(iter.storage[i]);
                            if (i == iter.anchor) {
                                item.entity = storage.dense[iter.cursor];
                                const j = @intFromEnum(@as(Component, field));
                                @field(item, std.meta.fieldNames(Component)[j]) =
                                    &storage.data[iter.cursor];
                            }
                        }

                        // then look up the fields in the other pools
                        inline for (fields, 0..) |field, i| {
                            const storage: *StorageType(field) = @ptrFromInt(iter.storage[i]);
                            if (i != iter.anchor) {
                                if (!storage.contains(item.entity)) {
                                    continue :find_element;
                                }
                                const j = @intFromEnum(@as(Component, field));
                                @field(item, std.meta.fieldNames(Component)[j]) =
                                    storage.getPtr(item.entity);
                            }
                        }

                        return item;
                    }
                    return null;
                }
            };
        }
    };
}

const TT1 = struct {
    int: i32,
    float: f32,
};
test "State" {
    var state = State(TT1).init(std.testing.allocator, 2);
    defer state.deinit(std.testing.allocator);

    {
        const e0 = state.create();
        state.add(e0, .int, 0);
        state.add(e0, .float, 0.0);

        const e1 = state.create();
        state.add(e1, .int, 1);

        const e2 = state.create();
        state.add(e2, .float, 2.0);

        const e3 = state.create();
        state.add(e3, .int, 3);
        state.add(e3, .float, 3.0);

        const e4 = state.create();
        state.destroy(e4);

        try std.testing.expect(state.exists(e0));
        try std.testing.expect(!state.exists(e4));
    }

    std.debug.print("\n", .{});

    {
        var iter = state.iterator(.{.int});
        while (iter.next()) |e| {
            std.debug.print("{} {}\n", .{ e, e.int.* });
        }
    }

    {
        var iter = state.iterator(.{.float});
        while (iter.next()) |e| {
            std.debug.print("{} {}\n", .{ e, e.float.* });
        }
    }

    {
        var iter = state.iterator(.{ .int, .float });
        while (iter.next()) |e| {
            std.debug.print("{} {} {}\n", .{ e, e.int.*, e.float.* });
        }
    }
}
