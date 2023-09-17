const std = @import("std");

pub const Entity = u32;

// some specialized forms and variants of functions from std.sort below
// because I don't want to bother with contexts and stuff

// we could probably optimize by doing linear searches below a certain size
// or incorporating other heuristics

/// find index of an element
fn binarySearch(entities: []Entity, key: Entity) ?usize {
    var left: usize = 0;
    var right: usize = entities.len;

    while (left < right) {
        const mid = left + (right - left) / 2;
        switch (std.math.order(key, entities[mid])) {
            .eq => return mid,
            .gt => left = mid + 1,
            .lt => right = mid,
        }
    }
    return null;
}

/// find the position where inserting yields a sorted list
fn lowerBound(entities: []Entity, key: Entity) usize {
    var left: usize = 0;
    var right: usize = entities.len;

    while (left < right) {
        const mid = left + (right - left) / 2;
        switch (std.math.order(key, entities[mid])) {
            .eq => return mid,
            .gt => left = mid + 1,
            .lt => right = mid,
        }
    }
    return left;
}

pub fn isSorted(entities: []Entity) bool {
    // NOTE only used in asserts
    var i: usize = 1;
    while (i < entities.len) : (i += 1) {
        if (entities[i] < entities[i - 1]) {
            return false;
        }
    }

    return true;
}

pub fn Table(comptime T: type) type {
    return struct {
        const Self = @This();

        // should be an arena, as Table does not even try to clean up
        alloc: std.mem.Allocator,

        entities: []Entity,
        data: []T,
        len: usize,

        // these are merged when we call update
        created_entities: []Entity,
        created_data: []T,
        created_len: usize,

        // these are destroyed when we call update
        destroyed_entities: []Entity,
        destroyed_len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .entities = &[_]Entity{},
                .data = &[_]T{},
                .len = 0,
                .created_entities = &[_]Entity{},
                .created_data = &[_]T{},
                .created_len = 0,
                .destroyed_entities = &[_]Entity{},
                .destroyed_len = 0,
            };
        }

        pub fn initCopy(alloc: std.mem.Allocator, other: Self) Self {
            std.debug.assert(other.created_len == 0);
            std.debug.assert(other.destroyed_len == 0);

            var table = Self{
                .alloc = alloc,
                .entities = undefined,
                .data = undefined,
                .len = undefined,
                .created_entities = &[_]Entity{},
                .created_data = &[_]T{},
                .created_len = 0,
                .destroyed_entities = &[_]Entity{},
                .destroyed_len = 0,
            };
            // NOTE we could slightly optimieze if we only copy up until len
            table.entities = table.alloc.dupe(Entity, other.entities) catch @panic("out of memory");
            if (T != void) {
                table.data = table.alloc.dupe(T, other.data) catch @panic("out of memory");
            }
            table.len = other.len;

            return table;
        }

        pub fn create(table: *Self, entity: Entity, value: T) void {
            // only create the entry if it doesn't already exist, and isn't already being created
            _ = table.findCreated(entity) orelse table.find(entity) orelse {
                if (T != void) {
                    std.debug.assert(table.created_entities.len == table.created_data.len);
                }
                if (table.created_len == table.created_entities.len) {
                    const len = @max(16, table.created_entities.len * 2);
                    expand(Entity, &table.created_entities, len, table.alloc);
                    expand(T, &table.created_data, len, table.alloc);
                }

                // insert such that created_entities remains in sorted order
                const i = lowerBound(table.created_entities[0..table.created_len], entity);
                table.created_len += 1;

                std.mem.copyBackwards(
                    Entity,
                    table.created_entities[i + 1 .. table.created_len],
                    table.created_entities[i .. table.created_len - 1],
                );
                table.created_entities[i] = entity;

                if (T != void) {
                    std.mem.copyBackwards(
                        T,
                        table.created_data[i + 1 .. table.created_len],
                        table.created_data[i .. table.created_len - 1],
                    );
                    table.created_data[i] = value;
                }

                // std.debug.assert(isSorted(table.created_entities[0..table.created_len]));
                return;
            };
        }

        pub fn destroy(table: *Self, entity: Entity) void {
            // don't destroy an entry twice and only destroy entries that exist
            _ = table.findDestroyed(entity) orelse {
                if (table.find(entity) == null) {
                    return;
                }

                if (table.destroyed_len == table.destroyed_entities.len) {
                    const len = @max(16, table.destroyed_entities.len);
                    expand(Entity, &table.destroyed_entities, len, table.alloc);
                }
                const i = lowerBound(table.destroyed_entities[0..table.destroyed_len], entity);
                table.destroyed_len += 1;

                std.mem.copyBackwards(
                    Entity,
                    table.destroyed_entities[i + 1 .. table.destroyed_len],
                    table.destroyed_entities[i .. table.destroyed_len - 1],
                );

                table.destroyed_entities[i] = entity;
                // std.debug.assert(isSorted(table.destroyed_entities[0..table.destroyed_len]));
                return;
            };
        }

        pub fn update(table: *Self) void {
            if (T != void) {
                std.debug.assert(table.entities.len == table.data.len);
                std.debug.assert(table.created_entities.len == table.created_data.len);
            }

            // TODO can we destroy and merge created in one linear time pass ?

            if (table.destroyed_len > 0) {
                // we know all items in destroyed_entities are also in entities
                // because we check in the call to remove
                var i: usize = table.find(table.destroyed_entities[0]).?;
                var j: usize = 1;
                var k: usize = 1;

                while (i + k < table.len) {
                    if (j < table.destroyed_len and table.destroyed_entities[j] == table.entities[i + k]) {
                        j += 1;
                        k += 1;
                    } else {
                        table.entities[i] = table.entities[i + k];
                        if (T != void) {
                            table.data[i] = table.data[i + k];
                        }
                        i += 1;
                    }
                }

                table.len -= table.destroyed_len;
                table.destroyed_len = 0;

                // std.debug.assert(isSorted(table.entities[0..table.len]));
            }

            if (table.created_len > 0) {
                const len = table.len + table.created_len;
                if (len > table.entities.len) {
                    expand(Entity, &table.entities, len, table.alloc);
                    expand(T, &table.data, len, table.alloc);
                }

                var i = table.len;
                var j = table.created_len;
                var end = len;

                while (j > 0) {
                    if (i > 0 and table.entities[i - 1] > table.created_entities[j - 1]) {
                        table.entities[end - 1] = table.entities[i - 1];
                        if (T != void) {
                            table.data[end - 1] = table.data[i - 1];
                        }
                        i -= 1;
                    } else {
                        table.entities[end - 1] = table.created_entities[j - 1];
                        if (T != void) {
                            table.data[end - 1] = table.created_data[j - 1];
                        }
                        j -= 1;
                    }

                    end -= 1;
                }

                table.len = len;
                table.created_len = 0;

                // std.debug.assert(isSorted(table.entities[0..table.len]));
            }
        }

        pub fn contains(table: Self, entity: Entity) bool {
            return table.find(entity) != null;
        }

        pub fn data(table: Self) []Entity {
            return table.data[0..table.len];
        }

        pub fn get(table: Self, entity: Entity) ?T {
            const i = table.find(entity) orelse return null;
            return table.data[i];
        }

        pub fn getPtr(table: Self, entity: Entity) ?*T {
            const i = table.find(entity) orelse return null;
            return &table.data[i];
        }

        fn set(table: *Self, entity: Entity, value: T) void {
            const i = table.find(entity) orelse {
                return;
            };
            table.data[i] = value;
        }

        fn expand(comptime U: type, arr: *[]U, min: usize, alloc: std.mem.Allocator) void {
            if (U == void) return;

            const len = std.math.ceilPowerOfTwoAssert(usize, min);
            const new = alloc.alloc(U, len) catch @panic("out of memory");
            std.mem.copy(U, new, arr.*);
            arr.* = new;
        }

        fn find(table: Self, entity: Entity) ?usize {
            return binarySearch(table.entities[0..table.len], entity);
        }

        fn findCreated(table: Self, entity: Entity) ?usize {
            return binarySearch(table.created_entities[0..table.created_len], entity);
        }

        fn findDestroyed(table: Self, entity: Entity) ?usize {
            return binarySearch(table.destroyed_entities[0..table.destroyed_len], entity);
        }
    };
}

test "Table create, destroy & update fuzz" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var table = Table(u32).init(arena.allocator());
    var rng = std.rand.DefaultPrng.init(2701);

    const N = 256;

    for (0..N * N) |_| {
        const x = rng.random().uintLessThan(u32, N);
        const y = rng.random().uintLessThan(u32, N);
        const z = rng.random().uintLessThan(u32, N);

        const u = blk: {
            var a: u32 = rng.random().uintLessThan(u32, N);
            while (a == x or a == y or a == z) {
                a = rng.random().uintLessThan(u32, N);
            }
            break :blk a;
        };
        const v = blk: {
            var a: u32 = rng.random().uintLessThan(u32, N);
            while (a == x or a == y or a == z) {
                a = rng.random().uintLessThan(u32, N);
            }
            break :blk a;
        };
        const w = blk: {
            var a: u32 = rng.random().uintLessThan(u32, N);
            while (a == x or a == y or a == z) {
                a = rng.random().uintLessThan(u32, N);
            }
            break :blk a;
        };

        table.destroy(x);
        table.destroy(y);
        table.destroy(z);

        table.create(u, u);
        table.create(v, v);
        table.create(w, w);

        table.update();
        try std.testing.expect(isSorted(table.entities[0..table.len]));

        try std.testing.expectEqual(false, table.contains(x));
        try std.testing.expectEqual(false, table.contains(y));
        try std.testing.expectEqual(false, table.contains(z));

        try std.testing.expectEqual(true, table.contains(u));
        try std.testing.expectEqual(true, table.contains(v));
        try std.testing.expectEqual(true, table.contains(w));
        // check that we've also replaced the old data
        try std.testing.expectEqual(u, table.get(u).?);
        try std.testing.expectEqual(v, table.get(v).?);
        try std.testing.expectEqual(w, table.get(w).?);
    }
}

test "Table void fuzz" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var table = Table(void).init(arena.allocator());
    var rng = std.rand.DefaultPrng.init(2701);
    const N = 256;

    for (0..N * N) |_| {
        const x = rng.random().uintLessThan(u32, N);
        const u = blk: {
            var a: u32 = rng.random().uintLessThan(u32, N);
            while (a == x) {
                a = rng.random().uintLessThan(u32, N);
            }
            break :blk a;
        };

        table.destroy(x);
        table.create(u, {});

        table.update();
        try std.testing.expect(isSorted(table.entities[0..table.len]));
        try std.testing.expectEqual(false, table.contains(x));
        try std.testing.expectEqual(true, table.contains(u));
    }
}

pub fn State(comptime T: type) type {
    const TFields = std.meta.fields(T);
    const Component = std.meta.FieldEnum(T);
    const n_tables = std.meta.fields(Component).len;

    return struct {
        const Self = @This();

        fn Iterator(comptime fields: anytype) type {
            const n_fields = blk: {
                var i: usize = 0;
                for (fields) |_| {
                    i += 1;
                }
                break :blk i;
            };
            std.debug.assert(n_fields > 0);

            return struct {
                const Iter = @This();

                tables: [n_fields]usize,
                cursors: [n_fields]usize,
                at_end: bool,

                const Item = blk: {
                    var item_fields: [n_fields]std.builtin.Type.StructField = undefined;

                    inline for (fields, 0..) |field, i| {
                        const j = @intFromEnum(@as(Component, field));
                        item_fields[i] = std.builtin.Type.StructField{
                            .name = std.meta.fieldNames(Component)[j],
                            .type = *FieldType(field),
                            .default_value = null,
                            .is_comptime = false,
                            .alignment = @alignOf(usize),
                        };
                    }

                    break :blk @Type(std.builtin.Type{ .Struct = std.builtin.Type.Struct{
                        .layout = .Auto,
                        .is_tuple = false,
                        .backing_integer = null,
                        .fields = &item_fields,
                        .decls = &.{},
                    } });
                };

                pub fn init(state: *Self) Iter {
                    var iter = Iter{
                        .tables = undefined,
                        .cursors = [_]usize{0} ** n_fields,
                        .at_end = false,
                    };

                    inline for (fields, 0..) |field, i| {
                        const j = @intFromEnum(@as(Component, field));
                        iter.tables[i] = state.tables[j];
                    }

                    return iter;
                }

                pub fn next(iter: *Iter) ?Item {
                    while (!iter.at_end) {
                        // if all cursors point to the same element, increment all and return true;
                        var entities: [n_fields]Entity = undefined;
                        inline for (fields, 0..n_fields) |field, i| {
                            const table: *TableType(field) = @ptrFromInt(iter.tables[i]);
                            if (iter.cursors[i] == table.len) {
                                iter.at_end = true;
                                return null;
                            }
                            entities[i] = table.entities[iter.cursors[i]];
                        }

                        if (std.mem.allEqual(Entity, &entities, entities[0])) {
                            for (&iter.cursors) |*cursor| {
                                cursor.* += 1;
                            }
                            var item: Item = undefined;
                            inline for (fields, 0..) |field, i| {
                                const table: *TableType(field) = @ptrFromInt(iter.tables[i]);
                                const j = @intFromEnum(@as(Component, field));
                                @field(item, std.meta.fieldNames(Component)[j]) = &table.data[iter.cursors[i] - 1];
                            }
                            return item;
                        } else {
                            // otherwise increment the cursor that points to the lowest numbered entity
                            const ixmin = std.mem.indexOfMin(Entity, &entities);
                            iter.cursors[ixmin] += 1;
                        }
                    }
                    return null;
                }
            };
        }

        alloc: std.mem.Allocator,
        arena: std.heap.ArenaAllocator, // tables live in the arena
        prev: ?*Self,

        entities: Table(void),
        n_total_entities: Entity,

        // type-erased pointers to Tables
        // the types are given by the order of types in TFields
        tables: [n_tables]usize,

        fn initTables(state: *Self) void {
            state.entities = Table(void).init(state.arena.allocator());
            inline for (0..n_tables) |i| {
                const c: Component = @enumFromInt(i);
                const TT = TableType(c);
                const table = state.arena.allocator().create(TT) catch @panic("out of memory");
                table.* = TT.init(state.arena.allocator());
                state.tables[i] = @intFromPtr(table);
            }
        }

        pub fn init(alloc: std.mem.Allocator, n_states: usize) *Self {
            var state = alloc.create(Self) catch @panic("out of memory");
            state.* = Self{
                .alloc = alloc,
                .arena = std.heap.ArenaAllocator.init(alloc),
                .prev = null,
                .entities = undefined,
                .n_total_entities = 0,
                .tables = [_]usize{0} ** n_tables,
            };
            state.initTables();

            for (1..n_states) |_| {
                const next = alloc.create(Self) catch @panic("out of memory");
                next.* = Self{
                    .alloc = alloc,
                    .arena = std.heap.ArenaAllocator.init(alloc),
                    .prev = null,
                    .entities = undefined,
                    .n_total_entities = 0,
                    .tables = [_]usize{0} ** n_tables,
                };
                next.initTables();
                next.prev = state;
                state = next;
            }

            return state;
        }

        pub fn deinit(state: *Self) void {
            if (state.prev) |prev| {
                prev.deinit();
            }
            state.arena.deinit(); // effectively deinits the tables
            state.alloc.destroy(state);
        }

        fn reinit(state: *Self) void {
            _ = state.arena.reset(.retain_capacity);
            state.initTables();
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

            _ = next.arena.reset(.retain_capacity);
            next.entities = Table(void).initCopy(next.arena.allocator(), state.entities);
            inline for (0..n_tables) |i| {
                const c: Component = @enumFromInt(i);
                const TT = TableType(c);
                const old_table: *TT = @ptrFromInt(state.tables[i]);
                const new_table = next.arena.allocator().create(TT) catch @panic("out of memory");
                new_table.* = TT.initCopy(next.arena.allocator(), old_table.*);
                next.tables[i] = @intFromPtr(new_table);
            }

            return next;
        }

        pub fn getTable(state: *Self, comptime c: Component) *TableType(c) {
            const i: usize = @intFromEnum(c);
            return @ptrFromInt(state.tables[i]);
        }

        pub fn create(state: *Self) Entity {
            if (state.n_total_entities == std.math.maxInt(Entity)) {
                @panic("out of entities");
            }

            const entity = state.n_total_entities;
            state.entities.create(entity, {});
            state.n_total_entities += 1;
            return entity;
        }

        pub fn destroy(state: *Self, entity: Entity) void {
            if (state.entities.contains(entity)) {
                state.entities.destroy(entity);
                inline for (0..n_tables) |i| {
                    const c: Component = @enumFromInt(i);
                    const table: *TableType(c) = @ptrFromInt(state.tables[i]);
                    table.destroy(entity);
                }
            }
        }

        pub fn add(state: *Self, entity: Entity, comptime c: Component, value: FieldType(c)) void {
            // std.debug.assert(state.entities.contains(entity)); unsure if I want this...
            const table = state.getTable(c);
            table.create(entity, value);
        }

        pub fn remove(state: *Self, entity: Entity, comptime c: Component) void {
            const table = state.getTable(c);
            table.destroy(entity);
        }

        pub fn set(state: *Self, entity: Entity, comptime c: Component, value: FieldType(c)) void {
            std.debug.assert(state.entities.contains(entity));
            const table = state.getTable(c);
            table.set(entity, value);
        }

        pub fn get(state: *Self, entity: Entity, comptime c: Component) ?FieldType(c) {
            std.debug.assert(state.entities.contains(entity));
            const table = state.getTable(c);
            return table.get(entity);
        }

        pub fn getPtr(state: *Self, entity: Entity, comptime c: Component) ?*FieldType(c) {
            std.debug.assert(state.entities.contains(entity));
            const table = state.getTable(c);
            return table.getPtr(entity);
        }

        pub fn updateAll(state: *Self) void {
            state.entities.update();
            inline for (0..n_tables) |i| {
                const c: Component = @enumFromInt(i);
                const table: *TableType(c) = @ptrFromInt(state.tables[i]);
                table.update();
            }
        }

        pub fn contains(state: Self, entity: Entity) bool {
            return state.entities.contains(entity);
        }

        pub fn iterator(state: *Self, fields: anytype) Iterator(fields) {
            return Iterator(fields).init(state);
        }

        fn FieldType(comptime c: Component) type {
            return TFields[@intFromEnum(c)].type;
        }

        fn TableType(comptime c: Component) type {
            return Table(TFields[@intFromEnum(c)].type);
        }

        /// given a type, get the Component enum if it's in T
        fn typeEnum(comptime U: type) ?Component {
            inline for (TFields, 0..) |field, i| {
                if (field.type == U) {
                    return @enumFromInt(i);
                }
            }
            return null;
        }
    };
}

const TT1 = struct {
    int: i32,
    float: f32,
};
test "State basics" {
    var state = State(TT1).init(std.testing.allocator, 2);
    defer state.deinit();

    const e0 = state.create();
    try std.testing.expectEqual(@as(Entity, 0), e0);

    const e1 = state.create();
    try std.testing.expectEqual(@as(Entity, 1), e1);

    try std.testing.expect(!state.contains(e0));
    try std.testing.expect(!state.contains(e1));

    state.updateAll();

    try std.testing.expect(state.contains(e0));
    try std.testing.expect(state.contains(e1));

    state.destroy(e0);
    state.updateAll();

    try std.testing.expect(!state.contains(e0));
    try std.testing.expect(state.contains(e1));

    state = state.step();

    try std.testing.expect(!state.contains(e0));
    try std.testing.expect(state.contains(e1));

    state.destroy(e1);
    state.updateAll();

    try std.testing.expect(!state.contains(e0));
    try std.testing.expect(!state.contains(e1));
    // we should not be messing up the previous state
    try std.testing.expect(!state.prev.?.contains(e0));
    try std.testing.expect(state.prev.?.contains(e1));
}

const TT2 = struct {
    int: i32,
    float: f32,
};
test "State iterator" {
    var state = State(TT2).init(std.testing.allocator, 2);
    defer state.deinit();

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

    state.updateAll();

    std.debug.print("\n", .{});

    {
        var iter = state.iterator(.{.int});
        while (iter.next()) |item| {
            std.debug.print("{}\n", .{item.int.*});
        }
    }
    {
        var iter = state.iterator(.{.float});
        while (iter.next()) |item| {
            std.debug.print("{}\n", .{item.float.*});
        }
    }
    {
        var iter = state.iterator(.{ .int, .float });
        while (iter.next()) |item| {
            std.debug.print("{} {}\n", .{ item.int.*, item.int.* });
        }
    }
}
