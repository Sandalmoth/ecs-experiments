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

        pub fn create(table: *Self, entity: Entity, value: T) void {
            // only create the entry if it doesn't already exist, and isn't already being created
            _ = table.findCreated(entity) orelse table.find(entity) orelse {
                std.debug.assert(table.created_entities.len == table.created_data.len);
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
                std.mem.copyBackwards(
                    T,
                    table.created_data[i + 1 .. table.created_len],
                    table.created_data[i .. table.created_len - 1],
                );

                table.created_entities[i] = entity;
                table.created_data[i] = value;
                std.debug.assert(isSorted(table.created_entities[0..table.created_len]));
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
                std.debug.assert(isSorted(table.destroyed_entities[0..table.destroyed_len]));
                return;
            };
        }

        pub fn update(table: *Self) void {
            std.debug.assert(table.entities.len == table.data.len);
            std.debug.assert(table.created_entities.len == table.created_data.len);

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
                        table.data[i] = table.data[i + k];
                        i += 1;
                    }
                }

                table.len -= table.destroyed_len;
                table.destroyed_len = 0;

                std.debug.assert(isSorted(table.entities[0..table.len]));
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
                        table.data[end - 1] = table.data[i - 1];
                        i -= 1;
                    } else {
                        table.entities[end - 1] = table.created_entities[j - 1];
                        table.data[end - 1] = table.created_data[j - 1];
                        j -= 1;
                    }

                    end -= 1;
                }

                table.len = len;
                table.created_len = 0;

                std.debug.assert(isSorted(table.entities[0..table.len]));
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
