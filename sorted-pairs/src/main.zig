const std = @import("std");

pub const Entity = u32;
fn order_entity(context: void, lhs: Entity, rhs: Entity) std.math.Order {
    _ = context;
    return std.math.order(lhs, rhs);
}
fn less_than_entity(context: void, lhs: Entity, rhs: Entity) bool {
    _ = context;
    return lhs < rhs;
}

pub fn Context(comptime T: type) type {
    std.debug.assert(@sizeOf(usize) == @sizeOf(*Table(u32)));

    const TFields = std.meta.fields(T);
    const Component = std.meta.FieldEnum(T);
    const n_components = std.meta.fields(Component).len;

    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,

        n_entities: Entity,
        components: [n_components]usize,

        prev: *Self,
        next: *Self,

        pub fn init(alloc: std.mem.Allocator) Self {
            var ctx = Self{
                .alloc = alloc,
                .n_entities = 0,
                .components = undefined, // initialized below
                .prev = undefined,
                .next = undefined,
            };

            // It's pretty incredible that this is even possible
            // hell yeah comptime
            inline for (0..n_components) |i| {
                var table = alloc.create(Table((TFields[i].type))) catch @panic("out of memory");
                table.* = @TypeOf(table.*).init(alloc);
                ctx.components[i] = @intFromPtr(table);
            }

            return ctx;
        }

        pub fn deinit(ctx: *Self) void {
            inline for (0..n_components) |i| {
                var table: *Table(TFields[i].type) = @ptrFromInt(ctx.components[i]);
                table.deinit();
                ctx.alloc.destroy(table);
            }
        }

        pub fn create(ctx: *Self) Entity {
            std.debug.assert(ctx.n_entities < std.math.maxInt(u32));

            const result = ctx.n_entities;
            ctx.n_entities += 1;
            return result;
        }

        pub fn destroy(ctx: *Self) Entity {
            // somehow access all the Tables
            // and call destroy with the id
            _ = ctx;
        }

        pub fn update(ctx: *Self) void {
            inline for (0..n_components) |i| {
                var table: *Table(TFields[i].type) = @ptrFromInt(ctx.components[i]);
                table.update();
            }
        }

        fn fieldType(comptime component: Component) type {
            const ix_table: usize = @intFromEnum(component);
            return TFields[ix_table].type;
        }

        fn getTable(ctx: Self, comptime component: Component) *Table(fieldType(component)) {
            const ix_table: usize = @intFromEnum(component);
            return @ptrFromInt(ctx.components[ix_table]);
        }

        pub fn get(ctx: *Self, entity: Entity, comptime component: Component) ?fieldType(component) {
            var table = ctx.getTable(component);
            return table.get(entity);
        }

        pub fn set(ctx: *Self, entity: Entity, comptime component: Component, value: fieldType(component)) void {
            var table = ctx.getTable(component);
            table.set(entity, value);
        }

        pub fn add(ctx: *Self, entity: Entity, comptime component: Component, value: fieldType(component)) void {
            var table = ctx.getTable(component);
            table.add(entity, value) catch @panic("out of memory");
        }

        pub fn remove(ctx: *Self, entity: Entity, comptime component: Component) void {
            var table = ctx.getTable(component);
            table.remove(entity);
        }
    };
}

/// sorted array of (Entity, T) pairs
pub fn Table(comptime T: type) type {
    // overall internal construction is similar to std.ArrayList
    // const Pair = struct {
    //     ent: Entity,
    //     cpn: T,
    // };

    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,

        entities: []Entity,
        components: []T,
        len: usize,

        // these are merged when we call update
        future_entities: []Entity,
        future_components: []T,
        future_len: usize,

        // these are destroyed when we call update
        future_nonentities: []Entity,
        future_nonlen: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .entities = &.{}, // why is this the syntax for an empty slice?
                .components = &.{},
                .len = 0,
                .future_entities = &.{},
                .future_components = &.{},
                .future_len = 0,
                .future_nonentities = &.{},
                .future_nonlen = 0,
            };
        }

        pub fn deinit(self: Self) void {
            self.alloc.free(self.entities);
            if (@sizeOf(T) > 0) {
                self.alloc.free(self.components);
            }
        }

        pub fn update(table: *Self) void {
            // we could probably optimize this by doing the removal and merge at the same time
            // but it seems like a tricky algorithm to implement

            std.debug.assert(table.entities.len == table.components.len);
            std.debug.assert(table.future_entities.len == table.future_components.len);

            // remove elements in the non-list
            // while maintaining sorted order
            if (table.future_nonlen > 0) {
                var i: usize = table.find(table.future_nonentities[0]).?;
                var j: usize = 0;
                var k: usize = 0;

                while (j < table.future_nonlen and i + k < table.len) {
                    if (table.future_nonentities[j] == table.entities[i]) {
                        j += 1;
                        k += 1;
                    }
                    table.entities[i] = table.entities[i + k];
                    i += 1;
                }
            }
            table.len -= table.future_nonlen;

            // if we are out of memory, allocate more
            if (table.len + table.future_len > table.entities.len) {
                expandToMin(table.alloc, &table.entities, &table.components, table.len + table.future_len);
            }
            std.mem.copy(Entity, table.entities[table.len..], table.future_entities[0..table.future_len]);
            std.mem.copy(T, table.components[table.len..], table.future_components[0..table.future_len]);

            // merge the future into the past
            // i think this is a linear time algorithm, but not sure
            var len = table.len;
            table.len += table.future_len;

            var i: usize = 0;
            var j: usize = 0;
            while (i < table.len) {
                if (i == len) {
                    table.entities[i] = table.entities[j];
                    table.components[i] = table.components[j];
                    j += 1;
                    len += 1;
                }
                if (table.entities[i] < table.entities[j]) {
                    i += 1;
                } else {
                    const tmp1 = table.entities[i];
                    table.entities[i] = table.entities[j];
                    table.entities[i] = tmp1;
                    const tmp2 = table.components[i];
                    table.components[i] = table.components[j];
                    table.components[i] = tmp2;
                    i += 1;
                }
            }

            table.future_len = 0;
            table.future_nonlen = 0;

            std.debug.print("{any}\n", .{table.entities[0..table.len]});

            std.debug.assert(std.sort.isSorted(Entity, table.entities, {}, less_than_entity));
        }

        /// get the index where an entity resides
        pub fn find(table: Self, entity: Entity) ?usize {
            std.debug.assert(table.entities.len == table.components.len);
            // TODO add a cache?
            return std.sort.binarySearch(Entity, entity, table.entities[0..table.len], {}, order_entity);
        }

        pub fn future_find(table: Self, entity: Entity) ?usize {
            std.debug.assert(table.future_entities.len == table.future_components.len);
            // TODO add a cache?
            return std.sort.binarySearch(Entity, entity, table.future_entities[0..table.future_len], {}, order_entity);
        }

        pub fn future_nonfind(table: Self, entity: Entity) ?usize {
            // TODO add a cache?
            return std.sort.binarySearch(Entity, entity, table.future_nonentities[0..table.future_nonlen], {}, order_entity);
        }

        pub fn set(table: *Self, entity: Entity, value: T) void {
            const ix = table.find(entity) orelse {
                std.log.info("Attempted to set inactive {s} component of entity {} to {}", .{ @typeName(T), entity, value });
                return;
            };
            table.components[ix] = value;
        }

        pub fn get(table: Self, entity: Entity) ?T {
            const ix = table.find(entity) orelse return null;
            return table.components[ix];
        }

        fn expandToMin(alloc: std.mem.Allocator, entities: *[]Entity, components: *[]T, min_size: usize) void {
            std.debug.assert(entities.*.len == components.*.len);
            const new_len: usize = @max(@max(16, 2 * entities.*.len), std.math.ceilPowerOfTwoAssert(usize, min_size));
            var new_entities = alloc.alloc(Entity, new_len) catch @panic("out of memory");
            var new_components = alloc.alloc(T, new_len) catch @panic("out of memory");
            std.mem.copy(Entity, new_entities, entities.*);
            std.mem.copy(T, new_components, components.*);
            alloc.free(entities.*);
            alloc.free(components.*);
            entities.* = new_entities;
            components.* = new_components;
        }

        /// where to insert into entities to maintain sorted order
        fn lowerBound(entities: []Entity, key: Entity) usize {
            // basically same as std.sort.binarySearch
            // but doesn't require finding the item
            var left: usize = 0;
            var right: usize = entities.len;

            while (left < right) {
                const mid = left + (right - left) / 2;
                switch (order_entity({}, key, entities[mid])) {
                    .eq => unreachable,
                    .gt => left = mid + 1,
                    .lt => right = mid,
                }
            }
            return left;
        }

        inline fn expand(alloc: std.mem.Allocator, entities: *[]Entity, components: *[]T) void {
            expandToMin(alloc, entities, components, entities.len + 1);
        }

        pub fn add(table: *Self, entity: Entity, value: T) !void {
            // we maintain a sorted future_components
            _ = table.find(entity) orelse table.future_find(entity) orelse {
                // if we are out of memory, allocate more
                if (table.future_len >= table.future_components.len) {
                    expand(table.alloc, &table.future_entities, &table.future_components);
                }

                // now find where to insert into future entities
                const ix = lowerBound(table.future_entities[0..table.future_len], entity);
                table.future_len += 1;
                std.mem.copyBackwards(
                    Entity,
                    table.future_entities[ix + 1 .. table.future_len],
                    table.future_entities[ix .. table.future_len - 1],
                );
                std.mem.copyBackwards(
                    T,
                    table.future_components[ix + 1 .. table.future_len],
                    table.future_components[ix .. table.future_len - 1],
                );

                table.future_entities[ix] = entity;
                table.future_components[ix] = value;
                std.debug.assert(std.sort.isSorted(Entity, table.future_entities, {}, less_than_entity));
                return;
            };
            std.log.info("Attempted to add active {s} component of entity {} with new value {}", .{ @typeName(T), entity, value });
        }

        pub fn remove(table: *Self, entity: Entity) void {
            _ = table.future_nonfind(entity) orelse {
                if (table.find(entity) == null) {
                    std.log.info("Attempted to remove {s} component of entity {} which does not have that component", .{ @typeName(T), entity });
                    return;
                }

                if (table.future_nonlen >= table.future_nonentities.len) {
                    const new_len: usize = @max(16, 2 * table.future_nonentities.len);
                    var new_nonentities = table.alloc.alloc(Entity, new_len) catch @panic("out of memory");
                    std.mem.copy(Entity, new_nonentities, table.future_nonentities);
                    table.alloc.free(table.future_nonentities);
                    table.future_nonentities = new_nonentities;
                }

                const ix = lowerBound(table.future_nonentities[0..table.future_nonlen], entity);
                table.future_nonlen += 1;
                std.mem.copyBackwards(
                    Entity,
                    table.future_nonentities[ix + 1 .. table.future_nonlen],
                    table.future_nonentities[ix .. table.future_nonlen - 1],
                );

                table.future_nonentities[ix] = entity;
                std.debug.assert(std.sort.isSorted(Entity, table.future_nonentities, {}, less_than_entity));
                return;
            };
            std.log.info("Attempted to remove {s} component of entity {} which is already being removed", .{ @typeName(T), entity });
        }
    };
}

const _E = struct {
    pos: @Vector(4, f32),
    hp: u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var ctx = Context(_E).init(alloc);
    defer ctx.deinit();

    const e0 = ctx.create();
    ctx.set(e0, .hp, 3);
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});

    ctx.add(e0, .hp, 4);
    ctx.update();
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});
    ctx.set(e0, .hp, 5);
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});

    const e1 = ctx.create();
    const e2 = ctx.create();
    const e3 = ctx.create();
    const e4 = ctx.create();
    ctx.add(e3, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.add(e1, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.add(e2, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.add(e0, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.add(e4, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.update();

    ctx.remove(e3, .pos);
    ctx.update();
}

test "simple test" {}
