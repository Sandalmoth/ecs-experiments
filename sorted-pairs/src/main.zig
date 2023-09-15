const std = @import("std");

pub const Entity = u32;
fn order_entity(context: void, lhs: Entity, rhs: Entity) std.math.Order {
    _ = context;
    return std.math.order(lhs, rhs);
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

        pub fn init(alloc: std.mem.Allocator) !Self {
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
                var table = try alloc.create(Table((TFields[i].type)));
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
            // const ix_table: usize = @intFromEnum(component);
            // var table: *Table(TFields[ix_table].type) = @ptrFromInt(ctx.components[ix_table]);
            return table.get(entity);
        }

        pub fn set(ctx: *Self, entity: Entity, comptime component: Component, value: fieldType(component)) void {
            var table = ctx.getTable(component);
            // const ix_table: usize = @intFromEnum(component);
            // var table: *Table(TFields[ix_table].type) = @ptrFromInt(ctx.components[ix_table]);
            table.set(entity, value);
        }

        pub fn add(ctx: *Self, entity: Entity, comptime component: Component, value: fieldType(component)) !void {
            var table = ctx.getTable(component);
            // const ix_table: usize = @intFromEnum(component);
            // var table: *Table(TFields[ix_table].type) = @ptrFromInt(ctx.components[ix_table]);
            try table.add(entity, value);
        }

        // pub fn remove(ctx: *Self, entity: Entity, comptime component: Component) void {
        //     var table = ctx.getTable(component);
        //     // const ix_table: usize = @intFromEnum(component);
        //     // var table: *Table(TFields[ix_table].type) = @ptrFromInt(ctx.components[ix_table]);
        //     table.remove(entity);
        // }
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
        // future_nonentities: []Entity,
        // future_nonentity_len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .entities = &.{}, // why is this the syntax for an empty slice?
                .components = &.{},
                .len = 0,
                .future_entities = &.{},
                .future_components = &.{},
                .future_len = 0,
            };
        }

        pub fn deinit(self: Self) void {
            self.alloc.free(self.entities);
            if (@sizeOf(T) > 0) {
                self.alloc.free(self.components);
            }
        }

        pub fn update(table: *Self) void {
            std.debug.assert(table.entities.len == table.components.len);
            // if we are out of memory, allocate more
            if (table.len + table.future_len > table.entities.len) {
                expandToMin(table.alloc, &table.entities, &table.components, table.len + table.future_len);
            }
            std.mem.copy(Entity, table.entities[table.len..], table.future_entities[0..table.future_len]);
            std.mem.copy(T, table.components[table.len..], table.future_components[0..table.future_len]);
            table.len += table.future_len;

            if (table.len < 2) {
                return; // sorted by default
            }

            // the best known shell sort gap sequence according to arXiv:2112.11112
            const gaps = [_]usize{ 743745, 331490, 147748, 65853, 29351, 13082, 5831, 2599, 1158, 512, 230, 102, 45, 20, 9, 4, 1 };
            var first_gap: usize = 0;
            while (gaps[first_gap] > table.len) : (first_gap += 1) {}

            for (gaps[first_gap..]) |gap| {
                var i: usize = gap;
                while (i < table.len) : (i += 1) {
                    var j: usize = i;
                    const tmp1 = table.entities[i];
                    const tmp2 = table.components[i];
                    while (j >= gap and table.entities[j - gap] > tmp1) : (j -= gap) {
                        table.entities[j] = table.entities[j - gap];
                        table.components[j] = table.components[j - gap];
                    }
                    table.entities[j] = tmp1;
                    table.components[j] = tmp2;
                }
            }
            std.debug.print("{any}\n", .{table.entities[0..table.len]});
        }

        /// get the index where an entity resides
        pub fn find(table: Self, entity: Entity) ?usize {
            std.debug.assert(table.entities.len == table.components.len);
            // TODO add a cache?
            return std.sort.binarySearch(Entity, entity, table.entities[0..table.len], {}, order_entity);
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

        inline fn expand(alloc: std.mem.Allocator, entities: *[]Entity, components: *[]T) void {
            expandToMin(alloc, entities, components, entities.len + 1);
        }

        pub fn add(table: *Self, entity: Entity, value: T) !void {
            // but like: how do we know that we haven't already queued this up for creation?
            // we could maintain a sorted future_components and then do a merge during update, instead of a sort?
            const ix = table.find(entity) orelse {
                // if we are out of memory, allocate more
                if (table.future_len >= table.future_components.len) {
                    expand(table.alloc, &table.future_entities, &table.future_components);
                }

                table.future_entities[table.future_len] = entity;
                table.future_components[table.future_len] = value;
                table.future_len += 1;
                return;
            };
            _ = ix;
            std.log.info("Attempted to add active {s} component of entity {} with new value {}", .{ @typeName(T), entity, value });
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

    var ctx = try Context(_E).init(alloc);
    defer ctx.deinit();

    const e0 = ctx.create();
    ctx.set(e0, .hp, 3);
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});

    try ctx.add(e0, .hp, 4);
    ctx.update();
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});
    ctx.set(e0, .hp, 5);
    std.debug.print("{?}\n", .{ctx.get(e0, .hp)});

    const e1 = ctx.create();
    const e2 = ctx.create();
    const e3 = ctx.create();
    try ctx.add(e3, .pos, @as(@Vector(4, f32), @splat(0)));
    try ctx.add(e1, .pos, @as(@Vector(4, f32), @splat(0)));
    try ctx.add(e2, .pos, @as(@Vector(4, f32), @splat(0)));
    try ctx.add(e0, .pos, @as(@Vector(4, f32), @splat(0)));
    ctx.update();
}

test "simple test" {}
