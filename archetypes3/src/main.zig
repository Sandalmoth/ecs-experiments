const builtin = @import("builtin");
const std = @import("std");

const BlockPool = @import("block_pool.zig").BlockPool;

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
const single_threaded = builtin.single_threaded;

pub fn ECS(comptime Components: type, comptime Queues: type) type {
    return struct {
        const Self = @This();

        const Component = std.meta.FieldEnum(Components);
        const n_components = std.meta.fields(Component).len;
        fn ComponentType(comptime c: Component) type {
            return @FieldType(Components, @tagName(c));
        }

        const Queue = std.meta.FieldEnum(Queues);
        const n_queues = std.meta.fields(Queue).len;
        fn QueueType(comptime c: Queue) type {
            return @FieldType(Queues, @tagName(c));
        }

        const Entity = u64;
        const Archetype = std.StaticBitSet(n_components);

        const EntityTemplate = gentype: {
            // take the Components type and generate a variant where all fields are optionals
            var fields: [n_components]std.builtin.Type.StructField = undefined;
            @memcpy(&fields, std.meta.fields(Components));
            for (0..fields.len) |i| {
                fields[i].default_value = &@as(?fields[i].type, null);
                fields[i].type = @Type(.{ .optional = .{ .child = fields[i].type } });
            }
            const typeinfo: std.builtin.Type = .{ .@"struct" = std.builtin.Type.Struct{
                .layout = .auto,
                .fields = &fields,
                .decls = &.{},
                .is_tuple = false,
            } };
            break :gentype @Type(typeinfo);
        };

        const Page = struct {
            components: [n_components]usize,
        };

        // pub const QueryInfo = struct {
        //     include_read: []const Component,
        //     include_modify: []const Component,
        //     optional_read: []const Component,
        //     optional_modify: []const Component,
        //     exclude: []const Component,
        // };
        // pub fn Query(comptime info: QueryInfo) type {
        //     _ = info;
        //     return struct {};
        // }

        const World = struct {
            // TODO check the performance implications of linear archetype searches
            // it should primarily impact create or insert/remove-component operations i think
            ecs: *Self,
            pages: std.ArrayList(*Page),
            archetypes: std.ArrayList(Archetype),
            // insert_queues:
            // remove_queues:
            // create_queue
            // destroy_queue

            pub fn create(world: *World, template: EntityTemplate) Entity {
                // TODO verify that data has no extra fields (sanity check/reduce bugs)
                var archetype = Archetype.initEmpty();
                inline for (std.meta.fields(EntityTemplate), 0..) |field, i| {
                    if (@field(template, field.name) != null) archetype.set(i);
                }
                std.debug.print("{}\n", .{archetype});
                std.debug.print("{s}\n", .{@typeName(@TypeOf(template))});
                return world.ecs.newEntity();
            }
        };

        alloc: std.mem.Allocator, // used sparingly, mostly for large allocations inside BlockPool
        id_counter: u64, // any number except 0 is fine
        pool: BlockPool,

        fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .id_counter = 1,
                .pool = BlockPool.init(alloc),
            };
        }

        fn deinit(ecs: *Self) void {
            ecs.pool.deinit();
            ecs.* = undefined;
        }

        fn newEntity(ecs: *Self) u64 {
            // xorshift* with 2^64 - 1 period (0 is fixed point, and also the null entity)
            var x = ecs.id_counter;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            ecs.id_counter = x;
            return x *% 0x2545F4914F6CDD1D;
        }

        pub fn initWorld(ecs: *Self) World {
            return .{
                .ecs = ecs,
                .pages = std.ArrayList(*Page).init(ecs.alloc),
                .archetypes = std.ArrayList(Archetype).init(ecs.alloc),
            };
        }

        pub fn deinitWorld(ecs: *Self, world: *World) void {
            _ = ecs;
            world.pages.deinit();
            world.archetypes.deinit();
        }
    };
}

const TestComponents = struct {
    a: u128,
    b: f32,
};
const TestQueues = struct {
    c: i64,
};

const Foo = struct {
    a: ?u32 = null,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const ecstype = ECS(TestComponents, TestQueues);
    var ecs = ecstype.init(alloc);
    defer ecs.deinit();

    std.debug.print("{}\n", .{ecs});
    std.debug.print("{s}\n", .{@typeName(ecstype.ComponentType(.b))});

    const zero: u64 = 0;
    std.debug.print("{}\n", .{std.hash.XxHash3.hash(0, std.mem.asBytes(&zero))});

    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});

    var world = ecs.initWorld();
    defer ecs.deinitWorld(&world);

    _ = world.create(.{});
    _ = world.create(.{ .a = 123 });
    _ = world.create(.{ .b = 2.0 });
    _ = world.create(.{ .a = 1337, .b = 33.4 });

    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .a)});
    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .b)});
}
