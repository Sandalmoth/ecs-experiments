const builtin = @import("builtin");
const std = @import("std");

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
const single_threaded = builtin.single_threaded;

const ECSInfo = struct {
    Component: type,
    Queue: type,
};

const Builder = struct {
    const EnumLiteral = @TypeOf(._);

    component_names: []const EnumLiteral,
    // component_names: []const [:0]const u8,
    component_types: []const type,
    queue_names: []const EnumLiteral,
    // queue_names: []const [:0]const u8,
    queue_types: []const type,

    const begin = Builder{
        .component_names = &.{},
        .component_types = &.{},
        .queue_names = &.{},
        .queue_types = &.{},
    };

    fn addComponent(
        comptime builder: Builder,
        comptime component_name: EnumLiteral,
        comptime component_type: type,
    ) Builder {
        return Builder{
            .component_names = builder.component_names ++ .{component_name},
            .component_types = builder.component_types ++ .{component_type},
            .queue_names = builder.queue_names,
            .queue_types = builder.queue_types,
        };
    }

    fn addQueue(
        comptime builder: Builder,
        comptime queue_name: EnumLiteral,
        comptime queue_type: type,
    ) Builder {
        return Builder{
            .component_names = builder.component_names,
            .component_types = builder.component_types,
            .queue_names = builder.queue_names ++ .{queue_name},
            .queue_types = builder.queue_types ++ .{queue_type},
        };
    }

    fn end(comptime builder: Builder) ECSInfo {
        const n_components = builder.component_names.len;
        const n_queues = builder.queue_names.len;
        var component_enum_fields: [n_components]std.builtin.Type.EnumField = undefined;
        var queue_enum_fields: [n_queues]std.builtin.Type.EnumField = undefined;
        var decls = [_]std.builtin.Type.Declaration{};

        inline for (builder.component_names, 0..) |name, i| {
            component_enum_fields[i] = .{
                .name = @tagName(name),
                .value = i,
            };
        }

        inline for (builder.queue_names, 0..) |name, i| {
            queue_enum_fields[i] = .{
                .name = @tagName(name),
                .value = i,
            };
        }

        return .{
            .Component = @Type(.{ .@"enum" = .{
                .tag_type = std.math.IntFittingRange(0, n_components - 1),
                .fields = &component_enum_fields,
                .decls = &decls,
                .is_exhaustive = true,
            } }),
            .Queue = @Type(.{ .@"enum" = .{
                .tag_type = std.math.IntFittingRange(0, n_queues - 1),
                .fields = &queue_enum_fields,
                .decls = &decls,
                .is_exhaustive = true,
            } }),
        };
    }
};

pub fn ECS(comptime info: ECSInfo) type {

    // var enumFields: [builder.compo.len]std.builtin.Type.EnumField = undefined;

    return struct {
        const Component = info.Component;
        const Queue = info.Queue;

        fn foo(c: Component) void {
            std.debug.print("{s}\n", .{@tagName(c)});
        }

        // const Component = @Type(.{
        //     .@"enum" = .{
        //         .tag_type = u32,
        //         .fields
        //         .decls = &.{},
        //         .is_exhaustive = true,
        //     }
        // });
        // const Component = std.meta.FieldEnum(Components);
        // const n_components = std.meta.fields(Component).len;

        // const Queue = blk: {
        //     break :blk void;
        // };
        // const n_queues = std.meta.fields(Queue).len;

        // fn ComponentType(comptime c: Component) type {
        //     return std.meta.fields(Components)[@intFromEnum(c)].type;
        // }
        // fn QueueType(comptime q: Queue) type {
        //     return std.meta.fields(Queues)[@intFromEnum(q)].type;
        // }

        // const Entity = u64;

        // const BlockPool = @import("block_pool.zig").BlockPool;

        // const Page = struct {};

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

        // const World = struct {};
    };
}

pub fn main() !void {
    const ecs = ECS(
        Builder.begin
            .addComponent(.a, u32)
            .addComponent(.b, f32)
            .addQueue(.c, i64)
            .end(),
    );

    std.debug.print("{}\n", .{ecs});
    ecs.foo(.a);
    ecs.foo(.b);
}
