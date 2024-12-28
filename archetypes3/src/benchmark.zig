const std = @import("std");
const ECS = @import("main.zig").ECS(struct {
    a: u32,
    b: u32,
    c: u32,
}, struct {});

var acc: u64 = 0;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench10(alloc);
}

var s0_it: usize = 0;
const system0_info = ECS.ScheduleInfo.init(.{ .read_write = &[_]ECS.Component{ .a, .b, .c } });
fn system0(view: ECS.ScheduleView) void {
    var page_iterator = view.pageIterator(.{ .include_read = &[_]ECS.Component{ .a, .b, .c } });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            entity.getPtr(.a).* *%= (entity.get(.b) +% entity.get(.c));
            s0_it += 1;
        }
    }
}

var s1_it: usize = 0;
const system1_info = ECS.ScheduleInfo.init(.{ .read_write = &[_]ECS.Component{ .a, .b } });
fn system1(view: ECS.ScheduleView) void {
    var page_iterator = view.pageIterator(.{ .include_read = &[_]ECS.Component{ .a, .b } });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            entity.getPtr(.a).* +%= entity.get(.b);
            s1_it += 1;
        }
    }
}

var s2_it: usize = 0;
const system2_info = ECS.ScheduleInfo.init(.{ .read_write = &[_]ECS.Component{.a} });
fn system2(view: ECS.ScheduleView) void {
    var page_iterator = view.pageIterator(.{ .include_read = &[_]ECS.Component{.a} });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            acc += entity.get(.a);
            s2_it += 1;
        }
    }
}

fn bench10(alloc: std.mem.Allocator) !void {
    var rng = std.Random.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    var rand = rng.random();

    // std.debug.print("len_1\t\t\tlen_2\t\t\tlen_3\t\t\tins\tdel\tcopy\tdeinit\titer_1\titer_12\titer_123\n", .{});

    for (6..18) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var ecs = ECS.init(alloc);
        defer ecs.deinit();

        var world = ecs.initWorld();
        defer ecs.deinitWorld(&world);

        var entities = try std.ArrayList(u64).initCapacity(alloc, n);
        defer entities.deinit();

        var timer = try std.time.Timer.start();

        for (0..n) |i| {
            var t = ECS.EntityTemplate{};
            if (rand.float(f32) < 0.3) t.a = @intCast(i);
            if (rand.float(f32) < 0.6) t.b = @intCast(i);
            if (rand.float(f32) < 0.9) t.c = @intCast(i);
            const e = try world.queueCreate(t);
            try entities.append(e);
        }
        try world.processQueues();
        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        // skip del for now, maybe later

        s0_it = 0;
        world.eval(system0_info, system0);
        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s0_it));

        s1_it = 0;
        world.eval(system1_info, system1);
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s1_it));

        s2_it = 0;
        world.eval(system2_info, system2);
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s2_it));

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                n,
                t_ins,
                t_it0,
                t_it1,
                t_it2,
            },
        );
    }

    std.debug.print("{}\n", .{acc});
}
