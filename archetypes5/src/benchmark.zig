const std = @import("std");

const Key = @import("main.zig").Key;
const BlockPool = @import("main.zig").BlockPool;
const KeyGenerator = @import("main.zig").KeyGenerator;
const CtxPV = @import("main.zig").Context(struct {
    pos: Vec2d,
    vel: Vec2d,
}, struct {}, struct {});
const CtxPV2 = @import("main.zig").Context(struct {
    pos: Vec2d,
    vel: Vec2d,
    c1: u32,
    c2: u32,
    c3: u32,
    c4: u32,
    c5: u32,
}, struct {}, struct {});
const WorldPV = CtxPV.World;
const WorldPV2 = CtxPV2.World;

var acc: u64 = 0;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try benchgo(alloc);
}

const Vec2d = struct { x: f64, y: f64 };

fn addPosVel(view: CtxPV.View(.{
    .component_read = &.{.vel},
    .component_read_write = &.{.pos},
})) void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.vel},
        .include_read_write = &.{.pos},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            const pos = entity.getPtr(.pos);
            const vel = entity.get(.vel);
            pos.x += vel.x;
            pos.y += vel.y;
        }
    }
}

fn insertVel(view: CtxPV.View(.{
    .component_read = &.{.pos},
})) !void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.pos},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            try view.queueInsert(entity.key(), .vel, .{ .x = 0, .y = 0 });
        }
    }
}

fn removeVel(view: CtxPV.View(.{
    .component_read = &.{ .pos, .vel },
})) !void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{ .pos, .vel },
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            try view.queueRemove(entity.key(), .vel);
        }
    }
}

fn addPosVel2(view: CtxPV2.View(.{
    .component_read = &.{.vel},
    .component_read_write = &.{.pos},
})) void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.vel},
        .include_read_write = &.{.pos},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            const pos = entity.getPtr(.pos);
            const vel = entity.get(.vel);
            pos.x += vel.x;
            pos.y += vel.y;
        }
    }
}

fn benchgo(alloc: std.mem.Allocator) !void {
    const ns = [_]usize{ 1, 4, 16, 64, 256, 1024, 16 * 1024, 256 * 1024, 1024 * 1024 };

    std.debug.print("--- query ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV.create(&pool, &keygen);
        defer world.destroy();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
                .vel = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try ctx.eval(world, addPosVel);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- query, sparse ---\n", .{});
    for (ns) |n| {
        if (n == 1024 * 1024) continue; // skip in this case
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV.create(&pool, &keygen);
        defer world.destroy();

        for (0..n * 10) |i| {
            const x: f64 = @floatFromInt(i);
            if (i % 10 == 0) {
                _ = try world.queueCreate(.{
                    .pos = .{ .x = x, .y = x },
                    .vel = .{ .x = x, .y = x },
                });
            } else {
                _ = try world.queueCreate(.{
                    .pos = .{ .x = x, .y = x },
                });
            }
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try ctx.eval(world, addPosVel);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- query, fragmented ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV2.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV2.create(&pool, &keygen);
        defer world.destroy();

        var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
        const rand = rng.random();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            var t = CtxPV2.Template{
                .pos = .{ .x = x, .y = x },
                .vel = .{ .x = x, .y = x },
            };
            if (rand.boolean()) t.c1 = 1;
            if (rand.boolean()) t.c2 = 2;
            if (rand.boolean()) t.c3 = 3;
            if (rand.boolean()) t.c4 = 4;
            if (rand.boolean()) t.c5 = 5;
            _ = try world.queueCreate(t);
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try ctx.eval(world, addPosVel2);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- create ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV.create(&pool, &keygen);
        defer world.destroy();

        _ = try world.queueCreate(.{
            .pos = .{ .x = 0, .y = 0 },
            .vel = .{ .x = 0, .y = 0 },
        });
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
                .vel = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- create, allocating ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV.create(&pool, &keygen);
        defer world.destroy();

        var timer = try std.time.Timer.start();
        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
                .vel = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- add/remove component ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var ctx = try CtxPV.create(&pool);
        defer ctx.destroy();
        var world = try WorldPV.create(&pool, &keygen);
        defer world.destroy();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try ctx.eval(world, insertVel);
        try world.resolveQueues();
        try ctx.eval(world, removeVel);
        try world.resolveQueues();
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }
}
