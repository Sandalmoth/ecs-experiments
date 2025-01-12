const std = @import("std");

const Key = @import("main.zig").Key;
const BlockPool = @import("main.zig").BlockPool;
const KeyGenerator = @import("main.zig").KeyGenerator;
const WorldABC = @import("main.zig").World(struct {
    a: u32,
    b: u32,
    c: u32,
}, struct {});
const WorldPV = @import("main.zig").World(struct {
    pos: Vec2d,
    vel: Vec2d,
}, struct {});
const WorldPV2 = @import("main.zig").World(struct {
    pos: Vec2d,
    vel: Vec2d,
    c1: u32,
    c2: u32,
    c3: u32,
    c4: u32,
    c5: u32,
}, struct {});

var acc: u64 = 0;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try benchgo(alloc);
    // try bench10(alloc);
}

const Vec2d = struct { x: f64, y: f64 };

fn addPosVel(view: WorldPV.WorldView(.{
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

fn insertVel(view: WorldPV.WorldView(.{
    .component_read = &.{.pos},
})) !void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.pos},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            try view.world.queueInsert(entity.key(), .vel, .{ .x = 0, .y = 0 });
        }
    }
}

fn removeVel(view: WorldPV.WorldView(.{
    .component_read = &.{ .pos, .vel },
})) !void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{ .pos, .vel },
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            try view.world.queueRemove(entity.key(), .vel);
        }
    }
}

fn addPosVel2(view: WorldPV2.WorldView(.{
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

        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
                .vel = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try world.eval(addPosVel);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- query, sparse ---\n", .{});
    for (ns) |n| {
        if (n == 1024 * 1024) continue; // skip in this case
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();

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
        try world.eval(addPosVel);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- query, fragmented ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var world = WorldPV2.init(&pool, &keygen);
        defer world.deinit();

        var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
        const rand = rng.random();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            var t = WorldPV2.Template{
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
        try world.eval(addPosVel2);
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- create ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();

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

        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();

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

        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();

        for (0..n) |i| {
            const x: f64 = @floatFromInt(i);
            _ = try world.queueCreate(.{
                .pos = .{ .x = x, .y = x },
            });
        }
        try world.resolveQueues();

        var timer = try std.time.Timer.start();
        try world.eval(insertVel);
        try world.resolveQueues();
        try world.eval(removeVel);
        try world.resolveQueues();
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }

    std.debug.print("--- create world ---\n", .{});
    for (ns) |n| {
        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var timer = try std.time.Timer.start();
        var world = WorldPV.init(&pool, &keygen);
        defer world.deinit();
        const t = @as(f64, @floatFromInt(timer.read())) / @as(f64, @floatFromInt(n));

        std.debug.print("{}\t{d:.2}\n", .{ n, t });
    }
}

var s0_it: usize = 0;
fn system0(view: WorldABC.WorldView(.{
    .component_read = &.{ .b, .c },
    .component_read_write = &.{.a},
})) void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{ .b, .c },
        .include_read_write = &.{.a},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            entity.getPtr(.a).* *%= (entity.get(.b) +% entity.get(.c));
            s0_it += 1;
        }
    }
}

var s1_it: usize = 0;
fn system1(view: WorldABC.WorldView(.{
    .component_read = &.{.b},
    .component_read_write = &.{.a},
})) void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.b},
        .include_read_write = &.{.a},
    });
    while (page_iterator.next()) |page| {
        var entity_iterator = page.entityIterator();
        while (entity_iterator.next()) |entity| {
            entity.getPtr(.a).* +%= entity.get(.b);
            s1_it += 1;
        }
    }
}

var s2_it: usize = 0;
fn system2(view: WorldABC.WorldView(.{
    .component_read = &.{.a},
})) void {
    var page_iterator = view.pageIterator(.{
        .include_read = &.{.a},
    });
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

    for (6..18) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var pool = BlockPool.init(alloc);
        defer pool.deinit();
        var keygen = KeyGenerator{};

        var world = WorldABC.init(&pool, &keygen);
        defer world.deinit();

        var entities = try std.ArrayList(Key).initCapacity(alloc, n);
        defer entities.deinit();

        var timer = try std.time.Timer.start();

        for (0..n) |i| {
            var t = WorldABC.Template{};
            if (rand.float(f32) < 0.3) t.a = @intCast(i);
            if (rand.float(f32) < 0.6) t.b = @intCast(i);
            if (rand.float(f32) < 0.9) t.c = @intCast(i);
            const e = try world.queueCreate(t);
            try entities.append(e);
        }
        try world.resolveQueues();
        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        // skip del for now, maybe later

        s0_it = 0;
        try world.eval(system0);
        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s0_it));

        s1_it = 0;
        try world.eval(system1);
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s1_it));

        s2_it = 0;
        try world.eval(system2);
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
