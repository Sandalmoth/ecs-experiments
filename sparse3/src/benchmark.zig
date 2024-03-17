const std = @import("std");
const Storage = @import("main.zig").Storage;
const Table = @import("main.zig").Table;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench6(alloc);
    try bench5(alloc);
}

const Bench6 = struct {
    int1: u32,
    int2: u32,
    int3: u32,
};
fn bench6(alloc: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    var rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "len_1\tlen_2\tlen_3\tins\tdel\titer_1\titer_12\titer_123\n",
        .{},
    );

    for (6..17) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);
        _ = arena.reset(.retain_capacity);

        const t = try Table(Bench6).create(alloc);
        defer t.destroy();

        var timer = try std.time.Timer.start();

        var n_ins: usize = 0;
        for (0..n) |_| {
            const e = try t.spawn();
            if (rand.float(f32) < 0.3) t.add(.int1, e, e);
            if (rand.float(f32) < 0.6) t.add(.int2, e, e);
            if (rand.float(f32) < 0.9) t.add(.int3, e, e);
            n_ins += 1;
        }
        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n_ins));

        var _it = t.entities.iterator();
        const it = _it.iter();
        var to_del = std.ArrayList(u32).init(arena_alloc);
        while (it.next()) |e| {
            if (rand.float(f32) < 0.5) {
                try to_del.append(e);
            }
        }
        for (to_del.items) |e| {
            t.kill(e);
        }
        const t_del = @as(f64, @floatFromInt(timer.lap())) /
            @as(f64, @floatFromInt(to_del.items.len));

        var q123 = t.query(&.{ .int1, .int2, .int3 });
        const it123 = q123.iter();
        var n123: usize = 0;
        while (it123.next()) |k| {
            const v1 = t.get(.int1, k).?;
            const v2 = t.get(.int2, k).?;
            const v3 = t.get(.int3, k).?;
            v2.* +%= v3.* *% v1.*;
            n123 += 1;
        }
        const t_it123 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n123));

        var q12 = t.query(&.{ .int1, .int2 });
        const it12 = q12.iter();
        var n12: usize = 0;
        while (it12.next()) |k| {
            const v1 = t.get(.int1, k).?;
            const v2 = t.get(.int2, k).?;
            v2.* ^= v1.*;
            n12 += 1;
        }
        const t_it12 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n12));

        var q1 = t.query(&.{.int1});
        const it1 = q1.iter();
        var n1: usize = 0;
        while (it1.next()) |k| {
            const v1 = t.get(.int1, k).?;
            acc += v1.*;
            n1 += 1;
        }
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n1));

        try stdout.print(
            "{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                t.data(.int1).len,
                t.data(.int2).len,
                t.data(.int3).len,
                t_ins,
                t_del,
                t_it1,
                t_it12,
                t_it123,
            },
        );
    }

    std.debug.print("\n{}\n", .{acc});
}

fn bench5(alloc: std.mem.Allocator) !void {
    // a more ecs-like test where one component is iterated and compared with lookups in the other
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    std.debug.print("len_1\tlen_2\tlen_3\tins\titer_1\titer_12\titer_123\n", .{});

    for (6..17) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var s0 = try Storage.init(u32, alloc);
        defer s0.deinit(u32);
        var s1 = try Storage.init(u32, alloc);
        defer s1.deinit(u32);
        var s2 = try Storage.init(u32, alloc);
        defer s2.deinit(u32);

        var timer = try std.time.Timer.start();

        // insert some random values
        for (0..2 * n) |_| {
            const k = rng.random().int(u32) % n;
            if (s0.get(u32, k)) |_| {} else {
                try s0.add(u32, k, k);
            }
        }

        for (0..n) |_| {
            const k = rng.random().int(u32) % n;
            if (s1.get(u32, k)) |_| {} else {
                try s1.add(u32, k, k);
            }
        }

        for (0..n / 2) |_| {
            const k = rng.random().int(u32) % n;
            if (s2.get(u32, k)) |_| {} else {
                try s2.add(u32, k, k);
            }
        }

        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        // then do some iteration passes over one component while fetching the others
        var nit: usize = 0;
        var _it2 = s2.iterator();
        const it2 = _it2.iter();
        while (it2.next()) |k| {
            const x = s0.get(u32, k) orelse continue;
            const y = s1.get(u32, k) orelse continue;
            const v = s2.get(u32, k).?.*;
            x.* *%= (y.* +% v);
            nit += 1;
        }
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var _it1 = s1.iterator();
        const it1 = _it1.iter();
        while (it1.next()) |k| {
            const x = s0.get(u32, k) orelse continue;
            const v = s1.get(u32, k).?.*;
            x.* +%= v;
            nit += 1;
        }
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var _it0 = s0.iterator();
        const it0 = _it0.iter();
        while (it0.next()) |k| {
            const v = s0.get(u32, k).?.*;
            acc += v;
            nit += 1;
        }

        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        std.debug.print(
            "{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                s0.len,
                s1.len,
                s2.len,
                t_ins,
                t_it0,
                t_it1,
                t_it2,
            },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}
