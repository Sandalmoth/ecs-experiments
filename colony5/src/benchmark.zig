const std = @import("std");
const Colony = @import("main.zig").Colony;

pub fn main() !void {
    const seed: u64 = @bitCast(std.time.microTimestamp());
    try ref1(seed);
    try bench1(seed);
}

const T1 = struct {
    x: u64,
    y: u64,

    fn next(t: *T1) void {
        t.x ^= t.y;
        t.y +%= t.x;
        t.x *%= 89;
    }
};

const ns_1 = [_]usize{
    100,
    316,
    1000,
    3162,
    10_000,
    31_623,
    100_000,
    316_228,
    1000_000,
    3162_278,
    10_000_000,
};

fn ref1(seed: u64) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const stdout = std.io.getStdOut().writer();

    var rng = std.Random.DefaultPrng.init(seed);
    const rand = rng.random();

    var t = T1{
        .x = rand.int(u64),
        .y = rand.int(u64),
    };

    var acc: u64 = 0;
    var timer = try std.time.Timer.start();

    try stdout.print("--- std.ArrayList ---\n", .{});
    try stdout.print("n\t\tins1\tdel\tit1\tins2\tit2\n", .{});

    for (ns_1) |n| {
        var a = std.ArrayList(T1).init(alloc);
        defer a.deinit();

        var del = std.ArrayList(usize).init(alloc);
        defer del.deinit();

        timer.reset();
        for (0..n) |i| {
            try a.append(t);
            if (rand.boolean()) try del.append(i);
            t.next();
        }
        const t_ins1 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        std.sort.heap(usize, del.items, {}, std.sort.desc(usize));

        timer.reset();
        for (del.items) |i| _ = a.swapRemove(i);
        const t_del = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (a.items) |_t| acc +%= _t.x *% _t.y;
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (0..n) |_| {
            try a.append(t);
            t.next();
        }
        const t_ins2 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (a.items) |_t| acc +%= _t.x *% _t.y;
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        try stdout.print(
            "{:>12}\t{d:.3}\t{d:.3}\t{d:.3}\t{d:.3}\t{d:.3}\n",
            .{ n, t_ins1, t_del, t_it1, t_ins2, t_it2 },
        );
    }
    std.debug.print("{}\n", .{acc});
}

fn bench1(seed: u64) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const stdout = std.io.getStdOut().writer();

    var rng = std.Random.DefaultPrng.init(seed);
    const rand = rng.random();

    var t = T1{
        .x = rand.int(u64),
        .y = rand.int(u64),
    };

    var acc: u64 = 0;
    var timer = try std.time.Timer.start();

    try stdout.print("--- Colony ---\n", .{});
    try stdout.print("n\t\tins1\tdel\tit1\tins2\tit2\n", .{});

    for (ns_1) |n| {
        var a = Colony(T1).init(alloc);
        defer a.deinit();

        var del = std.ArrayList(*T1).init(alloc);
        defer del.deinit();

        timer.reset();
        for (0..n) |_| {
            const p = try a.insert();
            p.* = t;
            if (rand.boolean()) try del.append(p);
            t.next();
        }
        const t_ins1 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        timer.reset();
        for (del.items) |p| _ = a.erase(p);
        const t_del = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        var it1 = a.iterator();
        while (it1.next()) |p| acc +%= p.x *% p.y;
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (0..n) |_| {
            const p = try a.insert();
            p.* = t;
            t.next();
        }
        const t_ins2 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        var it2 = a.iterator();
        while (it2.next()) |p| acc +%= p.x *% p.y;
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        try stdout.print(
            "{:>12}\t{d:.3}\t{d:.3}\t{d:.3}\t{d:.3}\t{d:.3}\n",
            .{ n, t_ins1, t_del, t_it1, t_ins2, t_it2 },
        );
    }
    std.debug.print("{}\n", .{acc});
}
