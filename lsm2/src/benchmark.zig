const std = @import("std");
const Storage = @import("main.zig").Storage;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench9(alloc);
    // try bench8(alloc);
    // try bench7(alloc);
}

fn bench9(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "len\tadd\tcpct\tgetc\tgeth\tchrn\n",
        .{},
    );
    try stdout.print(
        "\tns\tus\tns\tns\tus\n",
        .{},
    );

    var s = Storage.init(f64, alloc);
    defer s.deinit(f64);

    var a = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer a.deinit();

    const dn = 2048;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s.add(f64, x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
        }
        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nchu: usize = 0;
        for (0..100) |_| {
            s.compact(f64);
            for (0..dn) |_| {
                const i = rand.uintLessThan(usize, a.items.len);
                const x = a.items[i];
                if (i < rand.uintLessThan(usize, a.items.len)) {
                    if (s.has(x)) {
                        s.del(f64, x);
                    } else {
                        s.add(f64, x, @floatFromInt(x));
                    }
                    nchu += 1;
                }
            }
        }
        const t_churn = 1e-3 * @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nchu));

        var nget: usize = 0;
        for (a.items, 0..) |x, i| {
            if (i < rand.uintLessThanBiased(usize, a.items.len)) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_geth = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        s.compact(f64);
        const t_compact = 1e-3 * @as(f64, @floatFromInt(timer.lap()));

        nget = 0;
        for (a.items, 0..) |x, i| {
            if (i < rand.uintLessThanBiased(usize, a.items.len)) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_getc = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        try stdout.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s.len, t_add, t_compact, t_getc, t_geth, t_churn },
        );
    }

    std.debug.print("{}\n", .{acc});
}

fn bench8(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "len\tadd\tcpct\tgetc\tgeth\tchrn\n",
        .{},
    );
    try stdout.print(
        "\tns\tus\tns\tns\tus\n",
        .{},
    );

    var s = Storage.init(f64, alloc);
    defer s.deinit(f64);

    var a = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer a.deinit();

    const dn = 2048;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s.add(f64, x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
        }
        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nchu: usize = 0;
        for (0..dn) |_| {
            const i = rand.uintLessThan(usize, a.items.len);
            const x = a.items[i];
            if (i < rand.uintLessThan(usize, a.items.len)) {
                if (s.has(x)) {
                    s.del(f64, x);
                } else {
                    s.add(f64, x, @floatFromInt(x));
                }
                nchu += 1;
            }
        }
        const t_churn = 1e-3 * @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nchu));

        var nget: usize = 0;
        for (a.items, 0..) |x, i| {
            if (i < rand.uintLessThanBiased(usize, a.items.len)) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_geth = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        s.compact(f64);
        const t_compact = 1e-3 * @as(f64, @floatFromInt(timer.lap()));

        nget = 0;
        for (a.items, 0..) |x, i| {
            if (i < rand.uintLessThanBiased(usize, a.items.len)) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_getc = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        try stdout.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s.len, t_add, t_compact, t_getc, t_geth, t_churn },
        );
    }

    std.debug.print("{}\n", .{acc});
}

fn bench7(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "len\tadd\tcpct\tgetc\tgeth\n",
        .{},
    );
    try stdout.print(
        "\tns\tus\tns\tns\n",
        .{},
    );

    var s = Storage.init(f64, alloc);
    defer s.deinit(f64);

    var a = try std.ArrayList(u64).initCapacity(alloc, 2 * 2 * 65536);
    defer a.deinit();

    const dn = 1024;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s.add(f64, x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
        }

        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nget: usize = 0;
        for (a.items) |x| {
            if (rand.float(f32) < 0.5) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_geth = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        s.compact(f64);
        const t_compact = 1e-3 * @as(f64, @floatFromInt(timer.lap()));

        nget = 0;
        for (a.items) |x| {
            if (rand.float(f32) < 0.5) {
                if (s.has(x)) {
                    acc +%= x;
                }
                nget += 1;
            }
        }
        const t_getc = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nget));

        try stdout.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s.len, t_add, t_compact, t_getc, t_geth },
        );
    }

    std.debug.print("{}\n", .{acc});
}
