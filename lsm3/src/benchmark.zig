const std = @import("std");
const Storage = @import("main.zig").StorageImpl;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench10(alloc);
    try bench9(alloc);
}

fn bench10(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "lenb\tlenr\tlenb\tlenr\tadd\tcpct\tgetc\tgeth\tchrn\n",
        .{},
    );
    try stdout.print(
        "\t\t\t\tns\tus\tns\tns\tus\n",
        .{},
    );

    var s0 = Storage(f64).init(alloc);
    defer s0.deinit();
    var s1 = Storage(f64).init(alloc);
    defer s1.deinit();

    var a = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer a.deinit();
    var b = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer b.deinit();

    const dn = 2048;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s0.add(x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
            s1.add(x, @floatFromInt(x));
            try b.append(x);
            try b.append(rand.int(u64) | 1);
        }
        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nchu: usize = 0;
        for (0..100) |_| {
            s0.compact();
            s1.compact();
            for (0..dn) |_| {
                const i = rand.uintLessThan(usize, a.items.len);
                const j = rand.uintLessThan(usize, b.items.len);
                const x = a.items[i];
                const y = b.items[j];
                if (i < rand.uintLessThan(usize, a.items.len)) {
                    if (s0.has(x)) {
                        s0.del(x);
                    } else {
                        s0.add(x, @floatFromInt(x));
                    }
                    nchu += 1;
                }
                if (j < rand.uintLessThan(usize, b.items.len)) {
                    if (s1.has(y)) {
                        s1.del(y);
                    } else {
                        s1.add(y, @floatFromInt(y));
                    }
                    nchu += 1;
                }
            }
        }
        const t_churn = 1e-3 * @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nchu));

        // s0.compact();
        // s1.compact();
        // timer.reset();

        var nit: usize = 0;
        {
            for (s0.run.keys[0..s0.run.len]) |k| {
                acc +%= k;
                nit += 1;
            }
            var it = s0.bucket.data.keyIterator();
            while (it.next()) |k| {
                acc +%= k.*;
                nit += 1;
            }
        }
        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        {
            for (s1.run.keys[0..s1.run.len]) |k| {
                acc +%= k;
                nit += 1;
            }
            var it = s1.bucket.data.keyIterator();
            while (it.next()) |k| {
                acc +%= k.*;
                nit += 1;
            } f
        }
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        {
            for (s1.run.keys[0..s1.run.len]) |k| {
                if (!s0.has(k)) continue;
                acc +%= k;
                nit += 1;
            }
            var it = s1.bucket.data.keyIterator();
            while (it.next()) |k| {
                if (!s0.has(k.*)) continue;
                acc +%= k.*;
                nit += 1;
            }
        }
        const t_it12a = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        {
            var i: usize = 0;
            var j: usize = 0;

            while (true) {
                if (i == s0.run.len)
                if (s0.run.keys[i])
            }
        }
        const t_it12b = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        try stdout.print(
            "{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ t_add, t_churn, t_it0, t_it1, t_it12a, t_it12b },
        );
    }

    std.debug.print("{}\n", .{acc});
}

fn bench9(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "lenb\tlenr\tlenb\tlenr\tadd\tcpct\tgetc\tgeth\tchrn\n",
        .{},
    );
    try stdout.print(
        "\t\t\t\tns\tus\tns\tns\tus\n",
        .{},
    );

    var s = Storage(f64).init(alloc);
    defer s.deinit();

    var a = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer a.deinit();

    const dn = 2048;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s.add(x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
        }
        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nchu: usize = 0;
        for (0..100) |_| {
            s.compact();
            for (0..dn) |_| {
                const i = rand.uintLessThan(usize, a.items.len);
                const x = a.items[i];
                if (i < rand.uintLessThan(usize, a.items.len)) {
                    if (s.has(x)) {
                        s.del(x);
                    } else {
                        s.add(x, @floatFromInt(x));
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

        const nb_pre = s.bucket.count();
        const nr_pre = s.run.len;

        s.compact();
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
            "{}\t{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ nb_pre, nr_pre, s.bucket.count(), s.run.len, t_add, t_compact, t_getc, t_geth, t_churn },
        );
    }

    std.debug.print("{}\n", .{acc});
}
