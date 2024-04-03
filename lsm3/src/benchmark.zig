const std = @import("std");
const Storage = @import("main.zig").StorageImpl;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench9(alloc);
}

fn bench9(alloc: std.mem.Allocator) !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    const rand = rng.random();
    var stdout = std.io.getStdOut().writer();

    try stdout.print(
        "lenb\tlenr\tadd\tcpct\tgetc\tgeth\tchrn\n",
        .{},
    );
    try stdout.print(
        "\t\tns\tus\tns\tns\tus\n",
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
            "{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s.bucket.count(), s.run.len, t_add, t_compact, t_getc, t_geth, t_churn },
        );
    }

    std.debug.print("{}\n", .{acc});
}
