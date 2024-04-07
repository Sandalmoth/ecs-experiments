const std = @import("std");
const State = @import("main.zig").State;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench10(alloc);
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

    var s0 = State.create(alloc);
    defer s0.destroy(f64);
    var s1 = State.create(alloc);
    defer s1.destroy(f64);

    var a = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer a.deinit();
    var b = try std.ArrayList(u64).initCapacity(alloc, 1024 * 1024);
    defer b.deinit();

    const dn = 1000;
    var timer = try std.time.Timer.start();

    for (0..64) |_| {
        for (0..dn) |_| {
            const x = rand.int(u64) | 1; // avoid nil ( = 0 )
            s0.set(f64, x, @floatFromInt(x));
            try a.append(x);
            try a.append(rand.int(u64) | 1);
            s1.set(f64, x, @floatFromInt(x));
            try b.append(x);
            try b.append(rand.int(u64) | 1);
        }
        const t_add = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(dn));

        var nchu: usize = 0;
        {
            var sprev = s0;
            defer sprev.destroy(f64);
            s0 = s0.step(f64);
        }
        {
            var sprev = s1;
            defer sprev.destroy(f64);
            s1 = s1.step(f64);
        }
        for (0..dn) |_| {
            const i = rand.uintLessThan(usize, a.items.len);
            const j = rand.uintLessThan(usize, b.items.len);
            const x = a.items[i];
            const y = b.items[j];
            if (i < rand.uintLessThan(usize, a.items.len)) {
                if (s0.has(f64, x)) {
                    s0.del(f64, x);
                } else {
                    s0.set(f64, x, @floatFromInt(x));
                }
                nchu += 1;
            }
            if (j < rand.uintLessThan(usize, b.items.len)) {
                if (s1.has(f64, y)) {
                    s1.del(f64, y);
                } else {
                    s1.set(f64, y, @floatFromInt(y));
                }
                nchu += 1;
            }
        }
        const t_churn = 1e-3 * @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nchu));

        var nit: usize = 0;
        {
            // couldn't be bothered to make an iterator, but this is how it would work internally
            for (0..s0.n_pages) |k| {
                const p = s0.page(f64, k);
                for (0..p.len) |l| {
                    acc +%= p.keys[l];
                    nit += 1;
                }
                acc +%= k;
            }
        }
        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        {
            // couldn't be bothered to make an iterator, but this is how it would work internally
            for (0..s0.n_pages) |k| {
                const p = s0.page(f64, k);
                for (0..p.len) |l| {
                    const y: u64 = @intFromFloat((s1.get(f64, p.keys[l]) orelse continue).*);
                    acc +%= p.keys[l] / y;
                    nit += 1;
                }
                acc +%= k;
            }
        }
        const t_it01 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        try stdout.print(
            "{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ t_add, t_churn, t_it0, t_it01 },
        );
    }

    std.debug.print("{}\n", .{acc});
}
