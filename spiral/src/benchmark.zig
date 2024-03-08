const std = @import("std");
const Storage = @import("main.zig").Storage;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench5(alloc);
}

fn bench5(alloc: std.mem.Allocator) !void {
    // a more ecs-like test where one component is iterated and compared with lookups in the other
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    std.debug.print("len_1\tnb_1\tlen_2\tnb_2\tlen_3\tnb_3\tins\titer_1\titer_12\titer_123\n", .{});

    for (6..22) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var s0 = try Storage(u32, u32, 2048, 1024).init(alloc);
        defer s0.deinit();
        var s1 = try Storage(u32, u32, 2048, 1024).init(alloc);
        defer s1.deinit();
        var s2 = try Storage(u32, u32, 2048, 1024).init(alloc);
        defer s2.deinit();

        var timer = try std.time.Timer.start();

        // insert some random values
        for (0..2 * n) |_| {
            const k = rng.random().int(u32) % n;
            if (s0.get(k)) |_| {} else {
                try s0.add(k, k);
            }
        }

        for (0..n) |_| {
            const k = rng.random().int(u32) % n;
            if (s1.get(k)) |_| {} else {
                try s1.add(k, k);
            }
        }

        for (0..n / 2) |_| {
            const k = rng.random().int(u32) % n;
            if (s2.get(k)) |_| {} else {
                try s2.add(k, k);
            }
        }

        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        // then do some iteration passes over one component while fetching the others
        var it2 = s2.iterator();
        while (it2.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            const y = s1.get(kv.key);
            if (y == null) continue;
            x.?.* *%= (y.?.* +% kv.val);
        }

        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s2.len));

        var it1 = s1.iterator();
        while (it1.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            x.?.* +%= kv.val;
        }

        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s1.len));

        var it0 = s0.iterator();
        while (it0.next()) |kv| {
            acc += kv.val;
        }

        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(s0.len));

        std.debug.print(
            "{}\t{}\t{}\t{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                s0.len,
                s0.bucket_end - s0.bucket_begin,
                s1.len,
                s1.bucket_end - s1.bucket_begin,
                s2.len,
                s2.bucket_end - s2.bucket_begin,
                t_ins,
                t_it0,
                t_it1,
                t_it2,
            },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}
