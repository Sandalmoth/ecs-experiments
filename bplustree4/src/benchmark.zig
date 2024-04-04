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
    const rand = rng.random();

    std.debug.print("len_1\tlen_2\tlen_3\tins\titer_1\titer_12\titer_123\n", .{});

    for (6..22) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        const leaf_size = 60;
        var s0 = Storage(u32, leaf_size).init(alloc);
        defer s0.deinit();
        var s1 = Storage(u32, leaf_size).init(alloc);
        defer s1.deinit();
        var s2 = Storage(u32, leaf_size).init(alloc);
        defer s2.deinit();

        var entities = try std.ArrayList(u32).initCapacity(alloc, n);
        defer entities.deinit();

        var timer = try std.time.Timer.start();

        var n_ins: usize = 0;
        for (0..n) |_| {
            const e = rng.random().int(u32);
            if (e == std.math.maxInt(u32)) continue;
            if (s0.get(e) != null or s1.get(e) != null or s2.get(e) != null) continue;
            if (rand.float(f32) < 0.3) {
                try s2.add(e, e);
                n_ins += 1;
            }
            if (rand.float(f32) < 0.6) {
                try s1.add(e, e);
                n_ins += 1;
            }
            if (rand.float(f32) < 0.9) {
                try s0.add(e, e);
                n_ins += 1;
            }
            try entities.append(e);
        }
        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n_ins));

        // then do some iteration passes over one component while fetching the others
        var nit: usize = 0;
        var it2 = s2.iterator();
        while (it2.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            const y = s1.get(kv.key);
            if (y == null) continue;
            x.?.* *%= (y.?.* +% kv.val);
            nit += 1;
        }
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var it1 = s1.iterator();
        while (it1.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            x.?.* +%= kv.val;
            nit += 1;
        }
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var it0 = s0.iterator();
        while (it0.next()) |kv| {
            acc += kv.val;
            nit += 1;
        }
        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        // sorted merge style
        nit = 0;
        it0 = s0.iterator();
        it1 = s1.iterator();
        var kv0 = it0.next();
        var kv1 = it1.next();
        while (kv0 != null and kv1 != null) {
            if (kv0.?.key == kv1.?.key) {
                acc +%= kv0.?.val *% kv1.?.val;
                kv0 = it0.next();
                kv1 = it1.next();
                nit += 1;
            } else if (kv0.?.key < kv1.?.key) {
                kv0 = it0.next();
            } else {
                kv1 = it1.next();
            }
        }
        const t_it1b = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        it0 = s0.iterator();
        it1 = s1.iterator();
        it2 = s2.iterator();
        kv0 = it0.next();
        kv1 = it1.next();
        var kv2 = it2.next();
        while (kv0 != null and kv1 != null and kv2 != null) {
            if (kv0.?.key == kv1.?.key and kv0.?.key == kv2.?.key) {
                acc +%= kv0.?.val *% kv1.?.val *% kv2.?.val;
                kv0 = it0.next();
                kv1 = it1.next();
                kv2 = it2.next();
                nit += 1;
            } else if (kv0.?.key < kv1.?.key or kv0.?.key < kv2.?.key) {
                kv0 = it0.next();
            } else if (kv1.?.key < kv0.?.key or kv1.?.key < kv2.?.key) {
                kv1 = it1.next();
            } else if (kv2.?.key < kv0.?.key or kv2.?.key < kv1.?.key) {
                kv2 = it2.next();
            }
        }
        const t_it2b = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        std.debug.print(
            "{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s0.len, s1.len, s2.len, t_ins, t_it0, t_it1, t_it2, t_it1b, t_it2b },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}
