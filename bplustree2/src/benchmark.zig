const std = @import("std");
const Storage = @import("main.zig").Storage;
const Deque = @import("deque.zig").FixedDeque;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench3(alloc);
    try bench2();
    try bench1(alloc);
}

fn bench3(alloc: std.mem.Allocator) !void {
    // a more ecs-like test where one component is iterated and compared with lookups in the other
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    var s0 = Storage(u32, u32, 31, 15).init(alloc);
    defer s0.deinit();
    var s1 = Storage(u32, u32, 31, 15).init(alloc);
    defer s1.deinit();
    var s2 = Storage(u32, u32, 31, 15).init(alloc);
    defer s2.deinit();

    std.debug.print("len_1\tlen_2\tlen_3\tindel\titer_1\titer_2\titer_3\n", .{});

    for (6..21) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var timer = try std.time.Timer.start();

        // a bunch of insertion/deletion churn
        for (0..2 * n) |_| {
            const k = rng.random().int(u32) % n;
            if (s0.get(k)) |_| {
                s0.del(k);
            } else {
                try s0.add(k, k);
            }
        }

        for (0..n) |_| {
            const k = rng.random().int(u32) % n;
            if (s1.get(k)) |_| {
                s1.del(k);
            } else {
                try s1.add(k, k);
            }
        }

        for (0..n / 2) |_| {
            const k = rng.random().int(u32) % n;
            if (s2.get(k)) |_| {
                s2.del(k);
            } else {
                try s2.add(k, k);
            }
        }

        const t_indel = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        // then do some iteration passes over one component while fetching the others
        var it2 = s2.iterator();
        while (it2.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            const y = s1.get(kv.key);
            if (y == null) continue;
            x.?.* *%= (y.?.* +% kv.val);
        }

        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        var it1 = s1.iterator();
        while (it1.next()) |kv| {
            const x = s0.get(kv.key);
            if (x == null) continue;
            x.?.* +%= kv.val;
        }

        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        var it0 = s0.iterator();
        while (it0.next()) |kv| {
            acc += kv.val;
        }

        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n));

        std.debug.print(
            "{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ s0.len, s1.len, s2.len, t_indel, t_it0, t_it1, t_it2 },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}

fn bench2() !void {
    var acc: u64 = 0;
    var rng = std.rand.Xoshiro256.init(@intCast(std.time.microTimestamp()));

    std.debug.print("len\tlb_ms\tup_ms\n", .{});

    inline for (.{ 4, 8, 16, 32, 64, 128 }) |n| {
        var dq = Deque(n, u32){};

        for (0..n) |i| {
            dq.pushBack(@intCast(i));
        }
        std.debug.assert(dq.isSorted());

        var timer = try std.time.Timer.start();

        for (0..1_000_000) |_| {
            acc += dq.lowerBound(rng.random().int(u32) % n);
        }

        const t_lb: f32 = @as(f32, @floatFromInt(timer.lap())) * 1e-6;

        for (0..1_000_000) |_| {
            acc += dq.upperBound(rng.random().int(u32) % n);
        }

        const t_ub: f32 = @as(f32, @floatFromInt(timer.lap())) * 1e-6;

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\n",
            .{ n, t_lb, t_ub },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}

fn bench1(alloc: std.mem.Allocator) !void {
    const M = 21;

    std.debug.print("len\tcrt_ns\tget_ns\n", .{});

    var acc: @Vector(4, f32) = @splat(0.0);

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var i: u32 = 2_654_435_761;
        var s = Storage(u32, @Vector(4, f32), 31, 15).init(alloc);
        defer s.deinit();

        var timer = try std.time.Timer.start();

        for (0..len) |j| {
            try s.add(i, @as(@Vector(4, f32), @splat(@floatFromInt(j))));
            i +%= 2_654_435_761;
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        i = 2_654_435_761;
        for (0..len) |_| {
            acc += s.get(i).?.*;
            i +%= 2_654_435_761;
        }

        const get_time: f64 = @floatFromInt(timer.lap());

        const il2: f32 = 1 / @as(f32, @floatFromInt(len));

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\n",
            .{ len, create_time * il2, get_time * il2 },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}
