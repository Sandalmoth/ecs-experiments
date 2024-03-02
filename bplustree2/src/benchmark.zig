const std = @import("std");
const Storage = @import("main.zig").Storage;
const Deque = @import("deque.zig").FixedDeque;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench2();
    try bench1(alloc);
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
