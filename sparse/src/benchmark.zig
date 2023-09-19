const std = @import("std");
const State = @import("main.zig").State;

const PVA = struct {
    pos: @Vector(2, f32),
    vel: @Vector(2, f32),
    acc: @Vector(2, f32),
};
fn pos_vel_acc(alloc: std.mem.Allocator) !void {
    var rng = std.rand.DefaultPrng.init(2701);

    const M = 18;
    const N = 1024;

    std.debug.print("len\tcrt_ms\titer_us\n", .{});

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var state = State(PVA).init(alloc, 20);
        defer state.deinit(alloc);

        var timer = try std.time.Timer.start();

        for (0..len) |_| {
            const e = state.create();
            if (rng.random().boolean()) {
                state.add(e, .pos, @Vector(2, f32){ 1.0, 1.0 });
            }
            if (rng.random().boolean()) {
                state.add(e, .vel, @Vector(2, f32){ 1.0, 1.0 });
            }
            if (rng.random().boolean()) {
                state.add(e, .acc, @Vector(2, f32){ 1.0, 1.0 });
            }
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        for (0..N) |_| {
            var iter_va = state.iterator(.{ .vel, .acc });
            while (iter_va.next()) |e| {
                e.vel.* += e.acc.*;
            }
            var iter_pv = state.iterator(.{ .pos, .vel });
            while (iter_pv.next()) |e| {
                e.pos.* += e.vel.*;
            }

            state = state.step();
        }

        const iter_time: f64 = @floatFromInt(timer.lap() / N);

        std.debug.print(
            "{}\t{d:.3}\t{d:.3}\n",
            .{ len, create_time * 1e-6, iter_time * 1e-3 },
        );
    }
}

const PV = struct {
    pos: @Vector(2, f32),
    vel: @Vector(2, f32),
};
fn pos_vel(alloc: std.mem.Allocator) !void {
    var rng = std.rand.DefaultPrng.init(2701);

    const M = 18;
    const N = 1024;

    std.debug.print("len\tcrt_ms\titer_us\n", .{});

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var state = State(PV).init(alloc, 20);
        defer state.deinit(alloc);

        var timer = try std.time.Timer.start();

        for (0..len) |_| {
            const e = state.create();
            state.add(e, .pos, @Vector(2, f32){ 1.0, 1.0 });
            if (rng.random().float(f32) < 0.1) {
                state.add(e, .vel, @Vector(2, f32){ 1.0, 1.0 });
            }
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        for (0..N) |_| {
            var iter_pv = state.iterator(.{ .pos, .vel });
            while (iter_pv.next()) |e| {
                e.pos.* += e.vel.*;
            }

            var iter_p = state.iterator(.{.pos});
            while (iter_p.next()) |e| {
                _ = e;
            }

            state = state.step();
        }

        const iter_time: f64 = @floatFromInt(timer.lap() / N);

        std.debug.print(
            "{}\t{d:.3}\t{d:.3}\n",
            .{ len, create_time * 1e-6, iter_time * 1e-3 },
        );
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try pos_vel_acc(alloc);
    try pos_vel(alloc);
}
