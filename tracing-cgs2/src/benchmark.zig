const std = @import("std");
const State = @import("main.zig").State;

const Node = struct { parent: ?*Node };
const ST1 = struct { node: Node };
const ST2 = struct { number: u64 };

fn tree(alloc: std.mem.Allocator) !void {
    std.debug.print("--- big tree state transition test ---\n", .{});
    var state = State(ST1).init(alloc, 20);
    defer state.deinit();

    const root = state.create(.node);
    root.* = .{ .parent = null };

    var rng = std.rand.DefaultPrng.init(2701);

    const M = 32;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_item_ns\n", .{});
    for (0..M) |_| {

        // expand the tree
        var iter = state.iterCurrent(.node);
        while (iter.next()) |node| {
            if (rng.random().boolean()) {
                const n = state.create(.node);
                n.* = .{ .parent = node };
            }
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{.node});
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(.node).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

fn chain(alloc: std.mem.Allocator) !void {
    std.debug.print("--- chain state transition test ---\n", .{});
    var state = State(ST1).init(alloc, 20);
    defer state.deinit();

    var root = state.create(.node);
    root.* = .{ .parent = null };

    const M = 18;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_item_ns\n", .{});
    for (0..M) |_| {
        const end = state.getPool(.node).len;
        for (0..end) |_| {
            const n = state.create(.node);
            n.* = .{ .parent = root };
            root = n;
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{.node});
            root = state.update(root).?;
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(.node).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

fn flat(alloc: std.mem.Allocator) !void {
    std.debug.print("--- POD state transition test ---\n", .{});
    var state = State(ST2).init(alloc, 20);
    defer state.deinit();

    const root = state.create(.number);
    root.* = 0;

    const M = 18;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_item_ns\n", .{});
    for (0..M) |_| {
        const end = state.getPool(.number).len;
        for (0..end) |i| {
            const n = state.create(.number);
            n.* = @intCast(i);
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{.number});
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(.number).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

const PVA = struct {
    pos: ?*@Vector(2, f32),
    vel: ?*@Vector(2, f32),
    acc: ?*@Vector(2, f32),
};
const ST3 = struct {
    pva: PVA,
    pos: @Vector(2, f32),
    vel: @Vector(2, f32),
    acc: @Vector(2, f32),
};

fn pos_vel_acc(alloc: std.mem.Allocator) !void {
    std.debug.print("--- Iterating with possible misses ---\n", .{});
    var rng = std.rand.DefaultPrng.init(2701);

    const M = 16;
    const N = 1024;

    std.debug.print("len\tcreate_ms\titer_ms\n", .{});

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var state = State(ST3).init(alloc, 20);
        defer state.deinit();

        var timer = try std.time.Timer.start();

        for (0..len) |_| {
            const pva = state.create(.pva);
            if (rng.random().boolean()) {
                const pos = state.create(.pos);
                pos.* = @Vector(2, f32){ 1.0, 1.0 };
                pva.pos = pos;
            }
            if (rng.random().boolean()) {
                const vel = state.create(.vel);
                vel.* = @Vector(2, f32){ 1.0, 1.0 };
                pva.vel = vel;
            }
            if (rng.random().boolean()) {
                const acc = state.create(.acc);
                acc.* = @Vector(2, f32){ 1.0, 1.0 };
                pva.acc = acc;
            }
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        for (0..N) |_| {
            var iter = state.iterCurrent(.pva);
            while (iter.next()) |pva| {
                // i wish this syntax existed
                // if (pva.vel and pva.acc) |vel, acc| {
                //     vel += acc;
                // }
                if (pva.vel != null and pva.acc != null) {
                    pva.vel.?.* += pva.acc.?.*;
                }
                if (pva.vel != null and pva.pos != null) {
                    pva.pos.?.* += pva.vel.?.*;
                }
            }

            // state = state.step(.{ .pos, .vel, .acc, .pva });
            state = state.step(.{.pva});
        }

        const iter_time: f64 = @floatFromInt(timer.lap());

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\n",
            .{ len, create_time * 1e-6, iter_time * 1e-6 },
        );
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try tree(alloc);
    try chain(alloc);
    try flat(alloc);
    try pos_vel_acc(alloc);
}
