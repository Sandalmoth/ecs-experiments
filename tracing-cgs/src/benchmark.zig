const std = @import("std");
const State = @import("main.zig").State;

const BTNode = struct { parent: ?*BTNode };

fn bigTree(alloc: std.mem.Allocator) !void {
    std.debug.print("--- big tree state transition test ---\n", .{});
    var state = try State.init(alloc, 20);
    defer state.deinit();

    const root = state.create(BTNode);
    root.* = .{ .parent = null };

    var rng = std.rand.DefaultPrng.init(2701);

    const M = 32;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_node_ns\n", .{});
    for (0..M) |_| {

        // expand the tree
        var iter = state.iterator(BTNode); // we need an iterCurrent, that doesn't continue past len
        var i: usize = 0;
        const end = state.getPool(BTNode).len;
        while (iter.next()) |node| : (i += 1) {
            if (i == end) {
                break;
            }
            if (rng.random().boolean()) {
                const n = state.create(BTNode);
                n.* = .{ .parent = node };
            }
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{BTNode});
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(BTNode).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

fn chain(alloc: std.mem.Allocator) !void {
    std.debug.print("--- chain state transition test ---\n", .{});
    var state = try State.init(alloc, 20);
    defer state.deinit();

    var root = state.create(BTNode);
    root.* = .{ .parent = null };

    const M = 18;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_node_ns\n", .{});
    for (0..M) |_| {
        const end = state.getPool(BTNode).len;
        for (0..end) |_| {
            const n = state.create(BTNode);
            n.* = .{ .parent = root };
            root = n;
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{BTNode});
            root = try state.update(root);
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(BTNode).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

fn flat(alloc: std.mem.Allocator) !void {
    std.debug.print("--- POD state transition test ---\n", .{});
    var state = try State.init(alloc, 20);
    defer state.deinit();

    const root = state.create(u64);
    root.* = 0;

    const M = 18;
    const N = 32;

    std.debug.print("len\ttime_ms\ttime_per_node_ns\n", .{});
    for (0..M) |_| {
        const end = state.getPool(u64).len;
        for (0..end) |i| {
            const n = state.create(u64);
            n.* = @intCast(i);
        }

        // measure performance
        var timer = try std.time.Timer.start();
        for (0..N) |_| {
            state = state.step(.{u64});
        }
        const time: f64 = @as(f64, @floatFromInt(timer.read())) / N;
        const len = state.getPool(u64).len;
        std.debug.print("{}\t{d:.2}\t{d:.2}\n", .{ len, time * 1e-6, time / @as(f64, @floatFromInt(len)) });
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try bigTree(alloc);
    try chain(alloc);
    try flat(alloc);
}
