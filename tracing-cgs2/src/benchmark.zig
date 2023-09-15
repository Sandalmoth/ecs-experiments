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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try tree(alloc);
    try chain(alloc);
    try flat(alloc);
}
