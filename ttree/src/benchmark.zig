const std = @import("std");
const Storage = @import("main.zig").Storage;

fn lookup(alloc: std.mem.Allocator) !void {
    const M = 21;

    std.debug.print("len\tcrt_ns\tget_ns\n", .{});

    var acc: @Vector(4, f32) = @splat(0);

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var storage = Storage(@Vector(4, f32)).init(alloc);
        defer storage.deinit();

        var timer = try std.time.Timer.start();

        var i: usize = 0;
        for (0..len) |_| {
            try storage.add(i, @splat(@floatFromInt(i)));
            i = (i + 2_654_435_761) % (65536 * 65536); // mostly negligible perf cost
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        i = 0;
        for (0..len) |_| {
            acc += storage.get(i).?.*;
            i = (i + 2_654_435_761) % (65536 * 65536);
        }

        const get_time: f64 = @floatFromInt(timer.lap());

        const il2: f32 = 1 / @as(f32, @floatFromInt(len));

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\n",
            .{ len, create_time * il2, get_time * il2 },
        );
    }

    std.debug.print("printing to avoid optimizer dropping my result\n{}\n", .{acc});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try lookup(alloc);
}
