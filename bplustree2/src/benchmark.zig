const std = @import("std");
const Storage = @import("main.zig").Storage;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const M = 21;

    std.debug.print("len\tcrt_ns\tget_ns\n", .{});

    var acc: @Vector(4, f32) = @splat(0.0);

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var i: u32 = 2_654_435_761;
        var s = Storage(u32, @Vector(4, f32), 31, 15).init(alloc);
        i +%= 2_654_435_761;
        defer s.deinit();

        var timer = try std.time.Timer.start();

        for (0..len) |j| {
            try s.add(i, @as(@Vector(4, f32), @splat(@floatFromInt(j))));
            i +%= 2_654_435_761;
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        i = 2_654_435_761;
        i +%= 2_654_435_761;
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

    std.debug.print("printing to avoid optimizer dropping my result\n{}\n", .{acc});
}
