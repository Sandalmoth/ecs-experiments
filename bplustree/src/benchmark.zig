const std = @import("std");
const Storage = @import("main.zig").Storage;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // var acc: usize = 0;
    // var timer = try std.time.Timer.start();

    // std.debug.print("nsize\tlsize\tn\tadd\tget\tdel\n", .{});

    // var x: u32 = 0;
    // inline for (.{ 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512 }) |i| {
    //     inline for (.{ 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512 }) |j| {
    //         var s = Storage(usize, i, j).init(alloc);
    //         defer s.deinit();

    //         for (1..6) |k| {
    //             const N = std.math.pow(u32, 10, @intCast(k));

    //             timer.reset();

    //             x = 0;
    //             for (0..N) |l| {
    //                 try s.add(x, l);
    //                 x +%= 2_654_435_761;
    //             }
    //             const t_add = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

    //             x = 0;
    //             for (0..N) |_| {
    //                 const res = s.get(x);
    //                 if (res != null) {
    //                     acc += res.?.*;
    //                 }
    //                 x +%= 2_654_435_761;
    //             }
    //             const t_get = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

    //             x = 0;
    //             for (0..N) |_| {
    //                 s.del(x);
    //                 x +%= 2_654_435_761;
    //             }
    //             const t_del = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

    //             std.debug.print(
    //                 "{}\t{}\t{}\t{d:.3}\t{d:.3}\t{d:.3}\n",
    //                 .{ i, j, k, t_add, t_get, t_del },
    //             );
    //         }
    //     }
    // }

    // std.debug.print("{}\n", .{acc});

    const M = 21;

    std.debug.print("len\tcrt_ns\tget_ns\n", .{});

    var acc: @Vector(4, f32) = @splat(0.0);

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var i: u32 = 2_654_435_761;
        var p = Storage(@Vector(4, f32), 64, 64).init(alloc);
        i +%= 2_654_435_761;
        defer p.deinit();

        var timer = try std.time.Timer.start();

        for (0..len) |j| {
            try p.add(i, @as(@Vector(4, f32), @splat(@floatFromInt(j))));
            i +%= 2_654_435_761;
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        i = 2_654_435_761;
        i +%= 2_654_435_761;
        for (0..len) |_| {
            acc += p.get(i).?.*;
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
