const std = @import("std");
const Storage = @import("main.zig").Storage;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var acc: usize = 0;
    var timer = try std.time.Timer.start();

    std.debug.print("nsize\tlsize\tn\tadd\tget\tdel\n", .{});

    var x: u32 = 0;
    inline for (.{ 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512 }) |i| {
        inline for (.{ 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512 }) |j| {
            var s = Storage(usize, i, j).init(alloc);
            defer s.deinit();

            for (1..6) |k| {
                const N = std.math.pow(u32, 10, @intCast(k));

                timer.reset();

                x = 0;
                for (0..N) |l| {
                    try s.add(x, l);
                    x +%= 2_654_435_761;
                }
                const t_add = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

                x = 0;
                for (0..N) |_| {
                    const res = s.get(x);
                    if (res != null) {
                        acc += res.?.*;
                    }
                    x +%= 2_654_435_761;
                }
                const t_get = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

                x = 0;
                for (0..N) |_| {
                    s.del(x);
                    x +%= 2_654_435_761;
                }
                const t_del = @as(f32, @floatFromInt(timer.lap())) * 1e-3;

                std.debug.print(
                    "{}\t{}\t{}\t{d:.3}\t{d:.3}\t{d:.3}\n",
                    .{ i, j, k, t_add, t_get, t_del },
                );
            }
        }
    }

    std.debug.print("{}\n", .{acc});
}
