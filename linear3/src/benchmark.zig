const std = @import("std");
const Storage = @import("main.zig").StorageImpl;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    try bench5b(alloc);
}

fn bench5b(alloc: std.mem.Allocator) !void {
    // a more ecs-like test where one component is iterated and compared with lookups in the other
    var acc: u64 = 0;
    var rng = std.Random.Xoshiro256.init(@intCast(std.time.microTimestamp()));
    var rand = rng.random();

    std.debug.print("len_1\tlen_2\tlen_3\tins\titer_1\titer_12\titer_123\n", .{});

    // const page_size = 4096;
    const page_size = 16384;
    // const page_size = 65536;

    for (6..22) |log_n| {
        const n: u32 = @as(u32, 1) << @intCast(log_n);

        var s0 = Storage(page_size, 64, u32, std.math.maxInt(u32)).init(alloc);
        defer s0.deinit();
        var s1 = Storage(page_size, 64, u32, std.math.maxInt(u32)).init(alloc);
        defer s1.deinit();
        var s2 = Storage(page_size, 64, u32, std.math.maxInt(u32)).init(alloc);
        defer s2.deinit();

        var entities = try std.ArrayList(u32).initCapacity(alloc, n);
        defer entities.deinit();

        var timer = try std.time.Timer.start();

        var n_ins: usize = 0;
        for (0..n) |_| {
            const e = rng.random().int(u32);
            if (e == std.math.maxInt(u32)) continue;
            if (s0.has(e) or s1.has(e) or s2.has(e)) continue;
            if (rand.float(f32) < 0.3) {
                s2.add(u32, e, e);
                n_ins += 1;
            }
            if (rand.float(f32) < 0.6) {
                s1.add(u32, e, e);
                n_ins += 1;
            }
            if (rand.float(f32) < 0.9) {
                s0.add(u32, e, e);
                n_ins += 1;
            }
            try entities.append(e);
        }
        const t_ins = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n_ins));

        var n_del: usize = 0;
        for (entities.items) |e| {
            if (rand.float(f32) < 0.5) {
                _ = e;
                // if (s0.has(e)) s0.del(u32, e);
                // if (s1.has(e)) s1.del(u32, e);
                // if (s2.has(e)) s2.del(u32, e);
                n_del += 1;
            }
        }
        const t_del = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(n_del));

        // then do some iteration passes over one component while fetching the others
        var nit: usize = 0;
        var it2 = s2.keyiter();
        while (it2.next()) |k| {
            const x = s0.get(u32, k) orelse continue;
            const y = s1.get(u32, k) orelse continue;
            const v = s2.get(u32, k).?.*;
            x.* *%= (y.* +% v);
            nit += 1;
        }
        const t_it2 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var it1 = s1.keyiter();
        while (it1.next()) |k| {
            const x = s0.get(u32, k) orelse continue;
            const v = s1.get(u32, k).?.*;
            x.* +%= v;
            nit += 1;
        }
        const t_it1 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        nit = 0;
        var it0 = s0.keyiter();
        while (it0.next()) |k| {
            const v = s0.get(u32, k).?.*;
            acc += v;
            nit += 1;
        }

        const t_it0 = @as(f64, @floatFromInt(timer.lap())) / @as(f64, @floatFromInt(nit));

        std.debug.print(
            "{}\t{}\t{}\t{}\t{}\t{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                s0.len,
                s0.n_buckets,
                s1.len,
                s1.n_buckets,
                s2.len,
                s2.n_buckets,
                t_ins,
                t_del,
                t_it0,
                t_it1,
                t_it2,
            },
        );
    }

    std.debug.print("\n{}\n\n", .{acc});
}
