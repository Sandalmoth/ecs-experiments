const std = @import("std");

fn floatHash(a: u32) f64 {
    // var b: usize = std.hash.uint32(a);
    var b: usize = a; // we don't need hashing here as we're testing all the inputs
    b <<= 20;
    b |= 1023 << 52;
    return @as(f64, @bitCast(b)) - 1.0;
}

fn idistr(h: f64) f64 {
    return std.math.pow(f64, h, 1.0 / 3.0);
}

fn distr(h: f64) f64 {
    return h * h * h;
}

fn bucketMap(scale: f64, h: f64) usize {
    var t = @ceil(scale - h) + h;
    t = distr(t);
    return @as(usize, @intFromFloat(t));
}

fn bucketBegin(scale: f64) usize {
    const t = distr(scale) + 0.5;
    // const t = @ceil(distr(scale));
    return @as(usize, @intFromFloat(t));
}

fn bucketEnd(scale: f64) usize {
    const t = @ceil(distr(scale + 1));
    return @as(usize, @intFromFloat(t));
}

fn bucketIndex(scale: f64, key: u32) usize {
    const h = floatHash(key);
    return bucketMap(scale, h);
}

const K = 8;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var n_errors: usize = 0;

    const data = try alloc.alloc(u16, 1 << K);
    defer alloc.free(data);

    for (0..1 << K) |i| {
        data[i] = 0;
    }
    var s = idistr(0);
    for (0..1 << K) |i| {
        const loc = bucketIndex(s, @intCast(i));
        if (loc != 0) {
            std.debug.print("Error for s={} i={} -> {}\n", .{ s, i, loc });
            n_errors += 1;
        }
    }

    for (1..1000) |j| {
        // const s_new = idistr(@ceil(distr(s)) + 1);
        const s_new = idistr(@round(distr(s) + 1));
        std.debug.print("incrementing s: {} -> {}\n", .{ s, s_new });
        std.debug.print(
            "  new bucket range is: {} .. {} (n={})\n",
            .{ bucketBegin(s_new), bucketEnd(s_new), bucketEnd(s_new) - bucketBegin(s_new) },
        );
        s = s_new;

        for (0..1 << K) |i| {
            const loc = bucketIndex(s, @intCast(i));
            const old_loc = data[i];
            if (old_loc != loc and old_loc != j - 1) {
                std.debug.print("Error for s={} i={}: {} -> {}\n", .{ s, i, old_loc, loc });
                n_errors += 1;
            }
            data[i] = @intCast(loc);
        }
    }

    std.debug.print("total errors: {}\n", .{n_errors});
}
