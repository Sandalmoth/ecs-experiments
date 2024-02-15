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

    std.debug.print("len\tcrt_ms\titer_us\tstep_us\n", .{});

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

        var iter_time_ns: usize = 0;
        var step_time_ns: usize = 0;

        for (0..N) |_| {
            var iter_pva = state.iterator(.{ .pos, .vel, .acc });
            while (iter_pva.next()) |e| {
                e.vel.* += e.acc.*;
                e.pos.* += e.vel.*;
            }
            iter_time_ns += timer.lap();

            state = state.step();
            step_time_ns += timer.lap();
        }

        const iter_time: f64 = @floatFromInt(iter_time_ns / N);
        const step_time: f64 = @floatFromInt(step_time_ns / N);

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ len, create_time * 1e-6, iter_time * 1e-3, step_time * 1e-3 },
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

const Big = struct {
    aa: f32,
    ab: f32,
    ac: f32,
    ad: f32,
    ae: f32,
    af: f32,
    ag: f32,
    ah: f32,
    ai: f32,
    aj: f32,
    ak: f32,
    al: f32,
    am: f32,
    an: f32,
    ao: f32,
    ap: f32,
    aq: f32,
    ar: f32,
    as: f32,
    at: f32,
    au: f32,
    av: f32,
    aw: f32,
    ax: f32,
    ay: f32,
    az: f32,
    ba: f32,
    bb: f32,
    bc: f32,
    bd: f32,
    be: f32,
    bf: f32,
    bg: f32,
    bh: f32,
    bi: f32,
    bj: f32,
    bk: f32,
    bl: f32,
    bm: f32,
    bn: f32,
    bo: f32,
    bp: f32,
    bq: f32,
    br: f32,
    bs: f32,
    bt: f32,
    bu: f32,
    bv: f32,
    bw: f32,
    bx: f32,
    by: f32,
    bz: f32,
    ca: f32,
    cb: f32,
    cc: f32,
    cd: f32,
    ce: f32,
    cf: f32,
    cg: f32,
    ch: f32,
    ci: f32,
    cj: f32,
    ck: f32,
    cl: f32,
    cm: f32,
    cn: f32,
    co: f32,
    cp: f32,
    cq: f32,
    cr: f32,
    cs: f32,
    ct: f32,
    cu: f32,
    cv: f32,
    cw: f32,
    cx: f32,
    cy: f32,
    cz: f32,
    da: f32,
    db: f32,
    dc: f32,
    dd: f32,
    de: f32,
    df: f32,
    dg: f32,
    dh: f32,
    di: f32,
    dj: f32,
    dk: f32,
    dl: f32,
    dm: f32,
    dn: f32,
    do: f32,
    dp: f32,
    dq: f32,
    dr: f32,
    ds: f32,
    dt: f32,
    du: f32,
    dv: f32,
    dw: f32,
    dx: f32,
    dy: f32,
    dz: f32,
};
fn big(alloc: std.mem.Allocator) !void {
    var rng = std.rand.DefaultPrng.init(2701);

    const M = 18;
    const N = 1024;

    std.debug.print("len\tcrt_ms\titer_us\tstep_us\n", .{});

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var state = State(Big).init(alloc, 2);
        defer state.deinit(alloc);

        var timer = try std.time.Timer.start();

        for (0..len) |_| {
            const e = state.create();
            if (rng.random().boolean()) state.add(e, .aa, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ab, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ac, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ad, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ae, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .af, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ag, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ah, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ai, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .aj, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ak, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .al, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .am, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .an, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ao, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ap, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .aq, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ar, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .as, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .at, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .au, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .av, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .aw, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ax, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ay, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .az, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ba, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bb, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bc, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bd, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .be, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bf, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bg, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bh, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bi, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bj, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bk, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bl, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bm, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bn, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bo, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bp, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bq, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .br, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bs, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bt, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bu, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bv, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bw, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bx, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .by, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .bz, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ca, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cb, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cc, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cd, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ce, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cf, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cg, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ch, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ci, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cj, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ck, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cl, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cm, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cn, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .co, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cp, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cq, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cr, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cs, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ct, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cu, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cv, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cw, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cx, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cy, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .cz, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .da, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .db, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dc, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dd, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .de, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .df, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dg, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dh, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .di, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dj, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dk, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dl, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dm, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dn, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .do, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dp, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dq, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dr, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .ds, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dt, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .du, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dv, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dw, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dx, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dy, rng.random().float(f32) - 0.5);
            if (rng.random().boolean()) state.add(e, .dz, rng.random().float(f32) - 0.5);
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        var iter_time_ns: usize = 0;
        var step_time_ns: usize = 0;

        for (0..N) |_| {
            // var iter_pva = state.iterator(.{ .aa, .ab });
            // while (iter_pva.next()) |e| {
            //     e.aa.* += e.ab.*;
            // }
            iter_time_ns += timer.lap();

            state = state.step();
            step_time_ns += timer.lap();
        }

        const iter_time: f64 = @floatFromInt(iter_time_ns / N);
        const step_time: f64 = @floatFromInt(step_time_ns / N);

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{ len, create_time * 1e-6, iter_time * 1e-3, step_time * 1e-3 },
        );
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    try pos_vel_acc(alloc);
    // try pos_vel(alloc);
    try big(alloc);
}
