const std = @import("std");

const A = struct {
    comptime a: u32 = 0,
    b: u32,
};

const B = struct {
    comptime a: u32 = 0,
    b: u32,

    fn init(a: A) B {
        return .{
            .a = a.a + 1,
            .b = a.b * 3,
        };
    }
};

// this is not allowed, comptime fields do not work this way
// though they migth if https://github.com/ziglang/zig/issues/5675 passes
// test "partial compitme struct" {
//     const a = A{
//         .a = 3,
//         .b = 4,
//     };
//     const b = B.init(a);
//     std.debug.print("{}\n", .{a});
//     std.debug.print("{}\n", .{b});
// }

fn FooA(comptime x: u32) type {
    return struct {
        comptime a: u32 = x,
        b: u32,

        fn init(y: u32) @This() {
            return .{ .b = y };
        }
    };
}

fn FooB(comptime x: u32) type {
    return struct {
        comptime a: u32 = x + 1,
        b: u32,

        fn init(y: u32) @This() {
            return .{ .b = y * 3 };
        }
    };
}

// i don't like this at all, creates such a complicated mix of where each type needs data
test "encode in type" {
    const a = FooA(3).init(4);
    const b = FooB(a.a).init(a.b);
    std.debug.print("{}\n", .{a});
    std.debug.print("{}\n", .{b});
}

fn Bar(comptime _x: u32) type {
    return struct {
        const x = _x;
        a: u32,
    };
}

fn foobar(bar: Bar(3)) void {
    std.debug.print("{}\n", .{bar.a * @TypeOf(bar).x});
}

fn goo(f: anytype) void {
    const info = @typeInfo(@TypeOf(f));
    std.debug.print("{}\n", .{info.@"fn".return_type.?});
    inline for (info.@"fn".params) |param| {
        std.debug.print("{any}\n", .{param.type});
    }
    const x = info.@"fn".params[0].type.?.x;
    std.debug.print("{}\n", .{x});
}

// decent, too magical?
// i like how the function also encodes its own requirements
test "encode in type v2" {
    goo(foobar);
}

const RW = struct { x: u32, y: u32 };
const System = *const fn () void;
fn schedule(f: System, comptime rw: RW) void {
    _ = f;
    _ = rw;
}
