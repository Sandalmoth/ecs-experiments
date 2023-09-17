const std = @import("std");
const State = @import("main.zig").State;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    _ = alloc;
}
