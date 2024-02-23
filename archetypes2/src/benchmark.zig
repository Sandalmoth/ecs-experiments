const std = @import("std");
const World = @import("main.zig").World;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    _ = alloc;
}
