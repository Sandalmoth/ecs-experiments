const std = @import("std");

// so the first t-tree implementation i made has awful perf, i'd guess because
// - linear searching of huge nodes
// - (cache issues of t-trees in general)
// - (no balancing but probably fine since the keys were equidistributed and random-ish order)
// - (64 bit entity ids, but it's extremely unlikely that we'd need more than 32-bit)
// another datastructure that could work well is the CST-tree
// though i'm not gonna implement it exactly as written since i still want the fixed page size
// hence, each page will be laid out to have one node group and one data group
// and the length of each row in the data groups will be adjusted to fill the page
// this is probably a bit worse than the original CST-tree, but i think it could be ok still

// i'm gonna assume a cache line size of 64 bytes

const MEMORY_PAGE_SIZE = 16 * 1024;

fn Page(comptime V: type) type {
    // we could maybe fit slightly more per page by tuning the space reserved for the padding
    // or by iteratively reducing capacity like in the previous iteration (TODO)
    // but the iterative type definition doesn't play nice with zls right now
    // so for my convenience this will do
    const val_cap = ((MEMORY_PAGE_SIZE - 2048) / (@sizeOf(u32) + @sizeOf(V)) / 15);
    const key_cap = (val_cap / 16 + 1) * 16; // multiple of 16 rounding up from val_cap

    if (val_cap == 0) {
        @compileError("Page layout impossible for " ++ @typeName(u32) ++ ": " ++ @typeName(V));
    }

    return struct {
        const Self = @This();
        const nil: u32 = 0;

        parent: ?*Self,
        node: [15]u32 align(64),
        children: [16]?*Self align(64),
        keys: [15 * key_cap]u32 align(64),
        vals: [15 * val_cap]V,

        // .node is packed binary tree like this
        //            0
        //         /     \
        //      1           2
        //    /   \       /   \
        //   3     4     5     6
        //  / \   / \   / \   / \
        // 7   8 9  10 11 12 13 14
        // which holds the maximum value in each associated row of keys

        // to get a row of keys/data we can use
        // .keys[n*key_cap .. n*key_cap + val_cap]
        // .vals[n*val_cap .. (n + 1)*val_cap]

        /// empty nodes are ill-defined so create with an entry
        fn create(alloc: std.mem.Allocator, parent: ?*Self, key: u32, val: V) !*Self {
            var page = try alloc.create(Self);
            page.parent = parent;
            page.children = .{null} ** 16;
            page.node = .{nil} ** 15;
            page.keys = .{nil} ** (15 * key_cap);
            page.node[0] = key;
            page.keys[0] = key;
            page.vals[0] = val;
            return page;
        }

        /// also recursively destroys all children
        fn destroy(page: *Self, alloc: std.mem.Allocator) void {
            for (page.children) |child| {
                if (child == null) continue;
                child.?.destroy(alloc);
            }
            alloc.destroy(page);
        }

        const FindResult = struct {
            success: bool,
            page: *Self,
            n: u8, // what data row
            i: u32, // what index in the row
        };
        fn find(page: *Self, key: u32) FindResult {
            std.debug.assert(key != nil);

            // first search the node group
            var node: u8 = 0;
            var walk = page;
            var mark: FindResult = undefined;
            mark.page = page;
            mark.n = 0;
            mark.i = 0;

            var comp: u32 = walk.node[node];
            while (comp != nil) {
                if (key > comp) {
                    node = 2 * (node + 1);
                } else {
                    mark.page = walk;
                    mark.n = node;
                    mark.i = 0;
                    node = 2 * (node + 1) - 1;
                }

                if (node < 15) {
                    comp = walk.node[node];
                } else if (walk.children[node - 15] != null) {
                    walk = walk.children[node - 15].?;
                    node = 0;
                    comp = walk.node[node];
                } else {
                    comp = nil;
                }
            }

            // now try to find our key in that data row
            // TODO optimize, i'm thinking a branchless simd linear search
            const mark_n: usize = @intCast(mark.n);
            const row = @as([*]u32, @ptrCast(&page.keys[0])) + mark_n * key_cap;
            while (mark.i < val_cap) : (mark.i += 1) {
                if (row[mark.i] == nil or row[mark.i] >= key) {
                    break;
                }
            }

            mark.success = mark.i < val_cap and row[mark.i] == key;
            return mark;
        }

        /// key must not be present already
        fn insert(page: *Self, alloc: std.mem.Allocator, key: u32, val: V) !void {

            // we should first find out if the data row is the bounding row for our key
            // if so, we displace a value from the row anloc.page add our key
            // otherwise, we can recursively try to insert into a subnode

            var node: usize = 0;
            var comp = page.node[node];
            while (true) {

                // if there is space in this node, insert right here
                if (page.keys[node * key_cap + val_cap - 1] == 0) {
                    var i: usize = 0;
                    const row = @as([*]u32, @ptrCast(&page.keys[0])) + node * key_cap;
                    while (i < val_cap) : (i += 1) {
                        if (row[i] == nil or row[i] >= key) {
                            break;
                        }
                    }

                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});
                    std.mem.copyBackwards(
                        u32,
                        page.keys[node * key_cap + i + 1 .. node * key_cap + val_cap],
                        page.keys[node * key_cap + i .. node * key_cap + val_cap - 1],
                    );
                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});
                    std.mem.copyBackwards(
                        V,
                        page.vals[node * val_cap + i + 1 .. node * val_cap + val_cap],
                        page.vals[node * val_cap + i .. node * val_cap + val_cap - 1],
                    );
                    page.keys[node * key_cap + i] = key;
                    page.vals[node * val_cap + i] = val;
                    page.node[node] = @max(key, page.node[node]);
                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});

                    //                 std.debug.print("direct insertion\n", .{});
                    return;
                }

                // if this data row is the bounding row, displace and insert right here
                if (key > page.keys[node * key_cap] and
                    key < page.keys[node * key_cap + val_cap - 1])
                {
                    var i: usize = 0;
                    const row = @as([*]u32, @ptrCast(&page.keys[0])) + node * key_cap;
                    while (i < val_cap) : (i += 1) {
                        if (row[i] == nil or row[i] >= key) {
                            break;
                        }
                    }

                    const displaced_key = page.keys[node * key_cap + val_cap - 1];
                    const displaced_val = page.vals[node * val_cap + val_cap - 1];

                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});
                    std.mem.copyBackwards(
                        u32,
                        page.keys[node * key_cap + i + 1 .. node * key_cap + val_cap],
                        page.keys[node * key_cap + i .. node * key_cap + val_cap - 1],
                    );
                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});
                    std.mem.copyBackwards(
                        V,
                        page.vals[node * val_cap + i + 1 .. node * val_cap + val_cap],
                        page.vals[node * val_cap + i .. node * val_cap + val_cap - 1],
                    );
                    page.keys[node * key_cap + i] = key;
                    page.vals[node * val_cap + i] = val;
                    page.node[node] = page.keys[node * key_cap + val_cap - 1];
                    //                 std.debug.print("{any}\n", .{page.keys[node * key_cap .. node * key_cap + val_cap]});

                    //                 std.debug.print("direct insertion with displacement\n", .{});
                    return @call(
                        .always_tail,
                        insert,
                        .{ page, alloc, displaced_key, displaced_val },
                    );
                }

                // this is not our data row, so keep searching down
                if (key > comp) {
                    node = 2 * (node + 1);
                } else {
                    node = 2 * (node + 1) - 1;
                }

                if (node < 15) {
                    comp = page.node[node];
                } else if (page.children[node - 15] != null) {
                    return try @call(
                        .always_tail,
                        insert,
                        .{ page.children[node - 15].?, alloc, key, val },
                    );
                } else {
                    page.children[node - 15] = try Self.create(alloc, page, key, val);
                    return;
                }
            }

            unreachable;
        }

        fn debugPrint(page: *Self, depth: u32) void {
            for (0..depth) |_| std.debug.print(" ", .{});
            std.debug.print("{any}\n", .{page.node[0..1]});
            for (0..depth) |_| std.debug.print(" ", .{});
            std.debug.print("{any}\n", .{page.node[1..3]});
            for (0..depth) |_| std.debug.print(" ", .{});
            std.debug.print("{any}\n", .{page.node[3..7]});
            for (0..depth) |_| std.debug.print(" ", .{});
            std.debug.print("{any}\n", .{page.node[7..15]});

            for (page.children) |child| {
                if (child == null) continue;
                child.?.debugPrint(depth + 1);
            }
        }
    };
}

test "page layout" {
    try std.testing.expect(@sizeOf(Page(f32)) <= MEMORY_PAGE_SIZE);
    try std.testing.expect(@sizeOf(Page(f64)) <= MEMORY_PAGE_SIZE);
    try std.testing.expect(@sizeOf(Page(@Vector(2, f32))) <= MEMORY_PAGE_SIZE);
    try std.testing.expect(@sizeOf(Page(@Vector(4, f32))) <= MEMORY_PAGE_SIZE);
    try std.testing.expect(@sizeOf(Page(@Vector(8, f32))) <= MEMORY_PAGE_SIZE);
    try std.testing.expect(@sizeOf(Page(@Vector(16, f32))) <= MEMORY_PAGE_SIZE);

    // std.debug.print("\n", .{});
    // {
    //     const P = Page(Entity, @Vector(1, f32));
    //     const p: P = undefined;
    //     std.debug.print("{} {}\n", .{
    //         @sizeOf(P),
    //         p.vals.len,
    //     });
    // }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // var i: u32 = 2_654_435_761;
    // for (0..100_000) |j| {
    //     // std.debug.print("trying to insert\t{}\t{}\n", .{ i, j });
    //     // p.debugPrint(0);
    //     try p.insert(alloc, i, j);
    //     i +%= 2_654_435_761;
    // }

    // p.debugPrint(0);

    const M = 21;

    std.debug.print("len\tcrt_ns\tget_ns\n", .{});

    var acc: f32 = 0.0;

    for (0..M) |m| {
        const len = @as(usize, 1) << @intCast(m);
        var i: u32 = 2_654_435_761;
        var p = try Page(f32).create(alloc, null, i, 0);
        i +%= 2_654_435_761;
        defer p.destroy(alloc);

        var timer = try std.time.Timer.start();

        for (0..len) |j| {
            try p.insert(alloc, i, @as(f32, @floatFromInt(j)));
            i +%= 2_654_435_761;
        }

        const create_time: f64 = @floatFromInt(timer.lap());

        i = 2_654_435_761;
        i +%= 2_654_435_761;
        for (0..len) |_| {
            const res = p.find(i);
            acc += @as(f32, @floatFromInt(res.i));
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

test "scratch" {
    std.debug.print("\n", .{});
    var p = try Page(usize).create(std.testing.allocator, null, 100, 0);
    defer p.destroy(std.testing.allocator);
    p.debugPrint(0);

    var res99 = p.find(99);
    std.debug.print("{}\t{}\t{}\n", .{ res99.success, res99.n, res99.i });
    var res100 = p.find(100);
    std.debug.print("{}\t{}\t{}\n", .{ res100.success, res100.n, res100.i });
    var res101 = p.find(101);
    std.debug.print("{}\t{}\t{}\n", .{ res101.success, res101.n, res101.i });

    try p.insert(std.testing.allocator, 99, 1);
    try p.insert(std.testing.allocator, 101, 2);
    p.debugPrint(0);

    res99 = p.find(99);
    std.debug.print("{}\t{}\t{}\n", .{ res99.success, res99.n, res99.i });
    res100 = p.find(100);
    std.debug.print("{}\t{}\t{}\n", .{ res100.success, res100.n, res100.i });
    res101 = p.find(101);
    std.debug.print("{}\t{}\t{}\n", .{ res101.success, res101.n, res101.i });
}
