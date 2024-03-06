const std = @import("std");
const utl = @import("utils.zig");

// so the first trashtree was trash, no surprise
// here's a new idea
// ultimately, I want a system that can run on a fixed size allocator (probably 16k sized)
// so, what if we used one page as an index that indexes into hashmaps on other pages
// we can track the min/max values inserted into a map (maybe even an approximate median)
// and if a hashmap page gets too full, we split it by the median key
// and update the index to match the new key ranges

pub fn Storage(
    comptime K: type,
    comptime V: type,
    comptime INDEX_SIZE: comptime_int,
    comptime PAGE_SIZE: comptime_int,
) type {
    return struct {
        const nil = std.math.maxInt(K);

        const Self = @This();

        const KV = struct { key: K, val: V };

        const Page = struct {
            keys: [PAGE_SIZE]K,
            vals: [PAGE_SIZE]V,
            // skip: [SIZE]u16, // for faster iteration
            len: usize,
            min: K,
            max: K,
            // some way to estimate median for uneven distributions

            fn create(alloc: std.mem.Allocator) !*Page {
                var page = try alloc.create(Page);
                page.keys = .{nil} ** PAGE_SIZE;
                page.len = 0;
                page.min = std.math.maxInt(K);
                page.max = 0;
                return page;
            }

            fn destroy(page: *Page, alloc: std.mem.Allocator) void {
                alloc.destroy(page);
            }

            fn load(page: *Page) f32 {
                return @as(f32, @floatFromInt(page.len)) / PAGE_SIZE;
            }

            fn add(page: *Page, key: K, val: V) void {
                std.debug.assert(page.len < PAGE_SIZE);

                var loc = std.hash.uint32(key) % PAGE_SIZE;
                while (page.keys[loc] != nil) : (loc = (loc + 1) % PAGE_SIZE) {}
                page.keys[loc] = key;
                page.vals[loc] = val;
                page.min = @min(page.min, key);
                page.max = @max(page.max, key);
                page.len += 1;
            }

            fn get(page: *Page, key: K) ?*V {
                var loc = std.hash.uint32(key) % PAGE_SIZE;

                for (0..PAGE_SIZE) |_| {
                    if (page.keys[loc] == key) {
                        return &page.vals[loc];
                    } else if (page.keys[loc] == nil) {
                        return null;
                    }
                    loc = (loc + 1) % PAGE_SIZE;
                }
                return null;
            }

            const Iterator = struct {
                page: *Page,
                cursor: usize = 0,

                pub fn next(it: *Page.Iterator) ?KV {
                    if (it.cursor == PAGE_SIZE) return null;
                    while (it.cursor < PAGE_SIZE and
                        it.page.keys[it.cursor] == nil) : (it.cursor += 1)
                    {}
                    if (it.cursor == PAGE_SIZE) return null;
                    it.cursor += 1;
                    return .{
                        .key = it.page.keys[it.cursor - 1],
                        .val = it.page.vals[it.cursor - 1],
                    };
                }
            };

            pub fn iterator(page: *Page) Page.Iterator {
                return .{ .page = page };
            }

            fn debugPrint(page: *Page) void {
                std.debug.print("  [ ", .{});
                for (0..PAGE_SIZE) |i| {
                    if (page.keys[i] == nil) continue;
                    std.debug.print("{} ", .{page.keys[i]});
                }
                std.debug.print("]\n", .{});
            }
        };

        const Index = struct {
            limits: [INDEX_SIZE - 1]K align(64),
            pages: [INDEX_SIZE]?*Page align(64),
            n_pages: u32,

            fn create(alloc: std.mem.Allocator) !*Index {
                var index = try alloc.create(Index);
                index.pages = .{null} ** INDEX_SIZE;
                index.n_pages = 0;
                return index;
            }

            fn destroy(index: *Index, alloc: std.mem.Allocator) void {
                for (0..index.n_pages) |i| {
                    index.pages[i].?.destroy(alloc);
                }
                alloc.destroy(index);
            }

            fn add(index: *Index, alloc: std.mem.Allocator, key: K, val: V) !void {
                const loc: u32 = @min(
                    utl.lowerBound(K, &index.limits, @intCast(index.n_pages), key),
                    @as(u32, @intCast(index.n_pages - 1)),
                );

                if (index.n_pages < INDEX_SIZE and index.pages[loc].?.load() > 0.8) {
                    const low = try Page.create(alloc);
                    errdefer low.destroy(alloc);
                    const high = try Page.create(alloc);
                    const full = index.pages[loc].?;
                    const median = full.min + (full.max - full.min) / 2;

                    var it = full.iterator();
                    while (it.next()) |kv| {
                        if (kv.key < median) {
                            low.add(kv.key, kv.val);
                        } else {
                            high.add(kv.key, kv.val);
                        }
                    }
                    full.destroy(alloc);

                    index.pages[loc] = high;
                    utl.insert(?*Page, &index.pages, index.n_pages, loc, low);

                    if (loc > 0) index.limits[loc - 1] = low.min;
                    utl.insert(K, &index.limits, index.n_pages, loc, high.min);

                    index.n_pages += 1;

                    @call(.always_tail, Index.add, .{ index, alloc, key, val }) catch unreachable;
                }

                if (index.pages[loc].?.len == PAGE_SIZE) return error.StorageFull;
                index.pages[loc].?.add(key, val);
            }

            fn get(index: *Index, key: K) ?*V {
                const loc: u32 = @min(
                    utl.lowerBound(K, &index.limits, @intCast(index.n_pages), key),
                    @as(u32, @intCast(index.n_pages - 1)),
                );

                return index.pages[loc].?.get(key);
            }

            fn debugPrint(index: *Index) void {
                std.debug.print("  {any}\n", .{index.limits[0 .. index.n_pages - 1]});
                for (0..index.n_pages) |i| {
                    index.pages[i].?.debugPrint();
                }
            }
        };

        alloc: std.mem.Allocator,
        index: ?*Index,
        len: usize,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .len = 0,
                .index = null,
            };
        }

        pub fn deinit(storage: *Self) void {
            if (storage.index) |index| index.destroy(storage.alloc);
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: K) ?*V {
            if (storage.index) |index| return index.get(key);
            return null;
        }

        pub fn add(storage: *Self, key: K, val: V) !void {
            if (storage.index == null) {
                storage.index = try Index.create(storage.alloc);
                errdefer storage.index.?.destroy(storage.alloc);
                storage.index.?.pages[0] = try Page.create(storage.alloc);
                storage.index.?.n_pages = 1;
                storage.len = 1;
            }

            try storage.index.?.add(storage.alloc, key, val);
            storage.len += 1;
        }

        const Iterator = struct {
            index: ?*Index,
            cursor: usize = std.math.maxInt(usize),
            it: Page.Iterator = undefined,

            pub fn next(it: *Iterator) ?KV {
                if (it.index == null or it.cursor == it.index.?.n_pages) return null;

                if (it.cursor == std.math.maxInt(usize)) {
                    // first time iterator setup
                    it.cursor = 0;
                    it.it = it.index.?.pages[it.cursor].?.iterator();
                }

                const kv = it.it.next();
                if (kv == null) {
                    it.cursor += 1;
                    if (it.cursor < it.index.?.n_pages) {
                        it.it = it.index.?.pages[it.cursor].?.iterator();
                        // return @call(.always_tail, Iterator.next, .{it});
                        return it.next();
                    }

                    return null;
                }
                return kv.?;
            }
        };

        pub fn iterator(storage: *Self) Iterator {
            return .{ .index = storage.index };
        }

        pub fn debugPrint(storage: *Self) void {
            std.debug.print("Storage <{s} {s}>\n", .{ @typeName(K), @typeName(V) });
            if (storage.index) |index| {
                index.debugPrint();
            } else {
                std.debug.print("  {{}} []\n", .{});
            }
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage(u32, u32, 8, 8).init(alloc);
    defer s.deinit();

    s.debugPrint();
    for (0..32) |i| {
        const x: u32 = @intCast(i * 51 % 32);
        std.debug.print("\ninserting {}\n", .{x});
        try s.add(x, x);
        s.debugPrint();
        std.debug.assert(s.get(x).?.* == x);
        std.debug.assert(s.get(x + 32) == null);
    }

    var it = s.iterator();
    while (it.next()) |kv| {
        std.debug.print("{}\n", .{kv.key});
    }

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u32, 1024, 1024).Index),
        @sizeOf(Storage(u32, u32, 1024, 1024).Page),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u64, 1024, 1024).Index),
        @sizeOf(Storage(u32, u64, 1024, 1024).Page),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u128, 1024, 512).Index),
        @sizeOf(Storage(u32, u128, 1024, 512).Page),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u256, 1024, 256).Index),
        @sizeOf(Storage(u32, u256, 1024, 256).Page),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u512, 1024, 128).Index),
        @sizeOf(Storage(u32, u512, 1024, 128).Page),
    });

    std.debug.print("{} {}\n", .{
        @sizeOf(Storage(u32, u1024, 1024, 64).Index),
        @sizeOf(Storage(u32, u1024, 1024, 64).Page),
    });
}
