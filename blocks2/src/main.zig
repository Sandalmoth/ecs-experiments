const std = @import("std");

pub const Entity = u64;
pub const nil: Entity = 0;

const Time = u64;

const INDEX_SIZE = 1024;
const PAGE_SIZE = 1024;
const STATE_SIZE = 65536;

fn Page(comptime V: type) type {
    return struct {
        const Self = @This();

        keys: []Entity,
        vals: []V,
        dels: []Time,
        len: usize,
        rc: *usize,

        fn create(alloc: std.mem.Allocator) *Self {
            const page = alloc.create(Self) catch unreachable;
            page.keys = alloc.alloc(Entity, PAGE_SIZE) catch unreachable;
            page.vals = alloc.alloc(V, PAGE_SIZE) catch unreachable;
            page.dels = alloc.alloc(Time, PAGE_SIZE) catch unreachable;
            page.len = 0;
            page.rc = alloc.create(usize) catch unreachable;
            page.rc.* = 1;
            return page;
        }

        fn destroy(page: *Self, alloc: std.mem.Allocator) bool {
            page.rc.* -= 1;
            if (page.rc.* == 0) {
                alloc.free(page.keys);
                alloc.free(page.vals);
                alloc.free(page.dels);
                alloc.destroy(page.rc);
                alloc.destroy(page);
                return true;
            }
            return false;
        }

        /// returns index of element
        fn push(page: *Self, key: Entity, val: V) usize {
            if (page.len == page.keys.len) @panic("page is full");
            const ix = page.len;
            page.keys[ix] = key;
            page.vals[ix] = val;
            page.dels[ix] = std.math.maxInt(Time);
            page.len += 1;
            return ix;
        }

        fn debugPrint(page: *Self, t: Time) void {
            std.debug.print("  [ ", .{});
            for (0..page.len) |i| {
                if (t >= page.dels[i]) std.debug.print("~", .{});
                std.debug.print("{} ", .{page.keys[i]});
            }
            std.debug.print("]\n", .{});
        }
    };
}

pub fn State(comptime V: type) type {
    return struct {
        const Self = @This();
        const Indirect = u32;
        const Detail = packed struct {
            fingerprint: u8,
            page: u10,
            ix: u13,
            nil: bool,
        };

        comptime {
            std.debug.assert(@bitSizeOf(Indirect) == @bitSizeOf(Detail));
        }

        sparse: []Indirect,
        prev: ?*Self,
        time: Time,
        storage: *Storage(V),
        active_page: usize, // index into Storage.Index.Pages

        pub fn init(storage: *Storage(V)) Self {
            return .{
                .sparse = storage.alloc.alloc(Indirect, STATE_SIZE) catch @panic("oom"),
                .prev = null,
                .time = 0,
                .storage = storage,
                .active_page = storage.newPage(),
            };
        }

        pub fn deinit(state: *Self) void {
            if (state.prev) |prev| prev.deinit();
            _ = state.storage.index.pages[state.active_page].page.?.destroy(state.storage.alloc);
            state.storage.alloc.free(state.sparse);
        }

        pub fn add(state: *Self, key: Entity, val: V) void {
            const h = hash(key);
            var loc = h % state.sparse.len;
            const fingerprint: u8 = @intCast(h >> 24);

            while (true) : (loc = (loc + 1) % state.sparse.len) {
                const d: Detail = @bitCast(state.sparse[loc]);

                if (d.nil) {
                    // empty slot, insert into new page
                    const new_d = Detail{
                        .fingerprint = fingerprint,
                        .page = @intCast(state.active_page),
                        .ix = @intCast(state.storage.index.pages[state.active_page].page.?.push(key, val)),
                        .nil = false,
                    };
                    // TODO check overflow in page and get a new page if needed
                    state.sparse[loc] = @bitCast(new_d);
                    return;
                } else if (fingerprint == d.fingerprint) {
                    // lookup the actual key
                    const page = state.storage.index.pages[d.page].page.?;
                    if (key == page.keys[d.ix]) {
                        if (d.page == state.active_page) {
                            // we shoudl allow writing to any page owned by this state
                            // rather than only the active page
                            // we might need a next pointer in page (for destroying the chain)
                            // as well as a time on the page, to identify belonging to a state
                            std.log.debug("called add on already existing key {}", .{key});
                            page.keys[d.ix] = key;
                            page.vals[d.ix] = val;
                            return;
                        }
                        // key exists, but is not recent, del and insert into active page
                        page.dels[d.ix] = state.time;
                        const new_d = Detail{
                            .fingerprint = fingerprint,
                            .page = @intCast(state.active_page),
                            .ix = @intCast(state.storage.index.pages[state.active_page].page.?.push(key, val)),
                            .nil = false,
                        };
                        // TODO check overflow in page and get a new page if needed
                        state.sparse[loc] = @bitCast(new_d);
                    }
                }
            }
        }

        pub fn get(state: *Self, key: Entity) ?*V {
            const h = hash(key);
            var loc = h % state.sparse.len;
            const fingerprint: u8 = @intCast(h >> 24);

            while (true) : (loc = (loc + 1) % state.sparse.len) {
                const d: Detail = @bitCast(state.sparse[loc]);
                if (d.nil) return null;
                if (fingerprint == d.fingerprint) {
                    const page = state.storage.index.pages[d.page].page.?;
                    if (key == page.keys[d.ix]) return &page.vals[d.ix];
                }
            }
        }

        pub fn hash(key: Entity) u32 {
            return std.hash.XxHash32.hash(2701, std.mem.asBytes(&key));
        }
    };
}

pub fn Storage(comptime V: type) type {
    return struct {
        const Self = @This();

        const Node = union {
            next: usize,
            page: ?*Page(V),
        };

        const Index = struct {
            pages: [INDEX_SIZE]Node,
            free_head: usize,
        };

        alloc: std.mem.Allocator,
        index: *Index,

        pub fn init(alloc: std.mem.Allocator) Self {
            const storage = Self{
                .alloc = alloc,
                .index = alloc.create(Index) catch @panic("oom"),
            };

            storage.index.pages[0] = .{ .next = std.math.maxInt(usize) };
            for (1..INDEX_SIZE) |i| {
                storage.index.pages[i] = .{ .next = i - 1 };
            }
            storage.index.free_head = INDEX_SIZE - 1;

            return storage;
        }

        pub fn deinit(storage: *Self) void {
            storage.alloc.destroy(storage.index);
            storage.* = undefined;
        }

        pub fn get(storage: *Self, key: Entity) ?*V {
            _ = storage;
            _ = key;
            return null;
        }

        pub fn newPage(storage: *Self) usize {
            const page = Page(V).create(storage.alloc);
            const ix = storage.index.free_head;
            storage.index.free_head = storage.index.pages[ix].next;
            storage.index.pages[ix] = .{ .page = page };
            return ix;
        }

        // const Iterator = struct {
        //     const KV = struct { key: K, val: V };

        //     pub fn next(it: *Iterator) ?KV {
        //         _ = it;
        //         return null;
        //     }
        // };

        // pub fn iterator(storage: *Self) Iterator {
        //     _ = storage;
        //     return .{};
        // }
    };
}

test "scratch" {
    var r = Storage(f64).init(std.testing.allocator);
    defer r.deinit();

    var s = State(f64).init(&r);
    defer s.deinit();

    std.debug.print("\n", .{});

    s.add(1, 1.0);
    s.add(2, 1.0);
    std.debug.print("{?*}\n", .{s.get(1)});
    std.debug.print("{?*}\n", .{s.get(2)});
    std.debug.print("{?*}\n", .{s.get(3)});
}

pub fn main() void {}
