const std = @import("std");

// the semantics of copy on write provides some really awkward footguns
// so what if we keep a copy of everything
// and if it's updated, we then copy again in cycle
// but otherwise we can just keep the unedited copy

const PAGE_SIZE = 2701; // must be <= 8190 because of index type in State.Detail and flag values

pub const Entity = u64;
pub const nil: Entity = 0;

fn Page(comptime V: type) type {
    return struct {
        const Self = @This();

        keys: [PAGE_SIZE]Entity,
        vals: [PAGE_SIZE]V,
        modified: bool,
        len: usize,
        rc: usize,

        fn create(alloc: std.mem.Allocator) *Self {
            const page = alloc.create(Self) catch @panic("oom");
            page.modified = false;
            page.len = 0;
            page.rc = 1;
            return page;
        }

        fn destroy(page: *Self, alloc: std.mem.Allocator) void {
            page.rc -= 1;
            if (page.rc == 0) {
                alloc.destroy(page);
            }
        }

        /// returns index of element
        fn push(page: *Self, key: Entity, val: V) usize {
            if (page.len == page.keys.len) @panic("page is full");
            const ix = page.len;
            page.keys[ix] = key;
            page.vals[ix] = val;
            page.len += 1;
            return ix;
        }

        fn debugPrint(page: *Self) void {
            if (page.modified) {
                std.debug.print(" ~[ ", .{});
            } else {
                std.debug.print("  [ ", .{});
            }
            for (0..page.len) |i| {
                std.debug.print("{} ", .{page.keys[i]});
            }
            if (page.modified) {
                std.debug.print("]~\n", .{});
            } else {
                std.debug.print("]\n", .{});
            }
        }
    };
}

pub const State = struct {
    const Self = @This();

    const BUCKET_SIZE = 16369; // prime and uses most of the space
    // const BUCKET_SIZE = 8192; // prime and uses most of the space
    // const BUCKET_SIZE = 12289; // prime and far away from power of two (improves hash quality?)

    const Indirect = u32;
    const Detail = packed struct {
        fingerprint: u8,
        page: u11,
        ix: u13,
    };
    const ix_nil = std.math.maxInt(u12);
    const ix_tomb = std.math.maxInt(u12) - 1;

    pub const Bucket = struct {
        sparse: [BUCKET_SIZE]Indirect,
        // extendible or linear hashing (TODO)
    };

    alloc: std.mem.Allocator,
    buckets: [2048]?*Bucket, // 2048 buckets is overkill but we have the space...
    // extendible or linear hashing (TODO) but for this test, just use one page
    pages: [2048]?*anyopaque, // capped by Detail.page type
    n_pages: usize,
    active_page: usize,

    pub fn create(alloc: std.mem.Allocator) *Self {
        const state = alloc.create(State) catch @panic("oom");
        state.buckets[0] = alloc.create(Bucket) catch @panic("oom");
        state.buckets[0].?.sparse = .{@as(u32, @bitCast(Detail{
            .fingerprint = undefined,
            .page = undefined,
            .ix = ix_nil,
        }))} ** BUCKET_SIZE;
        state.n_pages = 0;
        state.alloc = alloc;
        state.active_page = std.math.maxInt(usize);
        return state;
    }

    pub fn step(state: *State, comptime V: type) *Self {
        const new_state = state.alloc.create(State) catch @panic("oom");
        // NOTE with an extendible hashing scheme this needs more care
        new_state.buckets[0] = state.alloc.create(Bucket) catch @panic("oom");
        new_state.buckets[0].?.* = state.buckets[0].?.*;

        for (0..state.n_pages) |i| {
            if (state.page(V, i).modified) {
                // copy the page
                new_state.pages[i] = Page(V).create(state.alloc);
                new_state.page(V, i).* = state.page(V, i).*;
                new_state.page(V, i).modified = false;
            } else {
                new_state.pages[i] = state.pages[i];
                new_state.page(V, i).rc += 1;
            }
        }

        new_state.n_pages = state.n_pages;
        new_state.alloc = state.alloc;
        new_state.active_page = state.active_page;
        return new_state;
    }

    pub fn destroy(state: *State, comptime V: type) void {
        state.alloc.destroy(state.buckets[0].?);
        for (0..state.n_pages) |i| {
            state.page(V, i).destroy(state.alloc);
        }
        state.alloc.destroy(state);
    }

    pub fn set(state: *State, comptime V: type, key: Entity, val: V) void {
        if (state.active_page == std.math.maxInt(usize)) state.newPage(V);
        var active_page = state.page(V, state.active_page);
        if (active_page.len == active_page.keys.len) {
            state.newPage(V);
            active_page = state.page(V, state.active_page);
        }

        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        // var probe: u32 = 0;
        for (0..BUCKET_SIZE) |probe| {
            // while (true) : (probe += 1) {
            const loc = (h +% probe) % BUCKET_SIZE;
            const d: Detail = @bitCast(state.buckets[0].?.sparse[loc]);

            // NOTE not implemented but: rely on dynamic hashing to clean tombstones
            if (d.ix == ix_tomb) continue;
            if (d.ix == ix_nil) {
                const new_d = Detail{
                    .fingerprint = fingerprint,
                    .page = @intCast(state.active_page),
                    .ix = @intCast(active_page.push(key, val)),
                };
                state.buckets[0].?.sparse[loc] = @bitCast(new_d);
                active_page.modified = true;
                return;
            } else if (fingerprint == d.fingerprint) {
                const d_page = state.page(V, d.page);
                if (key != d_page.keys[d.ix]) continue;
                d_page.keys[d.ix] = key;
                d_page.vals[d.ix] = val;
                const new_d = Detail{
                    .fingerprint = fingerprint,
                    .page = d.page,
                    .ix = d.ix,
                };
                state.buckets[0].?.sparse[loc] = @bitCast(new_d);
                d_page.modified = true;
                return;
            }
        }
        @panic("bucket is full");
    }

    pub fn has(state: *State, comptime V: type, key: Entity) bool {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        // var probe: u32 = 0;
        for (0..BUCKET_SIZE) |probe| {
            // while (true) : (probe += 1) {
            const loc = (h +% probe) % BUCKET_SIZE;
            const d: Detail = @bitCast(state.buckets[0].?.sparse[loc]);
            if (d.ix == ix_tomb) continue;
            if (d.ix == ix_nil) return false;
            const d_page = state.page(V, d.page);

            if (fingerprint == d.fingerprint and key == d_page.keys[d.ix]) {
                return true;
            }
        }
        @panic("bucket is full");
    }

    /// counts as modifying the data for snapshot data-sharing
    pub fn get(state: *State, comptime V: type, key: Entity) ?*V {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        // var probe: u32 = 0;
        for (0..BUCKET_SIZE) |probe| {
            // while (true) : (probe += 1) {
            const loc = (h +% probe) % BUCKET_SIZE;
            const d: Detail = @bitCast(state.buckets[0].?.sparse[loc]);
            if (d.ix == ix_tomb) continue;
            if (d.ix == ix_nil) return null;
            const d_page = state.page(V, d.page);

            if (fingerprint == d.fingerprint and key == d_page.keys[d.ix]) {
                d_page.modified = true;
                return &d_page.vals[d.ix];
            }
        }
        @panic("bucket is full");
    }

    pub fn del(state: *State, comptime V: type, key: Entity) void {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        // var probe: u32 = 0;
        for (0..BUCKET_SIZE) |probe| {
            // while (true) : (probe += 1) {
            const loc = (h +% probe) % BUCKET_SIZE;
            const d: Detail = @bitCast(state.buckets[0].?.sparse[loc]);
            const d_page = state.page(V, d.page);

            if (d.ix == ix_tomb) continue;
            if (d.ix == ix_nil) return;
            if (fingerprint == d.fingerprint and key == d_page.keys[d.ix]) {
                // swap erase...

                // we should probably swap with the actual last entry
                // or we could run into half-full page issues
                // but this is simpler and fine for the prototype

                // we can't simply use the active page, as then we'd need to deal with the special
                // case of it becoming empty

                std.debug.assert(d_page.len > 0);
                const last_key = d_page.keys[d_page.len - 1];
                const last_val = d_page.vals[d_page.len - 1];
                const last_h = hash(last_key);
                const last_fingerprint: u8 = @intCast(last_h >> 24);

                // std.debug.print("{} {} {}\n", .{ key, last_key, d_page.len });

                // var last_probe: u32 = 0;
                for (0..BUCKET_SIZE) |last_probe| {
                    // while (true) : (last_probe += 1) {
                    const last_loc = (last_h +% last_probe) % BUCKET_SIZE;
                    const last_d: Detail = @bitCast(state.buckets[0].?.sparse[last_loc]);
                    std.debug.assert(last_d.ix != ix_nil);
                    if (last_d.ix == ix_tomb) continue;
                    if (last_fingerprint == last_d.fingerprint) {
                        if (last_key == d_page.keys[last_d.ix]) {
                            // std.debug.print("{}\n", .{d});
                            // std.debug.print("{}\n", .{last_d});
                            // std.debug.assert(last_d.page == d.page);
                            // we found our entry, now we can swap-delete
                            const last_d_page = state.page(V, d.page);

                            d_page.keys[d.ix] = last_key;
                            d_page.vals[d.ix] = last_val;

                            const new_last_d = Detail{
                                .fingerprint = last_fingerprint,
                                .page = d.page,
                                .ix = d.ix,
                            };
                            state.buckets[0].?.sparse[last_loc] = @bitCast(new_last_d);

                            const new_d = Detail{
                                .fingerprint = undefined,
                                .page = undefined,
                                .ix = ix_tomb,
                            };
                            state.buckets[0].?.sparse[loc] = @bitCast(new_d);

                            last_d_page.len -= 1;
                            last_d_page.modified = true;
                            d_page.modified = true;
                            return;
                        }
                    }
                }

                @panic("bucket is full");
            }
        }
        @panic("bucket is full");
    }

    pub fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(2701, std.mem.asBytes(&key));
    }

    pub fn page(state: *State, comptime V: type, ix: usize) *Page(V) {
        std.debug.assert(state.pages[ix] != null);
        return @alignCast(@ptrCast(state.pages[ix].?));
    }

    fn newPage(state: *State, comptime V: type) void {
        std.debug.assert(state.n_pages < 2048);
        state.active_page = state.n_pages;
        state.n_pages += 1;
        state.pages[state.active_page] = Page(V).create(state.alloc);
    }

    fn debugPrint(state: *State, comptime V: type) void {
        std.debug.print("State <{s}>\n", .{@typeName(V)});
        for (0..state.n_pages) |i| {
            state.page(V, i).debugPrint();
        }
    }
};

comptime {
    std.debug.assert(@sizeOf(State) < 65536);
    std.debug.assert(@bitSizeOf(State.Indirect) == @bitSizeOf(State.Detail));
    std.debug.assert(@sizeOf(State.Bucket) < 65536);
}

test "scratch" {
    var s = State.create(std.testing.allocator);
    defer s.destroy(f64);

    std.debug.print("\n", .{});
    std.debug.print("{}\n", .{@sizeOf(Page(f64))});

    s.set(f64, 1, 1.0);
    s.set(f64, 3, 3.0);
    s.set(f64, 2, 2.0);
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    {
        var sprev = s;
        defer sprev.destroy(f64);
        s = s.step(f64);
    }

    s.debugPrint(f64);
    s.set(f64, 2, 2.0);
    s.debugPrint(f64);

    {
        var sprev = s;
        defer sprev.destroy(f64);
        s = s.step(f64);
    }

    s.debugPrint(f64);
    s.set(f64, 1, 1.0);
    s.set(f64, 4, 4.0);
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    s.del(f64, 3);
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    {
        var sprev = s;
        defer sprev.destroy(f64);
        s = s.step(f64);
    }
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    s.del(f64, 3);
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    s.set(f64, 3, 3.0);
    s.debugPrint(f64);

    s.del(f64, 1);
    s.del(f64, 2);
    s.del(f64, 3);
    s.del(f64, 4);
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    {
        var sprev = s;
        defer sprev.destroy(f64);
        s = s.step(f64);
    }
    s.debugPrint(f64);

    std.debug.print("{}\n", .{s.has(f64, 1)});
    std.debug.print("{}\n", .{s.has(f64, 2)});
    std.debug.print("{}\n", .{s.has(f64, 3)});
    std.debug.print("{}\n", .{s.has(f64, 4)});

    // sprev.debugPrint(f64);
}

test "storage fuzz" {
    const n = 1000;
    const m = 16;
    const l = 1000;

    for (0..l) |_| {
        var s = State.create(std.testing.allocator);
        defer s.destroy(f64);
        var h = std.AutoHashMap(Entity, f64).init(std.testing.allocator);
        defer h.deinit();
        var c = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
        defer c.deinit();

        var rng = std.rand.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
        const rand = rng.random();

        for (0..m) |_| {
            var a = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
            defer a.deinit();

            for (0..n) |_| {
                const k = (rand.int(Entity) | 1) & 63;
                const v = rand.float(f64);

                try std.testing.expect(s.has(f64, k) == h.contains(k));
                if (s.has(f64, k)) continue;

                // std.debug.print("{}\n", .{k});
                // s.debugPrint(f64);
                // std.debug.print("{} {}\n", .{ s.has(f64, k), h.contains(k) });

                s.set(f64, k, v);
                try h.put(k, v);
                try a.append(k);
                try c.append(k);
            }

            // s.debugPrint(f64);

            for (a.items) |k| {
                if (rand.boolean()) continue;

                // std.debug.print("{}\n", .{k});
                // s.debugPrint(f64);
                // std.debug.print("{} {}\n", .{ s.has(f64, k), h.contains(k) });

                try std.testing.expect(s.get(f64, k).?.* == h.getPtr(k).?.*);
                s.del(f64, k);
                try std.testing.expect(h.remove(k));

                // std.debug.print("{}\n", .{k});
                // s.debugPrint(f64);
                // std.debug.print("{} {}\n", .{ s.has(f64, k), h.contains(k) });

                try std.testing.expect(s.has(f64, k) == h.contains(k));
            }

            // for (c.items) |k| {
            //     if (rand.boolean()) continue;
            //     try std.testing.expect(s.has(f64, k) == h.contains(k));
            //     if (s.has(f64, k)) {
            //         s.del(f64, k);
            //         try std.testing.expect(h.remove(k));
            //     } else {
            //         const v = rand.float(f64);
            //         s.set(f64, k, v);
            //         try h.put(k, v);
            //     }
            //     try std.testing.expect(s.has(f64, k) == h.contains(k));
            // }

            var sprev = s;
            defer sprev.destroy(f64);
            s = s.step(f64);
        }
    }
}

pub fn main() void {}
