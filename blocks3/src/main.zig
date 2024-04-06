const std = @import("std");

// note, though I won't bother with a type-erased and auto-sized page here
// the idea is to run this on a 65536 byte block allocator

const PAGE_SIZE = 2701; // must be less than 4096 because of index type in State.Detail

pub const Entity = u64;
pub const nil: Entity = 0;

const Time = u64;

fn Page(comptime V: type) type {
    return struct {
        const Self = @This();

        keys: [PAGE_SIZE]Entity,
        vals: [PAGE_SIZE]V,
        dels: [PAGE_SIZE]Time,
        time: Time,
        len: usize,
        rc: usize,

        fn create(alloc: std.mem.Allocator, time: Time) *Self {
            const page = alloc.create(Self) catch @panic("oom");
            page.len = 0;
            page.rc = 1;
            page.time = time;
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

pub const State = struct {
    const Self = @This();

    const BUCKET_SIZE = 16369; // prime and uses most of the space
    // const BUCKET_SIZE = 12289; // prime and far away from power of two (improves hash quality?)

    const Indirect = u32;
    const Detail = packed struct {
        fingerprint: u8,
        page: u11,
        ix: u12,
        nil: bool, // we could store this in page/ix or as a special fingerprint...
        // (maxInt(ix) can never be used anyway, due to extra data in the pages)
    };

    pub const Bucket = struct {
        sparse: [BUCKET_SIZE]Indirect,
        // extendible or linear hashing (TODO)
    };

    alloc: std.mem.Allocator,
    buckets: [2048]?*Bucket, // 2048 buckets is overkill but we have the space...
    // extendible or linear hashing (TODO) but for this test, just use one page
    pages: [2048]?*anyopaque, // capped by Detail.page type
    empty: std.StaticBitSet(2048),
    active_page: usize,
    time: Time,

    pub fn create(alloc: std.mem.Allocator) *Self {
        const state = alloc.create(State) catch @panic("oom");
        state.buckets[0] = alloc.create(Bucket) catch @panic("oom");
        state.empty = std.StaticBitSet(2048).initFull();
        state.alloc = alloc;
        state.time = 0;
        state.active_page = std.math.maxInt(usize);
        return state;
    }

    pub fn destroy(state: *State, comptime V: type) void {
        state.alloc.destroy(state.buckets[0].?);
        var it = state.empty.iterator(.{ .kind = .unset });
        while (it.next()) |ix| {
            state.page(V, ix).destroy(state.alloc);
        }
        state.alloc.destroy(state);
    }

    pub fn set(state: *State, comptime V: type, key: Entity, val: V) void {
        if (state.active_page == std.math.maxInt(usize)) state.newPage(V);
        const active_page = state.page(V, state.active_page);
        if (active_page.len == active_page.keys.len) state.newPage(V);

        const h = hash(key);
        // TODO how do we get the least correlated bits compared to loc?
        // if the high-bits are low quality, then this isn't great.
        // we could do modulo a mersenne prime and mask the 255 low bits?
        // (needs to be mersenne to get an even distribution of fingerprints)
        const fingerprint: u8 = @intCast(h >> 24);
        var probe: u32 = 0;
        while (true) : (probe += 1) {
            const loc = (h +% probe) % BUCKET_SIZE;
            const d: Detail = @bitCast(state.buckets[0].?.sparse[loc]);

            if (d.nil) {
                const new_d = Detail{
                    .fingerprint = fingerprint,
                    .page = @intCast(state.active_page),
                    .ix = @intCast(active_page.push(key, val)),
                    .nil = false,
                };
                state.buckets[0].?.sparse[loc] = @bitCast(new_d);
                return;
            } else if (fingerprint == d.fingerprint) {

                // lookup the actual key
                const d_page = state.page(V, d.page);
                if (d_page.time == state.time) {
                    d_page.keys[d.ix] = key;
                    d_page.vals[d.ix] = val;
                    return;
                }
                // key exists, but is not recent, so del and insert into active page
                d_page.dels[d.ix] = state.time;
                const new_d = Detail{
                    .fingerprint = fingerprint,
                    .page = @intCast(state.active_page),
                    .ix = @intCast(active_page.push(key, val)),
                    .nil = false,
                };
                // TODO check overflow in page and get a new page if needed
                state.buckets[0].?.sparse[loc] = @bitCast(new_d);
            }
        }
    }

    pub fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(2701, std.mem.asBytes(&key));
    }

    fn page(state: *State, comptime V: type, ix: usize) *Page(V) {
        std.debug.assert(state.pages[ix] != null);
        return @alignCast(@ptrCast(state.pages[ix].?));
    }

    fn newPage(state: *State, comptime V: type) void {
        state.active_page = state.empty.findFirstSet() orelse @panic("no empty pages");
        state.empty.unset(state.active_page);
        state.pages[state.active_page] = Page(V).create(state.alloc, state.time);
    }

    fn debugPrint(state: *State, comptime V: type) void {
        std.debug.print("State {} <{s}>\n", .{ state.time, @typeName(V) });
        var it = state.empty.iterator(.{ .kind = .unset });
        while (it.next()) |ix| {
            state.page(V, ix).debugPrint(state.time);
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
    s.set(f64, 3, 1.0);
    s.set(f64, 2, 1.0);
    s.debugPrint(f64);
}

pub fn main() void {}
