const std = @import("std");

// takeaway here is that lsm tree is maybe viable-ish
// however, sorted list + binary search is much to slow of a find structure
// but with a b-tree for l0 and some fast sorted index for l-+, get might be passable
// and since it's sorted, it is possible to add "archetypes" externally by letting
// entitiy id's have the archetype in the high bits.
// but the forced k-way merge during iteration in each component will always be slower
// than the array iteration in archetypes.
// though we do get the very nice features of recently written components being faster to access
// and rarely updated components lasting longer for keeping copies of old state

const Entity = u64;
const nil: Entity = 0;

fn orderEntity(context: void, lhs: usize, rhs: usize) std.math.Order {
    _ = context;
    return std.math.order(lhs, rhs);
}

const Page = struct {
    keys: []Entity,
    _vals: *anyopaque,
    tombs: []bool,
    rc: *usize,
    len: usize,

    fn create(comptime V: type, alloc: std.mem.Allocator, capacity: usize) Page {
        var page = Page{
            .keys = undefined,
            ._vals = undefined,
            .tombs = undefined,
            .rc = undefined,
            .len = 0,
        };
        page.keys = alloc.alloc(Entity, capacity) catch unreachable;
        page._vals = @ptrCast(alloc.alloc(V, capacity) catch unreachable);
        page.tombs = alloc.alloc(bool, capacity) catch unreachable;
        page.rc = alloc.create(usize) catch unreachable;
        page.rc.* = 1;
        return page;
    }

    /// creates a new page by merging a and b, with priority to a in case of duplicates
    fn createMerge(
        comptime V: type,
        alloc: std.mem.Allocator,
        a: Page,
        b: Page,
        malebolge: bool, // whether to preserve any tombstones in the output
    ) Page {
        var page = Page.create(V, alloc, a.len + b.len);

        var ca: usize = 0;
        var cb: usize = 0;
        while (ca < a.len and cb < b.len) {
            if (a.keys[ca] == b.keys[cb]) {
                if (!(malebolge and a.tombs[ca]))
                    page.append(V, a.keys[ca], a.vals(V)[ca], a.tombs[ca]);
                ca += 1;
                cb += 1;
            } else if (a.keys[ca] < b.keys[cb]) {
                if (!(malebolge and a.tombs[ca]))
                    page.append(V, a.keys[ca], a.vals(V)[ca], a.tombs[ca]);
                ca += 1;
            } else {
                if (!(malebolge and b.tombs[cb]))
                    page.append(V, b.keys[cb], b.vals(V)[cb], b.tombs[cb]);
                cb += 1;
            }
        }
        while (ca < a.len) : (ca += 1) {
            if (!(malebolge and a.tombs[ca]))
                page.append(V, a.keys[ca], a.vals(V)[ca], a.tombs[ca]);
        }
        while (cb < b.len) : (cb += 1) {
            if (!(malebolge and b.tombs[cb]))
                page.append(V, b.keys[cb], b.vals(V)[cb], b.tombs[cb]);
        }

        return page;
    }

    fn destroy(page: *Page, comptime V: type, alloc: std.mem.Allocator) void {
        page.rc.* -= 1;
        if (page.rc.* == 0) {
            alloc.free(page.keys);
            alloc.free(page.vals(V));
            alloc.free(page.tombs);
            alloc.destroy(page.rc);
        }
    }

    fn vals(page: Page, comptime V: type) []V {
        const v: [*]V = @alignCast(@ptrCast(page._vals));
        return v[0..page.keys.len];
    }

    fn has(page: *Page, key: Entity) bool {
        std.debug.assert(key != nil);
        const loc = std.sort.binarySearch(
            usize,
            key,
            page.keys[0..page.len],
            {},
            orderEntity,
        ) orelse return false;
        return !page.tombs[loc];
    }

    fn get(page: *Page, comptime V: type, key: Entity) ?*V {
        std.debug.assert(key != nil);
        const loc = std.sort.binarySearch(
            usize,
            key,
            page.keys[0..page.len],
            {},
            orderEntity,
        ) orelse return null;
        if (page.tombs[loc]) return null;
        return &page.vals(V)[loc];
    }

    const HasResult = struct { has: bool, tomb: bool };
    fn has2(page: *Page, key: Entity) HasResult {
        std.debug.assert(key != nil);
        const loc = std.sort.binarySearch(
            usize,
            key,
            page.keys[0..page.len],
            {},
            orderEntity,
        ) orelse return .{ .has = false, .tomb = undefined };
        return .{ .has = true, .tomb = page.tombs[loc] };
    }

    fn GetResult(comptime V: type) type {
        return struct { get: ?*V, tomb: bool };
    }
    fn get2(page: *Page, comptime V: type, key: Entity) GetResult(V) {
        std.debug.assert(key != nil);
        const loc = std.sort.binarySearch(
            usize,
            key,
            page.keys[0..page.len],
            {},
            orderEntity,
        ) orelse return .{ .get = null, .tomb = undefined };
        // note that .get is garbage (but not null) if tomb is true
        return .{ .get = &page.vals(V)[loc], .tomb = page.tombs[loc] };
    }

    fn add(page: *Page, comptime V: type, key: Entity, val: V) void {
        std.debug.assert(key != nil);
        std.debug.assert(page.len < page.keys.len);
        var left: usize = 0;
        var right: usize = page.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (page.keys[mid] >= key) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        if (page.keys[left] == key) {
            page.tombs[left] = false;
            page.vals(V)[left] = val;
            return;
        }
        std.mem.copyBackwards(Entity, page.keys[left + 1 ..], page.keys[left..page.len]);
        std.mem.copyBackwards(V, page.vals(V)[left + 1 ..], page.vals(V)[left..page.len]);
        std.mem.copyBackwards(bool, page.tombs[left + 1 ..], page.tombs[left..page.len]);
        page.keys[left] = key;
        page.vals(V)[left] = val;
        page.tombs[left] = false;
        page.len += 1;
        std.debug.assert(page.valid());
    }

    fn del(page: *Page, comptime V: type, key: Entity) void {
        std.debug.assert(key != nil);
        std.debug.assert(page.len < page.keys.len);
        var left: usize = 0;
        var right: usize = page.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (page.keys[mid] >= key) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        if (page.keys[left] == key) {
            page.tombs[left] = true;
            return;
        }
        std.mem.copyBackwards(Entity, page.keys[left + 1 ..], page.keys[left..page.len]);
        std.mem.copyBackwards(V, page.vals(V)[left + 1 ..], page.vals(V)[left..page.len]);
        std.mem.copyBackwards(bool, page.tombs[left + 1 ..], page.tombs[left..page.len]);
        page.keys[left] = key;
        page.tombs[left] = true;
        page.len += 1;
        std.debug.assert(page.valid());
    }

    /// note that this function asserts that we are sorted
    fn append(page: *Page, comptime V: type, key: Entity, val: V, tomb: bool) void {
        std.debug.assert(key != nil);
        std.debug.assert(page.len < page.keys.len);
        if (page.len > 0) {
            std.debug.assert(key > page.keys[page.len - 1]);
        }
        page.keys[page.len] = key;
        page.vals(V)[page.len] = val;
        page.tombs[page.len] = tomb;
        page.len += 1;
    }

    /// tests that we are sorted and duplicate free
    fn valid(page: *Page) bool {
        if (page.len < 2) return true;
        for (1..page.len) |i| {
            if (page.keys[i] <= page.keys[i - 1]) return false;
        }
        return true;
    }
};

test "page fuzz" {
    const n = 4 * 1024;
    var p = Page.create(f64, std.testing.allocator, n);
    defer p.destroy(f64, std.testing.allocator);
    var h = std.AutoHashMap(Entity, f64).init(std.testing.allocator);
    defer h.deinit();
    var a = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
    defer a.deinit();
    var b = try std.ArrayList(Entity).initCapacity(std.testing.allocator, n);
    defer b.deinit();

    var rng = std.rand.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..n - 1) |_| {
        const k = rand.int(Entity) | 1;
        const v = rand.float(f64);
        try std.testing.expect(p.has(k) == h.contains(k));
        if (p.has(k)) continue;
        p.add(f64, k, v);
        try h.put(k, v);
        try a.append(k);
    }

    for (a.items) |k| {
        if (rand.boolean()) continue;
        try std.testing.expect(p.get(f64, k).?.* == h.getPtr(k).?.*);
        p.del(f64, k);
        try std.testing.expect(h.remove(k));
        try b.append(k);
        try std.testing.expect(p.has(k) == h.contains(k));
    }

    for (b.items) |k| {
        const v = rand.float(f64);
        try std.testing.expect(p.has(k) == h.contains(k));
        try std.testing.expect(!p.has(k));
        p.add(f64, k, v);
        try h.put(k, v);
    }

    for (a.items) |k| {
        if (rand.boolean()) continue;
        try std.testing.expect(p.get(f64, k).?.* == h.getPtr(k).?.*);
        p.del(f64, k);
        try std.testing.expect(h.remove(k));
        try std.testing.expect(p.has(k) == h.contains(k));
    }
}

test "page merge" {
    var p1 = Page.create(f64, std.testing.allocator, 16);
    defer p1.destroy(f64, std.testing.allocator);
    var p2 = Page.create(f64, std.testing.allocator, 16);
    defer p2.destroy(f64, std.testing.allocator);

    p1.add(f64, 1, 1.0);
    p1.add(f64, 3, 3.0);
    p1.add(f64, 4, 4.0);

    p2.add(f64, 2, 2.0);
    p2.del(f64, 4);

    // std.debug.print("\n", .{});
    // {
    //     var p3 = Page.createMerge(f64, std.testing.allocator, p1, p2, false);
    //     defer p3.destroy(f64, std.testing.allocator);
    //     std.debug.print("{any}\n", .{p3.keys[0..p3.len]});
    //     std.debug.print("{any}\n", .{p3.tombs[0..p3.len]});
    // }
    // {
    //     var p3 = Page.createMerge(f64, std.testing.allocator, p2, p1, false);
    //     defer p3.destroy(f64, std.testing.allocator);
    //     std.debug.print("{any}\n", .{p3.keys[0..p3.len]});
    //     std.debug.print("{any}\n", .{p3.tombs[0..p3.len]});
    // }
    // {
    //     var p3 = Page.createMerge(f64, std.testing.allocator, p1, p2, true);
    //     defer p3.destroy(f64, std.testing.allocator);
    //     std.debug.print("{any}\n", .{p3.keys[0..p3.len]});
    //     std.debug.print("{any}\n", .{p3.tombs[0..p3.len]});
    // }
    // {
    //     var p3 = Page.createMerge(f64, std.testing.allocator, p2, p1, true);
    //     defer p3.destroy(f64, std.testing.allocator);
    //     std.debug.print("{any}\n", .{p3.keys[0..p3.len]});
    //     std.debug.print("{any}\n", .{p3.tombs[0..p3.len]});
    // }
}

const CAP_L0 = 4096;
const CAP_L1 = 2 * CAP_L0;

pub const Storage = struct {
    alloc: std.mem.Allocator,
    len: usize, // just an approximation

    l0: Page,
    l1: Page,
    l2: Page,

    pub fn init(comptime V: type, alloc: std.mem.Allocator) Storage {
        return .{
            .alloc = alloc,
            .len = 0,
            .l0 = Page.create(V, alloc, CAP_L0),
            .l1 = Page.create(V, alloc, 0),
            .l2 = Page.create(V, alloc, 0),
        };
    }

    pub fn deinit(storage: *Storage, comptime V: type) void {
        storage.l0.destroy(V, storage.alloc);
        storage.l1.destroy(V, storage.alloc);
        storage.l2.destroy(V, storage.alloc);
        storage.* = undefined;
    }

    pub fn has(storage: *Storage, key: Entity) bool {
        const r0 = storage.l0.has2(key);
        // std.debug.print("r0 {}\n", .{r0});
        if (r0.has) {
            return !r0.tomb;
        }
        const r1 = storage.l1.has2(key);
        // std.debug.print("r1 {}\n", .{r1});
        if (r1.has) {
            return !r1.tomb;
        }
        const r2 = storage.l2.has2(key);
        // std.debug.print("r2 {}\n", .{r2});
        if (r2.has) {
            return !r2.tomb;
        }
        return false;
    }

    pub fn get(storage: *Storage, comptime V: type, key: Entity) ?*V {
        const r0 = storage.l0.get2(V, key);
        if (r0.get) |v| {
            return if (!r0.tomb) v else null;
        }
        const r1 = storage.l0.get2(V, key);
        if (r1.get) |v| {
            return if (!r1.tomb) v else null;
        }
        const r2 = storage.l0.get2(V, key);
        if (r2.get) |v| {
            return if (!r2.tomb) v else null;
        }
        return null;
    }

    pub fn add(storage: *Storage, comptime V: type, key: Entity, val: V) void {
        storage.l0.add(V, key, val);
        storage.len += 1;
    }

    pub fn del(storage: *Storage, comptime V: type, key: Entity) void {
        storage.l0.del(V, key);
        if (storage.len > 0) storage.len -= 1;
    }

    pub fn compact(storage: *Storage, comptime V: type) void {
        const new_l1 = Page.createMerge(V, storage.alloc, storage.l0, storage.l1, false);
        storage.l0.destroy(V, storage.alloc);
        storage.l1.destroy(V, storage.alloc);
        storage.l0 = Page.create(V, storage.alloc, CAP_L0);
        storage.l1 = new_l1;

        if (storage.l1.len > CAP_L1) {
            const new_l2 = Page.createMerge(V, storage.alloc, storage.l1, storage.l2, true);
            storage.l1.destroy(V, storage.alloc);
            storage.l2.destroy(V, storage.alloc);
            storage.l1 = Page.create(V, storage.alloc, 0);
            storage.l2 = new_l2;
        }
    }

    fn Iterator(comptime V: type) type {
        return struct {
            const Entry = struct { key: Entity, val_ptr: *V };

            pub fn next(it: *Iterator) ?Entry {
                _ = it;
                return null;
            }
        };
    }

    pub fn iterator(storage: *Storage, comptime V: type) Iterator(V) {
        _ = storage;
        return .{};
    }
};

test "storage fuzz" {
    const n = 1024;
    const m = 16;
    var s = Storage.init(f64, std.testing.allocator);
    defer s.deinit(f64);
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
            const k = rand.int(Entity) | 1;
            const v = rand.float(f64);
            try std.testing.expect(s.has(k) == h.contains(k));
            if (s.has(k)) continue;
            s.add(f64, k, v);
            try h.put(k, v);
            try a.append(k);
            try c.append(k);
        }

        for (a.items) |k| {
            if (rand.boolean()) continue;
            try std.testing.expect(s.get(f64, k).?.* == h.getPtr(k).?.*);
            s.del(f64, k);
            try std.testing.expect(h.remove(k));
            try std.testing.expect(s.has(k) == h.contains(k));
        }

        for (c.items) |k| {
            if (s.l0.len == s.l0.keys.len) break;
            if (rand.boolean()) continue;
            try std.testing.expect(s.has(k) == h.contains(k));
            if (s.has(k)) {
                s.del(f64, k);
                try std.testing.expect(h.remove(k));
            } else {
                const v = rand.float(f64);
                s.add(f64, k, v);
                try h.put(k, v);
            }
            try std.testing.expect(s.has(k) == h.contains(k));
        }

        s.compact(f64);
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var s = Storage.init(f64, alloc);
    defer s.deinit(f64);

    s.add(f64, 1, 1.0);
    s.add(f64, 3, 3.0);
    s.add(f64, 4, 4.0);
    std.debug.print("{any}\t{any}\n{any}\t{any}\n{any}\t{any}\n\n", .{
        s.l0.keys[0..s.l0.len],
        s.l0.tombs[0..s.l0.len],
        s.l1.keys[0..s.l1.len],
        s.l1.tombs[0..s.l1.len],
        s.l2.keys[0..s.l2.len],
        s.l2.tombs[0..s.l2.len],
    });

    s.compact(f64);
    std.debug.print("{any}\t{any}\n{any}\t{any}\n{any}\t{any}\n\n", .{
        s.l0.keys[0..s.l0.len],
        s.l0.tombs[0..s.l0.len],
        s.l1.keys[0..s.l1.len],
        s.l1.tombs[0..s.l1.len],
        s.l2.keys[0..s.l2.len],
        s.l2.tombs[0..s.l2.len],
    });

    s.add(f64, 2, 2.0);
    s.del(f64, 4);
    std.debug.print("{any}\t{any}\n{any}\t{any}\n{any}\t{any}\n\n", .{
        s.l0.keys[0..s.l0.len],
        s.l0.tombs[0..s.l0.len],
        s.l1.keys[0..s.l1.len],
        s.l1.tombs[0..s.l1.len],
        s.l2.keys[0..s.l2.len],
        s.l2.tombs[0..s.l2.len],
    });

    s.compact(f64);
    std.debug.print("{any}\t{any}\n{any}\t{any}\n{any}\t{any}\n\n", .{
        s.l0.keys[0..s.l0.len],
        s.l0.tombs[0..s.l0.len],
        s.l1.keys[0..s.l1.len],
        s.l1.tombs[0..s.l1.len],
        s.l2.keys[0..s.l2.len],
        s.l2.tombs[0..s.l2.len],
    });
    for (1..6) |i| {
        std.debug.print("{}\n", .{s.has(i)});
    }
}
