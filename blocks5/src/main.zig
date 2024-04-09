const std = @import("std");

const BLOCK_SIZE = 4 * 1024;
const BLOCK_ALIGN = 64;

const BUCKET_LOAD_MAX = 0.8;
const BUCKET_MERGE_MAX = 0.6;
const STORAGE_LOAD_MAX = 0.7;
const STORAGE_LOAD_MIN = 0.3;

const STORAGE_PRIME = 2654435741;
const BUCKET_PRIME = 2654435789;

const Entity = u64;
const nil: Entity = 0;

const BucketHeader = struct {
    next: ?*Bucket,
    len: usize,
};
const Bucket = struct {
    // const capacity = 997;
    const capacity = 11;
    const Location = packed struct {
        fingerprint: u8,
        page: u12,
        index: u12,
    };
    const ix_nil = std.math.maxInt(u12);

    const load_max = @as(comptime_int, @intFromFloat(
        BUCKET_LOAD_MAX * @as(comptime_float, capacity),
    ));
    const merge_max = @as(comptime_int, @intFromFloat(
        BUCKET_MERGE_MAX * @as(comptime_float, capacity),
    ));

    head: BucketHeader,
    locs: [capacity]Location,

    fn create(alloc: std.mem.Allocator) *Bucket {
        const bucket = alloc.create(Bucket) catch @panic("Bucket.create - out of memory");
        bucket.head = .{
            .next = null,
            .len = 0,
        };
        bucket.locs = .{Location{
            .fingerprint = undefined,
            .page = undefined,
            .index = ix_nil,
        }} ** capacity;
        return bucket;
    }

    fn destroy(bucket: *Bucket, alloc: std.mem.Allocator) void {
        if (bucket.head.next) |next| next.destroy(alloc);
        alloc.destroy(bucket);
    }

    fn set(
        bucket: *Bucket,
        alloc: std.mem.Allocator,
        page_index: *PageIndex,
        key: Entity,
        page: u12,
        index: u12,
    ) void {
        std.debug.assert(key != nil);

        if (bucket.head.len > load_max) {
            if (bucket.head.next == null) {
                bucket.head.next = Bucket.create(alloc);
            }
            return bucket.head.next.?.set(alloc, page_index, key, page, index);
        }

        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        var ix = h % capacity;
        while (bucket.locs[ix].index != ix_nil) : (ix = (ix + 1) % capacity) {
            const l = bucket.locs[ix];
            if (l.fingerprint == fingerprint) {
                const k = page_index.pages[l.page].?.head.keys[l.index];
                if (k == key) break;
            }
        }
        bucket.locs[ix] = .{
            .fingerprint = fingerprint,
            .page = page,
            .index = index,
        };
        page_index.pages[page].?.head.modified = true;
    }

    fn getLocPtr(bucket: *Bucket, page_index: *PageIndex, key: Entity) ?*Location {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        for (0..capacity) |probe| {
            const ix = (h + probe) % capacity;
            const l = bucket.locs[ix];
            if (l.index == ix_nil) break;
            if (l.fingerprint == fingerprint) {
                const k = page_index.pages[l.page].?.head.keys[l.index];
                if (k == key) return &bucket.locs[ix];
            }
        }

        if (bucket.head.next) |next| {
            return next.getLocPtr(page_index, key);
        } else {
            return null;
        }
    }

    fn has(bucket: *Bucket, page_index: *PageIndex, key: Entity) bool {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        for (0..capacity) |probe| {
            const ix = (h + probe) % capacity;
            const l = bucket.locs[ix];
            if (l.index == ix_nil) break;
            if (l.fingerprint == fingerprint) {
                const k = page_index.pages[l.page].?.head.keys[l.index];
                if (k == key) return true;
            }
        }

        if (bucket.head.next) |next| {
            return next.has(page_index, key);
        } else {
            return false;
        }
    }

    fn getPtr(bucket: *Bucket, comptime V: type, page_index: *PageIndex, key: Entity) ?*V {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        for (0..capacity) |probe| {
            const ix = (h + probe) % capacity;
            const l = bucket.locs[ix];
            if (l.index == ix_nil) break;
            if (l.fingerprint == fingerprint) {
                const k = page_index.pages[l.page].?.head.keys[l.index];
                if (k == key) {
                    page_index.pages[l.page].?.head.modified = true;
                    return &page_index.pages[l.page].?.head.vals(V)[l.index];
                }
            }
        }

        if (bucket.head.next) |next| {
            return next.getPtr(V, page_index, key);
        } else {
            return null;
        }
    }

    fn del(
        bucket: *Bucket,
        alloc: std.mem.Allocator,
        page_index: *PageIndex,
        n_pages: usize,
        key: Entity,
    ) void {
        const h = hash(key);
        const fingerprint: u8 = @intCast(h >> 24);
        for (0..capacity) |probe| {
            const ix = (h + probe) % capacity;
            const l = bucket.locs[ix];
            if (l.index == ix_nil) break;
            if (l.fingerprint == fingerprint) {
                const k = page_index.pages[l.page].?.head.keys[l.index];
                if (k != key) continue;

                // shuffle entries in bucket to preserve hashmap structure
                var ix_remove = ix;
                var ix_shift = ix_remove;
                var dist: usize = 1;
                while (true) {
                    ix_shift = (ix_shift + 1) % capacity;
                    const l_shift = bucket.locs[ix_shift];
                    if (l_shift.index == ix_nil) {
                        bucket.locs[ix_remove] = .{
                            .fingerprint = undefined,
                            .page = undefined,
                            .index = ix_nil,
                        };
                        return;
                    }
                    const k_shift = page_index.pages[l_shift.page].?.head.keys[l_shift.index];
                    const key_dist = (ix_shift -% hash(k_shift)) % capacity;
                    if (key_dist >= dist) {
                        bucket.locs[ix_remove] = bucket.locs[ix_shift];
                        ix_remove = ix_shift;
                        dist = 1;
                    } else {
                        dist += 1;
                    }
                }
            }
        }

        if (bucket.head.next) |next| {
            next.del(alloc, page_index, n_pages, key);
            if (next.head.len == 0) {
                bucket.head.next = next.head.next;
                next.destroy(alloc);
            } else if (bucket.head.len + next.head.len < merge_max) {
                std.debug.print("WANT TO MERGE BUCKET\n", .{});
                // TODO
            }
        }
    }

    fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(BUCKET_PRIME, std.mem.asBytes(&key));
    }

    fn debugPrint(bucket: Bucket, page_index: *PageIndex) void {
        std.debug.print(" [ ", .{});
        for (0..capacity) |i| {
            if (bucket.locs[i].index != ix_nil) {
                const l = bucket.locs[i];
                const k = page_index.pages[l.page].?.head.keys[l.index];
                // std.debug.print("{} ", .{k});
                std.debug.print("({},{})->{} ", .{ l.page, l.index, k });
            }
        }
        if (bucket.head.next) |next| {
            next.debugPrint(page_index);
            std.debug.print("] ->", .{});
        } else {
            std.debug.print("]\n", .{});
        }
    }
};

const BucketIndex = struct {
    buckets: [BLOCK_SIZE / @sizeOf(usize)]?*Bucket,
};

const PageHeader = struct {
    keys: [*]Entity,
    _vals: usize, // [*]V
    capacity: usize,
    len: usize,
    modified: bool,
    refcount: usize,

    fn vals(head: *PageHeader, comptime V: type) [*]V {
        return @ptrFromInt(head._vals);
    }
};
const Page = struct {
    head: PageHeader,
    bytes: [BLOCK_SIZE - 64]u8,

    pub fn create(comptime V: type, alloc: std.mem.Allocator) *Page {
        const page = alloc.create(Page) catch @panic("Page.create - out of memory");
        page.head = .{
            .keys = undefined,
            ._vals = undefined,
            // .capacity = page.bytes.len / (@sizeOf(Entity) + @sizeOf(V)),
            .capacity = 4,
            .len = 0,
            .modified = false,
            .refcount = 1,
        };

        // layout the keys and vals array in bytes
        while (page.head.capacity > 0) : (page.head.capacity -= 1) {
            var p: usize = @intFromPtr(&page.bytes[0]);
            page.head.keys = @ptrFromInt(p);
            p += @sizeOf(Entity) * page.head.capacity;
            p = std.mem.alignForward(usize, p, @alignOf(V));
            page.head._vals = p;
            p += @sizeOf(V) * page.head.capacity;

            if (p < @intFromPtr(&page.bytes[page.bytes.len - 1])) {
                break;
            }
        }
        if (page.head.capacity == 0) {
            @panic("Page.create - " ++ @typeName(V) ++ " cannot fit");
        }

        return page;
    }

    pub fn destroy(page: *Page, alloc: std.mem.Allocator) void {
        page.head.refcount -= 1;
        if (page.head.refcount == 0) {
            alloc.destroy(page);
        }
    }

    pub fn push(page: *Page, comptime V: type, key: Entity, val: V) usize {
        std.debug.assert(page.head.len < page.head.capacity);
        page.head.keys[page.head.len] = key;
        page.head.vals(V)[page.head.len] = val;
        page.head.modified = true;
        const result = page.head.len;
        page.head.len += 1;
        return result;
    }

    fn debugPrint(page: *Page, comptime V: type) void {
        std.debug.print(" [ ", .{});
        for (0..page.head.len) |i| {
            std.debug.print("{}:{} ", .{ page.head.keys[i], page.head.vals(V)[i] });
        }
        std.debug.print("] - {} item(s)\n", .{page.head.len});
    }
};

const PageIndex = struct {
    pages: [BLOCK_SIZE / @sizeOf(usize)]?*Page,
};

comptime {
    std.debug.assert(@sizeOf(Bucket) <= BLOCK_SIZE);
    std.debug.assert(@sizeOf(BucketIndex) <= BLOCK_SIZE);
    std.debug.assert(@sizeOf(Page) <= BLOCK_SIZE);
    std.debug.assert(@sizeOf(PageIndex) <= BLOCK_SIZE);
    std.debug.assert(@alignOf(Bucket) <= BLOCK_ALIGN);
    std.debug.assert(@alignOf(BucketIndex) <= BLOCK_ALIGN);
    std.debug.assert(@alignOf(Page) <= BLOCK_ALIGN);
    std.debug.assert(@alignOf(PageIndex) <= BLOCK_ALIGN);
}

const State = struct {
    alloc: std.mem.Allocator,
    len: usize,

    bucket_index: *BucketIndex,
    bucket_split: usize,
    bucket_round: usize,
    n_buckets: usize,

    page_index: *PageIndex,
    n_pages: usize,

    pub fn init(alloc: std.mem.Allocator) State {
        return .{
            .alloc = alloc,
            .len = 0,
            .bucket_index = alloc.create(BucketIndex) catch @panic("State.init - out of memory"),
            .bucket_split = 0,
            .bucket_round = 0,
            .n_buckets = 0,
            .page_index = alloc.create(PageIndex) catch @panic("State.init - out of memory"),
            .n_pages = 0,
        };
    }

    pub fn deinit(state: *State) void {
        for (0..state.n_buckets) |i| {
            state.bucket_index.buckets[i].?.destroy(state.alloc);
        }
        for (0..state.n_pages) |i| {
            state.page_index.pages[i].?.destroy(state.alloc);
        }
        state.alloc.destroy(state.bucket_index);
        state.alloc.destroy(state.page_index);
        state.* = undefined;
    }

    pub fn has(state: State, key: Entity) bool {
        if (state.n_buckets == 0) return false;
        if (key == nil) return false;
        const bucket_ix = state.bucketIndex(key);
        return state.bucket_index.buckets[bucket_ix].?.has(state.page_index, key);
    }

    pub fn getPtr(state: *State, comptime V: type, key: Entity) ?*V {
        if (state.n_buckets == 0) return null;
        if (key == nil) return null;
        const bucket_ix = state.bucketIndex(key);
        return state.bucket_index.buckets[bucket_ix].?.getPtr(V, state.page_index, key);
    }

    /// overwrites if present, inserts if not
    pub fn set(state: *State, comptime V: type, key: Entity, val: V) void {
        std.debug.assert(key != nil);
        if (state.bucketLoad() > STORAGE_LOAD_MAX) state.bucketExpand();
        if (state.pageFull()) state.pageExpand(V);

        const page = state.n_pages - 1;
        const index = state.page_index.pages[page].?.push(V, key, val);
        state.bucketSet(key, page, index);
        state.len += 1;
    }

    /// noop if not present
    pub fn del(state: *State, comptime V: type, key: Entity) void {
        std.debug.assert(key != nil);
        if (state.len == 0) return;
        if (state.bucketLoad() < STORAGE_LOAD_MIN) state.bucketShrink();
        if (state.pageEmpty()) state.pageShrink();

        const bucket_ix = state.bucketIndex(key);
        const loc = state.bucket_index.buckets[bucket_ix].?.getLocPtr(state.page_index, key).?;

        // first, replace our entry with the last entry on the last page
        const last_page = state.page_index.pages[state.n_pages - 1].?;
        std.debug.assert(last_page.head.len > 0);
        const last_key = last_page.head.keys[last_page.head.len - 1];
        const last_val = last_page.head.vals(V)[last_page.head.len - 1];

        const last_bucket_ix = state.bucketIndex(last_key);
        const last_loc = state.bucket_index.buckets[last_bucket_ix].?
            .getLocPtr(state.page_index, last_key).?;

        state.page_index.pages[last_loc.page].?.head.keys[last_loc.index] =
            state.page_index.pages[loc.page].?.head.keys[loc.index];
        state.page_index.pages[last_loc.page].?.head.vals(V)[last_loc.index] =
            state.page_index.pages[loc.page].?.head.vals(V)[loc.index];
        state.page_index.pages[loc.page].?.head.keys[loc.index] = last_key;
        state.page_index.pages[loc.page].?.head.vals(V)[loc.index] = last_val;
        const tmp_page = last_loc.page;
        const tmp_index = last_loc.index;
        last_loc.page = loc.page;
        last_loc.index = loc.index;
        loc.page = tmp_page;
        loc.index = tmp_index;

        state.page_index.pages[loc.page].?.head.modified = true;
        state.page_index.pages[last_loc.page].?.head.modified = true;

        // then we can delete in the hashmap
        state.bucket_index.buckets[bucket_ix].?.del(
            state.alloc,
            state.page_index,
            state.n_pages,
            key,
        );
        last_page.head.len -= 1;
        state.len -= 1;
    }

    fn bucketLoad(state: State) f64 {
        return if (state.n_buckets > 0)
            @as(f64, @floatFromInt(state.len)) /
                @as(f64, @floatFromInt(Bucket.capacity))
        else
            return 1.0;
    }

    fn bucketExpand(state: *State) void {
        const index = state.bucket_index;

        if (state.n_buckets == 0) {
            state.bucket_index.buckets[0] = Bucket.create(state.alloc);
            state.n_buckets += 1;
            return;
        }

        if (state.n_buckets == state.bucket_index.buckets.len) return;

        const splitting = index.buckets[state.bucket_split].?;
        index.buckets[state.bucket_split] = Bucket.create(state.alloc);
        index.buckets[state.n_buckets] = Bucket.create(state.alloc);
        state.bucket_split += 1;
        state.n_buckets += 1;

        for (splitting.locs) |loc| {
            if (loc.index == Bucket.ix_nil) continue;
            const key = state.page_index.pages[loc.page].?.head.keys[loc.index];
            const bucket_ix = state.bucketIndex(key);
            std.debug.assert(bucket_ix == state.bucket_split - 1 or
                bucket_ix == state.n_buckets - 1);
            index.buckets[bucket_ix].?.set(
                state.alloc,
                state.page_index,
                key,
                loc.page,
                loc.index,
            );
        }
        splitting.destroy(state.alloc);

        if (state.bucket_split == (@as(usize, 1) << @intCast(state.bucket_round))) {
            state.bucket_round += 1;
            state.bucket_split = 0;
        }
    }

    fn bucketShrink(state: *State) void {
        std.debug.print("WANT TO SHRINK BUCKET\n", .{});
        // TODO
        _ = state;
    }

    fn bucketSet(state: *State, key: Entity, page: usize, index: usize) void {
        const bucket_ix = state.bucketIndex(key);
        state.bucket_index.buckets[bucket_ix].?.set(
            state.alloc,
            state.page_index,
            key,
            @intCast(page),
            @intCast(index),
        );
    }

    fn hash(key: Entity) u32 {
        return std.hash.XxHash32.hash(STORAGE_PRIME, std.mem.asBytes(&key));
    }

    fn bucketIndex(state: State, key: Entity) usize {
        const h = hash(key);
        var loc = h & ((@as(usize, 1) << @intCast(state.bucket_round)) - 1);
        if (loc < state.bucket_split) { // i wonder if this branch predicts well
            loc = h & ((@as(usize, 1) << (@intCast(state.bucket_round + 1))) - 1);
        }
        return loc;
    }

    fn pageFull(state: State) bool {
        if (state.n_pages == 0) return true;
        const page = state.page_index.pages[state.n_pages - 1].?;
        std.debug.assert(page.head.len <= page.head.capacity);
        return page.head.len == page.head.capacity;
    }

    fn pageEmpty(state: State) bool {
        if (state.n_pages == 0) return false;
        const page = state.page_index.pages[state.n_pages - 1].?;
        return page.head.len == 0;
    }

    fn pageExpand(state: *State, comptime V: type) void {
        const index = state.page_index;
        if (state.n_pages == index.pages.len) @panic(
            "State.pageExpand - storage <" ++ @typeName(V) ++ "> is full",
        );

        index.pages[state.n_pages] = Page.create(V, state.alloc);
        state.n_pages += 1;
    }

    fn pageShrink(state: *State) void {
        std.debug.assert(state.n_pages > 0);
        state.page_index.pages[state.n_pages - 1].?.destroy(state.alloc);
        state.n_pages -= 1;
    }

    fn debugPrint(state: State, comptime V: type) void {
        std.debug.print("State - {} item(s)\n", .{state.len});
        for (0..state.n_buckets) |i| {
            std.debug.print(" bkt", .{});
            state.bucket_index.buckets[i].?.debugPrint(state.page_index);
        }
        for (0..state.n_pages) |i| {
            std.debug.print(" {:>3}", .{i});
            state.page_index.pages[i].?.debugPrint(V);
        }
    }
};

test "scratch" {
    std.debug.print("\n", .{});
    std.debug.print("{}\n", .{@sizeOf(State)});

    var s = State.init(std.testing.allocator);
    defer s.deinit();

    for (1..10) |i| {
        s.set(i64, i, @intCast(i * i));
    }
    s.debugPrint(i64);

    for (0..11) |i| {
        if (i < 1 or i >= 10) {
            try std.testing.expect(!s.has(i));
            try std.testing.expect(s.getPtr(i64, i) == null);
        } else {
            try std.testing.expect(s.has(i));
            const v: i64 = @intCast(i * i);
            try std.testing.expect(s.getPtr(i64, i).?.* == v);
        }
    }

    s.del(i64, 2);
    s.debugPrint(i64);
    s.del(i64, 4);
    s.debugPrint(i64);
    s.del(i64, 6);
    s.debugPrint(i64);
    s.del(i64, 8);
    s.debugPrint(i64);
}

pub fn main() void {}
