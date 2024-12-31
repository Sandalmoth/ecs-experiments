const builtin = @import("builtin");
const std = @import("std");

const single_threaded = builtin.single_threaded;

const DummyMutex = struct {
    fn lock(_: *DummyMutex) void {}
    fn unlock(_: *DummyMutex) void {}
};

const Mutex = if (single_threaded) DummyMutex else std.Thread.Mutex;

const BlockPool = @import("block_pool.zig").BlockPool;

// const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
// comptime {
//     std.debug.assert(!builtin.single_threaded);
// }

const UntypedQueue = struct {
    // NOTE lock-free-ish possible?
    // think about locking structure?
    // basically, we should either have multiple simultaneous writers
    // or one single reader
    // (or should we allow multiple simultaneous readers too?)
    // but reading and writing should never be allowed at the same time

    // we could have some accessors/views that allow for just one or the other
    // and then stick the locking in there for writing only

    // also, maybe iterator shouldn't exist?
    // or we should have a pageiterator & valueiterator like with the entities?

    const Page = struct {
        head: usize,
        tail: usize,
        capacity: usize,
        _values: usize,
        next: ?*Page,
        bytes: [BlockPool.block_size - 64]u8,

        fn create(pool: *BlockPool, comptime T: type) !*Page {
            const page = try pool.create(Page);
            page.head = 0;
            page.tail = 0;
            page.capacity = page.bytes.len / @sizeOf(T) - 1;
            page._values = std.mem.alignForward(usize, @intFromPtr(&page.bytes[0]), @alignOf(T));
            page.next = null;
            return page;
        }

        fn push(page: *Page, comptime T: type, value: T) void {
            std.debug.assert(page.tail < page.capacity);
            page.values(T)[page.tail] = value;
            page.tail += 1;
        }

        fn peek(page: *Page, comptime T: type) ?T {
            if (page.head == page.tail) return null;
            return page.values(T)[page.head];
        }

        fn pop(page: *Page, comptime T: type) ?T {
            if (page.head == page.tail) return null;
            const value = page.values(T)[page.head];
            page.head += 1;
            return value;
        }

        fn values(page: *Page, comptime T: type) [*]T {
            return @ptrFromInt(page._values);
        }
    };
    comptime {
        std.debug.assert(@sizeOf(Page) <= BlockPool.block_size);
    }

    pool: *BlockPool,
    len: usize,
    capacity: usize,
    head: ?*Page,
    tail: ?*Page,
    mutex: std.Thread.Mutex,

    fn init(pool: *BlockPool) UntypedQueue {
        return .{
            .pool = pool,
            .len = 0,
            .capacity = 0,
            .head = null,
            .tail = null,
            .mutex = std.Thread.Mutex{},
        };
    }

    fn deinit(queue: *UntypedQueue) void {
        queue.mutex.lock();
        var walk = queue.head;
        while (walk) |page| {
            walk = page.next;
            queue.pool.destroy(page);
        }
        queue.* = undefined;
    }

    fn reset(queue: *UntypedQueue) void {
        queue.mutex.lock();
        const pool = queue.pool;
        queue.deinit();
        queue.* = UntypedQueue.init(pool);
    }

    fn empty(queue: *UntypedQueue) bool {
        const head = queue.head orelse return true;
        return head.head == head.tail;
    }

    fn ensureCapacity(queue: *UntypedQueue, comptime T: type, n: usize) !void {
        while (queue.capacity < n) {
            if (queue.tail == null) {
                queue.tail = try Page.create(queue.pool, T);
                queue.head = queue.tail;
            } else if (queue.tail.?.tail == queue.tail.?.capacity) {
                const tail = try Page.create(queue.pool, T);
                queue.tail.?.next = tail;
                queue.tail = tail;
            } else {
                @panic("???");
            }
            queue.capacity += queue.tail.?.capacity;
        }
    }

    fn push(queue: *UntypedQueue, comptime T: type, value: T) !void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        try queue.ensureCapacity(T, 1);
        queue.tail.?.push(T, value);
        queue.len += 1;
        queue.capacity -= 1;
    }

    fn pushAssumeCapacity(queue: *UntypedQueue, comptime T: type, value: T) void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        std.debug.assert(queue.capacity >= 1);
        queue.tail.?.push(T, value);
        queue.len += 1;
        queue.capacity -= 1;
    }

    fn peek(queue: *UntypedQueue, comptime T: type) ?T {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        const head = queue.head orelse return null;
        return head.peek(T);
    }

    fn pop(queue: *UntypedQueue, comptime T: type) ?T {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        const head = queue.head orelse return null;
        const value = head.pop(T) orelse return null;
        if (head.head == head.capacity) {
            queue.head = head.next;
            queue.pool.destroy(head);
        }
        queue.len -= 1;
        return value;
    }

    fn pageIterator(queue: *UntypedQueue, comptime T: type) PageIterator(T) {
        // TODO
        _ = queue;
    }

    fn PageIterator(comptime T: type) type {
        // TODO
        _ = T;
        return struct {
            const ValueIterator = struct {};
        };
    }

    fn iterator(queue: *UntypedQueue, comptime T: type) Iterator(T) {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        return .{
            .page = queue.head,
            .cursor = if (queue.head) |head| head.head else undefined,
        };
    }

    fn Iterator(comptime T: type) type {
        return struct {
            const Self = @This();

            page: ?*Page,
            cursor: usize,

            fn next(it: *Self) ?T {
                const page = it.page orelse return null;

                while (it.cursor < page.tail) {
                    const result = page.values(T)[it.cursor];
                    it.cursor += 1;
                    return result;
                }

                it.page = page.next;
                if (it.page != null) it.cursor = it.page.?.head;
                return it.next();
            }
        };
    }
};

fn TypedQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        untyped: UntypedQueue,

        fn init(pool: *BlockPool) Self {
            return .{ .untyped = UntypedQueue.init(pool) };
        }

        fn deinit(queue: *Self) void {
            queue.untyped.deinit();
        }

        fn reset(queue: *Self) void {
            queue.untyped.reset();
        }

        fn empty(queue: *Self) bool {
            return queue.untyped.empty();
        }

        fn ensureCapacity(queue: *Self, n: usize) !void {
            try queue.untyped.ensureCapacity(T, n);
        }

        fn push(queue: *Self, value: T) !void {
            try queue.untyped.push(T, value);
        }

        fn pushAssumeCapacity(queue: *Self, value: T) void {
            queue.untyped.pushAssumeCapacity(T, value);
        }

        fn peek(queue: *Self) ?T {
            return queue.untyped.peek(T);
        }

        fn pop(queue: *Self) ?T {
            return queue.untyped.pop(T);
        }

        fn iterator(queue: *Self) UntypedQueue.Iterator(T) {
            return queue.untyped.iterator(T);
        }
    };
}

test "queue" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var q = TypedQueue(u32).init(&pool);
    defer q.deinit();
    var a = std.ArrayList(u32).init(std.testing.allocator);
    defer a.deinit();

    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..100) |_| {
        a.clearRetainingCapacity();
        const y = rand.uintLessThan(usize, 1000);
        for (0..y) |_| {
            const x = rand.int(u32);
            try a.append(x);
            try q.push(x);
        }
        var it = q.iterator();
        for (a.items) |x| {
            try std.testing.expectEqual(x, it.next());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop());
        }
        try std.testing.expectEqual(null, q.pop());
    }
}

fn BlockList(comptime T: type) type {
    return struct {
        const Self = @This();

        const Page = struct {
            const value_capacity = (BlockPool.block_size - 64) / @sizeOf(T) - 1;
            const child_capacity = (BlockPool.block_size - 64) / @sizeOf(?*Page) - 1;
            len: usize,
            _values: usize,
            _children: usize,
            bytes: [BlockPool.block_size - 64]u8,

            fn create(pool: *BlockPool) !*Page {
                const page = try pool.create(Page);
                const base = @intFromPtr(&page.bytes[0]);
                page._values = std.mem.alignForward(usize, base, @alignOf(T));
                page._children = std.mem.alignForward(usize, base, @alignOf(?*Page));
                return page;
            }

            fn values(page: *Page) [*]T {
                return @ptrFromInt(page._values);
            }

            fn children(page: *Page) [*]?*Page {
                return @ptrFromInt(page._children);
            }
        };

        pool: *BlockPool,
        root: ?*Page,
        len: usize,
        capacity: usize,
        depth: usize,

        fn init(pool: BlockPool) Self {
            return .{
                .pool = pool,
                .root = null,
                .len = 0,
                .depth = 0,
            };
        }

        fn ensureCapacityImpl(list: *Self, n: usize, walk: *Page, depth: usize) !void {
            // recursive addition of new pages
            if (depth == 0) {
                list.capacity += Page.value_capacity;
            }
            while (list.capacity < n) {
                if (walk.len == Page.child_capacity) return;
                walk.children()[walk.len] = try Page.create(list.pool);
                walk.len += 1;
                ensureCapacityImpl(n, walk.children()[walk.len - 1].?, depth - 1);
            }
        }
        fn ensureCapacity(list: *Self, n: usize) !void {
            if (list.capacity >= n) return;
            if (list.root == null) {
                list.root = try Page.create(list.pool);
                std.debug.assert(list.depth == 0);
            } else if (list.depth == 0) {
                // we only have a leaf page, add a node above it
                const page = try Page.create(list.pool);
                page.children[0] = list.root;
                list.root = page;
                page.len = 1;
                list.depth += 1;
            }
            list.capacity += Page.value_capacity;

            while (true) {
                list.ensureCapacityImpl(n, list.root, list.depth);
                if (list.capacity >= n) return;
                // we need to expand at the root level to make room
                const page = try Page.create(list.pool);
                page.children[0] = list.root;
                list.root = page;
                page.len = 1;
                list.depth += 1;
            }
        }
    };
}

test "blocklist" {}

pub const Key = enum(u64) {
    nil = 0,
    _,

    fn fingerprint(key: Key) u8 {
        return @intFromEnum(key) >> 56;
    }

    fn indexSlot(key: Key, depth: usize) usize {
        const mask = (@as(u64, 1) << @intCast(depth)) - 1;
        return @truncate(@intFromEnum(key) & mask);
    }

    fn bucketSlot(key: Key, comptime capacity: usize) usize {
        return (@intFromEnum(key) >> 32) % capacity;
    }
};

pub const KeyGen = struct {
    counter: u64 = 1,
    mutex: Mutex,

    fn next(keygen: *KeyGen) Key {
        // xorshift* with 2^64 - 1 period (0 is fixed point, and also the null entity)
        keygen.mutex.lock();
        defer keygen.mutex.unlock();
        var x = keygen.counter;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        keygen.counter = x;
        return @enumFromInt(x *% 0x2545F4914F6CDD1D);
    }
};

pub fn World(comptime ComponentSpec: type, comptime QueueSpec: type) type {
    return struct {
        pub const Component = std.meta.FieldEnum(ComponentSpec);
        const n_components = std.meta.fields(Component).len;
        fn ComponentType(comptime c: Component) type {
            return @FieldType(ComponentSpec, @tagName(c));
        }

        pub const Queue = std.meta.FieldEnum(QueueSpec);
        const n_queues = std.meta.fields(Queue).len;
        fn QueueType(comptime c: Queue) type {
            return @FieldType(QueueSpec, @tagName(c));
        }

        const ComponentSet = std.StaticBitSet(n_components);
        const QueueSet = std.StaticBitSet(n_queues);

        const RawWorldViewInfo = struct {
            component_read: []const Component = &.{},
            component_read_write: []const Component = &.{},
            queue_write: []const Queue = &.{},
            queue_read_write: []const Queue = &.{},

            fn reify(raw: RawWorldViewInfo) WorldViewInfo {
                var result = WorldViewInfo{
                    .component_read = ComponentSet.initEmpty(),
                    .component_read_write = ComponentSet.initEmpty(),
                    .queue_write = QueueSet.initEmpty(),
                    .queue_read_write = QueueSet.initEmpty(),
                };
                for (raw.component_read) |c| result.component_read.set(@intFromEnum(c));
                for (raw.component_read_write) |c| result.component_read_write.set(@intFromEnum(c));
                std.debug.assert(
                    result.component_read.intersectWith(result.component_read_write).count() == 0,
                );
                for (raw.queue_write) |c| result.queue_write.set(@intFromEnum(c));
                for (raw.queue_read_write) |c| result.queue_read_write.set(@intFromEnum(c));
                std.debug.assert(result.queue_write.intersectWith(result.queue_read_write).count() == 0);
                return result;
            }
        };
        const WorldViewInfo = struct {
            component_read: ComponentSet,
            component_read_write: ComponentSet,
            queue_write: QueueSet,
            queue_read_write: QueueSet,
        };

        pub const RawQueryInfo = struct {
            include_read: []const Component = &.{},
            include_read_write: []const Component = &.{},
            optional_read: []const Component = &.{},
            optional_read_write: []const Component = &.{},
            exclude: []const Component = &.{},

            fn reify(raw: RawQueryInfo) QueryInfo {
                var result = QueryInfo{
                    .include_read = ComponentSet.initEmpty(),
                    .include_read_write = ComponentSet.initEmpty(),
                    .optional_read = ComponentSet.initEmpty(),
                    .optional_read_write = ComponentSet.initEmpty(),
                    .exclude = ComponentSet.initEmpty(),
                };
                for (raw.include_read) |c| result.include_read.set(@intFromEnum(c));
                for (raw.include_read_write) |c| result.include_read_write.set(@intFromEnum(c));
                for (raw.optional_read) |c| result.optional_read.set(@intFromEnum(c));
                for (raw.optional_read_write) |c| result.optional_read_write.set(@intFromEnum(c));
                for (raw.exclude) |c| result.exclude.set(@intFromEnum(c));
                // assert that there are no overlaps
                const all = result.include_read
                    .unionWith(result.include_read_write)
                    .unionWith(result.optional_read)
                    .unionWith(result.optional_read_write)
                    .unionWith(result.exclude);
                const total = result.include_read.count() +
                    result.include_read_write.count() +
                    result.optional_read.count() +
                    result.optional_read_write.count() +
                    result.exclude.count();
                std.debug.assert(all.count() == total);
                return result;
            }
        };
        pub const QueryInfo = struct {
            include_read: ComponentSet,
            include_read_write: ComponentSet,
            optional_read: ComponentSet,
            optional_read_write: ComponentSet,
            exclude: ComponentSet,
        };

        fn WorldView(comptime raw_info: RawWorldViewInfo) type {
            return struct {
                comptime info: WorldViewInfo = raw_info.reify(),
            };
        }

        const Bucket = struct {};
        const Page = struct {};

        pool: *BlockPool,
        keygen: *KeyGen,

        // // pages hold the actual entity data, with one page per archetype
        // pages: std.ArrayList(*Page),
        // archetypes: std.ArrayList(Archetype),

        // // buckets hold a lookup table from an entity to it's location in a page
        // depth: usize = 0,
        // buckets: std.ArrayList(*Bucket), // extendible hashing
        // bucket_depths: std.ArrayList(usize),

        // create_queue: TypedQueue(CreateQueueEntry),
        // destroy_queue: TypedQueue(Entity),
        // insert_queues: [n_components]UntypedQueue,
        // remove_queues: [n_components]TypedQueue(Entity),
        // queues: [n_queues]UntypedQueue,

        fn init(pool: *BlockPool, keygen: *KeyGen) World {
            return .{
                .pool = pool,
                .keygen = keygen,
            };
        }

        fn deinit(world: *World) void {
            world.* = undefined;
        }
    };
}

test "world" {
    const W = World(struct { x: u32 }, struct {});
    const WW = W.WorldView(.{ .component_read = &[_]W.Component{.x} });
    const ww = WW{};
    std.debug.print("{}\n{} {}\n", .{ WW, @sizeOf(WW), @alignOf(WW) });
    std.debug.print("{}\n", .{ww.info});
}

// world
// page
// bucket
// worldview (reads/readwrites)
// pageiterator (includes/optionals/excludes)
// pageview (includes/optionals/excludes)
// entityiterator (includes/optionals/excludes)
// entityview (includes/optionals/excludes)
