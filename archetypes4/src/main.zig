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

/// either multiple threads writing to queue, or single thread reading/writing
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
    mutex: Mutex,

    fn init(pool: *BlockPool) UntypedQueue {
        return .{
            .pool = pool,
            .len = 0,
            .capacity = 0,
            .head = null,
            .tail = null,
            .mutex = Mutex{},
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

pub const Key = enum(u64) {
    nil = 0,
    _,

    fn fingerprint(key: Key) u8 {
        return @truncate(@intFromEnum(key) >> 28);
    }

    fn indexSlot(key: Key, depth: usize) usize {
        const mask = (@as(u64, 1) << @intCast(depth)) - 1;
        return @truncate(@intFromEnum(key) & mask);
    }

    fn bucketSlot(key: Key, comptime capacity: usize) usize {
        return (@intFromEnum(key) >> 36) % capacity;
    }
};

pub const KeyGenerator = struct {
    counter: u64 = 1,
    mutex: Mutex = Mutex{},

    fn next(keygen: *KeyGenerator) Key {
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
        const Self = @This();

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

        const CreateQueueEntry = struct {
            key: Key,
            template: Template,
        };
        fn InsertQueueEntry(comptime T: type) type {
            return struct {
                key: Key,
                value: T,
            };
        }

        const ComponentSet = std.StaticBitSet(n_components);
        const QueueSet = std.StaticBitSet(n_queues);

        const Bucket = struct {
            // buckets hold a lookup table from an entity to it's location in a page
            const capacity = std.math.floorPowerOfTwo(
                usize,
                (BlockPool.block_size - @sizeOf(usize)) /
                    (@sizeOf(?*Page) + @sizeOf(usize) + @sizeOf(u8)),
            ); // power of two for speed (TODO benchmark prime size)

            len: usize,
            pages: [capacity]?*Page,
            indices: [capacity]usize,
            fingerprints: [capacity]u8,

            fn create(pool: *BlockPool) !*Bucket {
                const bucket = try pool.create(Bucket);
                bucket.len = 0;
                bucket.pages = [_]?*Page{null} ** capacity;
                return bucket;
            }

            fn full(bucket: Bucket) bool {
                return bucket.len * 9 > capacity * 8; // TODO benchmark percentage
            }

            fn empty(bucket: Bucket) bool {
                return bucket.len * 9 < capacity; // TODO benchmark percentage
            }

            fn insert(bucket: *Bucket, key: Key, page: *Page, index: usize) bool {
                std.debug.assert(bucket.len < capacity);
                const fingerprint = key.fingerprint();
                var ix = key.bucketSlot();

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == key) return false;
                    }
                }

                bucket.pages[ix] = page;
                bucket.indices[ix] = index;
                bucket.fingerprints[ix] = fingerprint;
                bucket.len += 1;
                return true;
            }

            fn remove(bucket: *Bucket, key: Key) bool {
                const fingerprint = key.fingerprint();
                var ix = key.bucketSlot();

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e != key) continue;

                        // shuffle entries in bucket to preserve hashmap structure
                        var ix_remove = ix;
                        var ix_shift = ix_remove;
                        var dist: usize = 1;
                        while (true) {
                            ix_shift = (ix_shift + 1) % capacity;
                            const page_shift = bucket.pages[ix_shift] orelse {
                                bucket.pages[ix_remove] = null;
                                bucket.len -= 1;
                                return true;
                            };
                            const key_shift = page_shift.entities[bucket.indices[ix_shift]];
                            const key_dist = (ix_shift -% key_shift) % capacity;
                            if (key_dist >= dist) {
                                bucket.pages[ix_remove] = bucket.pages[ix_shift];
                                bucket.indices[ix_remove] = bucket.indices[ix_shift];
                                bucket.fingerprints[ix_remove] = bucket.fingerprints[ix_shift];
                                ix_remove = ix_shift;
                                dist = 1;
                            } else {
                                dist += 1;
                            }
                        }
                    }
                }

                return false;
            }

            fn update(bucket: *Bucket, key: Key, page: *Page, index: usize) bool {
                const fingerprint = key.fingerprint();
                var ix = key.bucketSlot();

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == key) {
                            bucket.pages[ix] = page;
                            bucket.indices[ix] = index;
                            bucket.fingerprints[ix] = fingerprint;
                            return true;
                        }
                    }
                }

                return false;
            }

            fn get(bucket: Bucket, key: Key) ?EntityView(.{}) {
                const fingerprint = key.fingerprint();
                var ix = key.bucketSlot();

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == key) return .{
                            .page = bucket.pages[ix].?,
                            .index = bucket.indices[ix],
                            .query = undefined,
                        };
                    }
                }

                return null;
            }
        };
        const BucketInfo = struct { bucket: *Bucket, depth: usize };

        const Page = struct {
            // pages hold the actual entity data, with one page per archetype
            const Head = struct {
                keys: [*]Key,
                components: [n_components]usize,
                capacity: usize,
                len: usize,
            };
            head: Head,
            data: [BlockPool.block_size - @sizeOf(Head)]u8,

            fn componentSet(page: Page) ComponentSet {
                var set = ComponentSet.initEmpty();
                for (page.components, 0..) |a, i| {
                    if (a != 0) set.set(i);
                }
                return set;
            }

            fn hasComponent(page: Page, component: Component) bool {
                return page.components[@intFromEnum(component)] != 0;
            }
        };
        const PageInfo = struct { page: *Page, set: ComponentSet };

        pub const Template = blk: {
            // generate a type that has all components as optionals
            var fields: [n_components]std.builtin.Type.StructField = undefined;
            @memcpy(&fields, std.meta.fields(ComponentSpec));
            for (0..fields.len) |i| {
                fields[i].default_value = &@as(?fields[i].type, null);
                fields[i].type = ?fields[i].type;
            }
            const info: std.builtin.Type = .{ .@"struct" = std.builtin.Type.Struct{
                .layout = .auto,
                .fields = &fields,
                .decls = &.{},
                .is_tuple = false,
            } };
            break :blk @Type(info);
            // it would be nice if it had a function that produced the componentset
            // but that's a lot of comptime-fu
        };

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

            const read_any = RawQueryInfo{
                .optinal_read = blk: {
                    var components: []const Component = &{};
                    for (0..n_components) |i| {
                        const component: Component = @enumFromInt(i);
                        components = components ++ component;
                    }
                    break :blk components;
                },
            };

            const read_write_any = RawQueryInfo{
                .optinal_read_write = blk: {
                    var components: []const Component = &{};
                    for (0..n_components) |i| {
                        const component: Component = @enumFromInt(i);
                        components = components ++ component;
                    }
                    break :blk components;
                },
            };

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
                world: *Self,
            };
        }

        fn PageView(comptime raw_info: RawQueryInfo) type {
            return struct {
                comptime info: QueryInfo = raw_info.reify(),
                page: *Page,
            };
        }

        fn EntityView(comptime raw_info: RawQueryInfo) type {
            return struct {
                comptime info: QueryInfo = raw_info.reify(),
                page: *Page,
                index: usize,
            };
        }

        pool: *BlockPool,
        keygen: *KeyGenerator,

        pages: std.MultiArrayList(PageInfo),
        buckets: std.MultiArrayList(BucketInfo),
        depth: usize, // extendible hashing

        create_queue: TypedQueue(CreateQueueEntry),
        destroy_queue: TypedQueue(Key),
        insert_queues: [n_components]UntypedQueue,
        remove_queues: [n_components]TypedQueue(Key),
        queues: [n_queues]UntypedQueue,

        pub fn init(pool: *BlockPool, keygen: *KeyGenerator) Self {
            var world = Self{
                .pool = pool,
                .keygen = keygen,
                .pages = std.MultiArrayList(PageInfo){},
                .buckets = std.MultiArrayList(BucketInfo){},
                .depth = 0,
                .create_queue = TypedQueue(CreateQueueEntry).init(pool),
                .destroy_queue = TypedQueue(Key).init(pool),
                .insert_queues = undefined,
                .remove_queues = undefined,
                .queues = undefined,
            };
            for (0..n_components) |i| {
                world.insert_queues[i] = UntypedQueue.init(pool);
                world.remove_queues[i] = TypedQueue(Key).init(pool);
            }
            for (0..n_queues) |i| {
                world.queues[i] = UntypedQueue.init(pool);
            }
            return world;
        }

        pub fn deinit(world: *Self) void {
            world.* = undefined;
        }

        pub fn queueCreate(world: *Self, template: Template) !Key {
            const key = world.keygen.next();
            try world.queueCreateKnown(key, template);
            return key;
        }

        fn queueCreateKnown(world: *Self, key: Key, template: Template) !void {
            std.debug.assert(key != .nil);
            try world.create_queue.push(.{ .key = key, .template = template });
        }

        pub fn queueDestroy(world: *Self, key: Key) !void {
            std.debug.assert(key != .nil);
            try world.destroy_queue.push(key);
        }

        pub fn queueInsert(
            world: *Self,
            key: Key,
            comptime component: Component,
            value: ComponentType(component),
        ) !void {
            std.debug.assert(key != .nil);
            try world.insert_queues[@intFromEnum(component)].push(
                InsertQueueEntry(ComponentType(component)),
                .{ .key = key, .value = value },
            );
        }

        pub fn queueRemove(world: *Self, key: Key, comptime component: Component) !void {
            std.debug.assert(key != .nil);
            try world.remove_queues[@intFromEnum(component)].push(key);
        }

        fn resolveCreateQueue(world: *Self) !void {
            while (true) {
                // peek first since if bucketEnsure or getPage fails we want to be able to retry
                const q = world.create_queue.peek() orelse break;
                try world.bucketEnsure(q.key);
                var set = ComponentSet.initEmpty();
                inline for (std.meta.fields(Template), 0..) |field, i| {
                    if (@field(q.template, field.name) != null) set.set(i);
                }
                const page = try world.getPage(set);
                const index = page.append(q.entity, q.template);
                world.bucketInsert(q.entity, page, index);
                _ = world.create_queue.pop();
            }
        }

        fn resolveDestroyQueue(world: *Self) void {
            while (world.destroy_queue.pop()) |k| {
                // NOTE trying to destroy an entity twice is legal (simpler that way imo)
                const view = world.bucketGet(k) orelse continue;
                world.bucketRemove(k);
                const moved = view.page.erase(k.index);
                if (moved != .nil) world.bucketUpdate(moved, view.page, view.index);
            }
        }

        pub fn resolveQueues(world: *Self) !void {
            // NOTE we could cancel out creates with immediate destroys
            // and save some processing in those cases, but it's unclear if that's worth it
            try world.resolveCreateQueue();
            world.resolveDestroyQueue();

            // inline for (0..n_components) |i| {
            //     const c: Component = @enumFromInt(i);
            //     const C = ComponentType(c);

            //     while (true) {
            //         const q = world.insert_queues[i].peek(InsertQueueEntry(C)) orelse break;
            //         const entity = world.get(q.entity) orelse continue;
            //         if (entity.page.hasComponent(c)) continue;
            //         var set = entity.page.componentSet();
            //         set.set(i);
            //         const page = try world.getPage(set);
            //         var template = entity.getTemplate();
            //         @field(template, @tagName(c)) = q.value;
            //         const index = page.append(q.entity, q.template);
            //         const moved = entity.page.erase(entity.index);
            //         world.updateLookup(q.entity, page, index);
            //         if (moved != 0) world.updateLookup(moved, entity.page, entity.index);
            //         _ = world.insert_queues[i].pop(InsertQueueEntry(C));
            //     }

            //     while (world.remove_queues[i].pop()) |e| {
            //         _ = e; // todo
            //     }

            //     // update components on entities by destroying and recreating at a batch level
            //     // alternative would be to update per modification which may be more efficent
            //     // as it causes less edits to the lookup table, and lower memory use overall
            //     // std.debug.assert(world.create_queue.empty());
            //     // std.debug.assert(world.destroy_queue.empty());
            //     // const c: Component = @enumFromInt(i);
            //     // const C = ComponentType(c);
            //     // while (world.insert_queues[i].pop(InsertQueueEntry(C))) |q| {
            //     //     const entity = world.lookup(q.entity) orelse continue;
            //     //     if (entity.page.components[i] != 0) {
            //     //         std.debug.print("entity has component, ignoring insert\n", .{});
            //     //         continue;
            //     //     }
            //     //     var template = entity.getTemplate();
            //     //     @field(template, @tagName(c)) = q.value;
            //     //     try world.queueCreate2(q.entity, template);
            //     //     try world.queueDestroy(q.entity);
            //     // }
            //     // world.processDestroyQueue();
            //     // try world.processCreateQueue();

            //     // std.debug.assert(world.create_queue.empty());
            //     // std.debug.assert(world.destroy_queue.empty());
            //     // while (world.remove_queues[i].pop()) |e| {
            //     //     const entity = world.lookup(e) orelse continue;
            //     //     if (entity.page.components[i] == 0) {
            //     //         std.debug.print("entity lacks component, ignoring remove\n", .{});
            //     //         continue;
            //     //     }
            //     //     var template = entity.getTemplate();
            //     //     @field(template, @tagName(c)) = null;
            //     //     try world.queueCreate2(e, template);
            //     //     try world.queueDestroy(e);
            //     // }
            //     // world.processDestroyQueue();
            //     // try world.processCreateQueue();
            // }

            // cleanup pages with no entities
            var ix: usize = 0;
            while (ix < world.pages.items.len) {
                if (world.pages.items[ix].len > 0) {
                    ix += 1;
                    continue;
                }
                const page = world.pages.items(.page)[ix];
                world.pages.swapRemove(ix);
                world.pool.destroy(page);
            }
        }

        fn getPage(world: *Self, set: ComponentSet) !*Page {
            _ = world;
            _ = set;
        }

        fn bucketEnsure(world: *Self, key: Key) !void {
            _ = world;
            _ = key;
        }

        /// always call bucketEnsure before calling this
        fn bucketInsert(world: *Self, key: Key, page: *Page, index: usize) void {
            _ = world;
            _ = key;
            _ = page;
            _ = index;
        }

        fn bucketUpdate(world: *Self, key: Key, page: *Page, index: usize) void {
            _ = world;
            _ = key;
            _ = page;
            _ = index;
        }

        fn bucketRemove(world: *Self, key: Key) void {
            _ = world;
            _ = key;
        }

        fn bucketGet(world: *Self, key: Key, comptime info: QueryInfo) EntityView(info) {
            _ = world;
            _ = key;
        }

        pub fn get(world: *Self, key: Key) ?EntityView(.read_write_any) {
            _ = world;
            _ = key;
        }
    };
}

test "world" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var kg = KeyGenerator{};

    const W = World(struct { x: u32, y: f64 }, struct {});
    var w = W.init(&pool, &kg);
    defer w.deinit();

    const e0 = try w.queueCreate(.{ .x = 1 });
    const e1 = try w.queueCreate(.{ .y = 2.5 });
    const e2 = try w.queueCreate(.{ .x = 3, .y = 3.5 });
    try w.resolveQueues();

    _ = e0;
    _ = e1;
    _ = e2;
}

// world
// page
// bucket
// worldview (reads/readwrites)
// pageiterator (includes/optionals/excludes)
// pageview (includes/optionals/excludes)
// entityiterator (includes/optionals/excludes)
// entityview (includes/optionals/excludes)
