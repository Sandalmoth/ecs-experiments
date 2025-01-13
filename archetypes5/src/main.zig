const builtin = @import("builtin");
const std = @import("std");

const single_threaded = builtin.single_threaded;

pub const BlockPool = @import("block_pool.zig").BlockPool;

// TODO FIXME there is some error in the queues, it can fail the test
/// either multiple threads writing to queue, or single thread reading/writing
const UntypedQueue = struct {
    // NOTE lock-free-ish possible?
    // think about locking structure?
    // basically, we should either have multiple simultaneous writers
    // or one single reader

    // what about iterators?
    // the ability to iterate over the values, in case we want to process the queue more than once
    // seems like it could be useful (though can be faked using count and pop->push)
    // a page iterator could be nice if we need to do multithreaded processing of a queue
    // however, it's unclear whether that'd actually be useful?

    // i think for now, push/pop functionality is enough
    // but it would be nicely consistent if both entities and queues have the same iterator vibe

    const Page = struct {
        const Head = struct {
            head: usize,
            tail: usize,
            capacity: usize,
            _values: usize,
            next: ?*Page,
        };
        header: Head,
        bytes: [BlockPool.block_size - @sizeOf(Head)]u8,

        fn create(pool: *BlockPool, comptime T: type) !*Page {
            const page = try pool.create(Page);
            page.header.head = 0;
            page.header.tail = 0;
            page.header.capacity = page.bytes.len / @sizeOf(T) - 1;
            page.header._values = std.mem.alignForward(usize, @intFromPtr(&page.bytes[0]), @alignOf(T));
            page.header.next = null;
            return page;
        }

        fn push(page: *Page, comptime T: type, value: T) void {
            std.debug.assert(page.header.tail < page.header.capacity);
            page.values(T)[page.header.tail] = value;
            page.header.tail += 1;
        }

        fn peek(page: *Page, comptime T: type) ?T {
            if (page.header.head == page.header.tail) return null;
            return page.values(T)[page.header.head];
        }

        fn pop(page: *Page, comptime T: type) ?T {
            if (page.header.head == page.header.tail) return null;
            const value = page.values(T)[page.header.head];
            page.header.head += 1;
            return value;
        }

        fn values(page: *Page, comptime T: type) [*]T {
            return @ptrFromInt(page.header._values);
        }

        fn full(page: *Page) bool {
            return page.header.tail == page.header.capacity;
        }
    };
    comptime {
        std.debug.assert(@sizeOf(Page) <= BlockPool.block_size);
    }

    // NOTE we have a singly linked list of pages
    // the page with the first value is at head, and is the actual head of the linked list
    // the page with the last value is at tail
    // end is the last page in the linked list
    // this setup ensures that we can ensure capacity beyond page-capacity limits

    pool: *BlockPool,
    len: usize,
    capacity: usize,
    head: ?*Page,
    tail: ?*Page,
    end: ?*Page,
    mutex: std.Thread.Mutex,

    fn init(pool: *BlockPool) UntypedQueue {
        return .{
            .pool = pool,
            .len = 0,
            .capacity = 0,
            .head = null,
            .tail = null,
            .end = null,
            .mutex = std.Thread.Mutex{},
        };
    }

    fn deinit(queue: *UntypedQueue) void {
        queue.mutex.lock();
        var walk = queue.head;
        while (walk) |page| {
            walk = page.header.next;
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

    /// protected by mutex
    fn ensureCapacity(queue: *UntypedQueue, comptime T: type, n: usize) !void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        try queue.ensureCapacityUnprotected(T, n);
    }
    fn ensureCapacityUnprotected(queue: *UntypedQueue, comptime T: type, n: usize) !void {
        if (queue.end == null) {
            std.debug.assert(queue.head == null);
            std.debug.assert(queue.tail == null);
            queue.end = try Page.create(queue.pool, T);
            queue.head = queue.end;
            queue.tail = queue.end;
            queue.capacity += queue.end.?.header.capacity;
        }

        while (queue.capacity < n) {
            const end = try Page.create(queue.pool, T);
            queue.end.?.header.next = end;
            queue.end = end;
            queue.capacity += queue.end.?.header.capacity;
        }
    }

    /// protected by mutex
    fn push(queue: *UntypedQueue, comptime T: type, value: T) !void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        try queue.ensureCapacityUnprotected(T, 1);
        if (queue.tail.?.full()) queue.tail = queue.tail.?.header.next;
        queue.tail.?.push(T, value);
        queue.len += 1;
        queue.capacity -= 1;
    }

    /// protected by mutex
    fn pushAssumeCapacity(queue: *UntypedQueue, comptime T: type, value: T) void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        std.debug.assert(queue.capacity >= 1);
        if (queue.tail.?.full()) queue.tail = queue.tail.?.header.next;
        queue.tail.?.push(T, value);
        queue.len += 1;
        queue.capacity -= 1;
    }

    fn peek(queue: *UntypedQueue, comptime T: type) ?T {
        const head = queue.head orelse return null;
        return head.peek(T);
    }

    fn pop(queue: *UntypedQueue, comptime T: type) ?T {
        const head = queue.head orelse return null;
        const value = head.pop(T) orelse return null;
        if (head.header.head == head.header.capacity) {
            queue.head = head.header.next;
            if (queue.head == null) {
                // if our head ran out of capacity, and it was also end, null out the tail/end
                queue.tail = null;
                queue.end = null;
            }
            queue.pool.destroy(head);
        }
        queue.len -= 1;
        return value;
    }

    fn count(queue: *UntypedQueue) usize {
        return queue.len;
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

        /// protected by mutex
        fn ensureCapacity(queue: *Self, n: usize) !void {
            try queue.untyped.ensureCapacity(T, n);
        }

        /// protected by mutex
        fn push(queue: *Self, value: T) !void {
            try queue.untyped.push(T, value);
        }

        /// protected by mutex
        fn pushAssumeCapacity(queue: *Self, value: T) void {
            queue.untyped.pushAssumeCapacity(T, value);
        }

        fn peek(queue: *Self) ?T {
            return queue.untyped.peek(T);
        }

        fn pop(queue: *Self) ?T {
            return queue.untyped.pop(T);
        }

        fn count(queue: *Self) usize {
            return queue.untyped.count();
        }
    };
}

test "queue edge cases" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var q = TypedQueue(u32).init(&pool);
    defer q.deinit();
    var a = std.ArrayList(u32).init(std.testing.allocator);
    defer a.deinit();

    for (0..10) |i| {
        a.clearRetainingCapacity();
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop());
        }
        try std.testing.expectEqual(null, q.pop());
    }

    for (0..10) |i| {
        a.clearRetainingCapacity();
        try q.ensureCapacity(4085);
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop());
        }
        try std.testing.expectEqual(null, q.pop());
        a.clearRetainingCapacity();
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop());
        }
        try std.testing.expectEqual(null, q.pop());
    }
}

test "queue fuzz" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var q = TypedQueue(u32).init(&pool);
    defer q.deinit();
    var a = std.ArrayList(u32).init(std.testing.allocator);
    defer a.deinit();

    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..1000) |_| {
        a.clearRetainingCapacity();
        const y = rand.uintLessThan(usize, 10000);
        try q.ensureCapacity(y / 2);
        for (0..y) |_| {
            const x = rand.int(u32);
            try a.append(x);
            try q.push(x);
            try std.testing.expectEqual(a.items.len, q.count());
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
    mutex: std.Thread.Mutex = std.Thread.Mutex{},

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

// ECS - struct templated on component, queue, and resource definitions
//       owns queues and definitions
// components - many reader or one read/write
// queues - many writer or one read/write
// resource - one read/write
// World - owns components
//         many worlds are possible
// View - used to access any component/queue/resource in the ECS

// is it awkward to not be able to just acccess components in a world?
// is there a scenario where we'd have multiple worlds, and want views without accessing the ecs?
// would requiring the use of a view with ecs acces in principle but forbidden via the info be bad?
// should we have worldview and ecsview as separate ideas?
// fn system(worldview)
// fn system(worldview, ecsview)
// fn system(ecsview)

// alternatively, we could let each world own also queues and resources
// but, this makes world splicing more awkward
// since the splicing should only encompass components, what are the rules
// for queues and resources when we splice/unsplice?
// presumably discard during splice into from the source
// and copy during slice out of to the target
// but it's ugly to have parts in the world that dissonate like that with the splicing purpose

// I think generating views from an ecs+world is good
// if a user really needs multi-ecs then that is possible
// but otherwise, all normal usages are possible by sequential application of ecs + world

pub fn Context(
    comptime ComponentSpec: type,
    comptime QueueSpec: type,
    comptime ResourceSpec: type,
) type {
    return struct {
        const Ctx = @This();

        pub const Component = std.meta.FieldEnum(ComponentSpec);
        const n_components = std.meta.fields(Component).len;
        fn ComponentType(comptime c: Component) type {
            return @FieldType(ComponentSpec, @tagName(c));
        }

        pub const Queue = std.meta.FieldEnum(QueueSpec);
        const n_queues = std.meta.fields(Queue).len;
        fn QueueType(comptime q: Queue) type {
            return @FieldType(QueueSpec, @tagName(q));
        }

        pub const Resource = std.meta.FieldEnum(ResourceSpec);
        const n_resources = std.meta.fields(Resource).len;
        fn ResourceType(comptime r: Resource) type {
            return @FieldType(ResourceSpec, @tagName(r));
        }

        const ComponentSet = std.EnumSet(Component);
        const QueueSet = std.EnumSet(Queue);
        const ResourceSet = std.EnumSet(Resource);

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
            // it would be nice if it had a function that produced a ComponentSet
            // but that's a lot of comptime-fu?
        };

        const RawViewInfo = struct {
            component_read: []const Component = &.{},
            component_read_write: []const Component = &.{},
            queue_write: []const Queue = &.{},
            queue_read_write: []const Queue = &.{},
            resource: []const Resource = &.{},

            pub const read_write_any = RawViewInfo{
                .component_read_write = blk: {
                    var components: []const Component = &.{};
                    for (0..n_components) |i| {
                        const component: Component = @enumFromInt(i);
                        components = components ++ .{component};
                    }
                    break :blk components;
                },
                .queue_read_write = blk: {
                    var queues: []const Queue = &.{};
                    for (0..n_queues) |i| {
                        const queue: Queue = @enumFromInt(i);
                        queues = queues ++ .{queue};
                    }
                    break :blk queues;
                },
                .resource = blk: {
                    var resources: []const Resource = &.{};
                    for (0..n_resources) |i| {
                        const resource: Resource = @enumFromInt(i);
                        resources = resources ++ .{resource};
                    }
                    break :blk resources;
                },
            };

            fn reify(raw: RawViewInfo) ViewInfo {
                var info = ViewInfo{
                    .component_read = ComponentSet.initEmpty(),
                    .component_read_write = ComponentSet.initEmpty(),
                    .queue_write = QueueSet.initEmpty(),
                    .queue_read_write = QueueSet.initEmpty(),
                    .resource = ResourceSet.initEmpty(),
                };
                for (raw.component_read) |c| info.component_read.insert(c);
                for (raw.component_read_write) |c| info.component_read_write.insert(c);
                std.debug.assert(
                    info.component_read.intersectWith(info.component_read_write).count() == 0,
                );
                for (raw.queue_write) |c| info.queue_write.insert(c);
                for (raw.queue_read_write) |c| info.queue_read_write.insert(c);
                std.debug.assert(
                    info.queue_write.intersectWith(info.queue_read_write).count() == 0,
                );
                for (raw.resource) |c| info.resource.insert(c);
                return info;
            }
        };
        const ViewInfo = struct {
            component_read: ComponentSet,
            component_read_write: ComponentSet,
            queue_write: QueueSet,
            queue_read_write: QueueSet,
            resource: ResourceSet,
        };

        pub const RawQueryInfo = struct {
            include_read: []const Component = &.{},
            include_read_write: []const Component = &.{},
            optional_read: []const Component = &.{},
            optional_read_write: []const Component = &.{},
            exclude: []const Component = &.{},

            pub const read_write_any = RawQueryInfo{
                .optional_read_write = blk: {
                    var components: []const Component = &.{};
                    for (0..n_components) |i| {
                        const component: Component = @enumFromInt(i);
                        components = components ++ .{component};
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
                for (raw.include_read) |c| result.include_read.insert(c);
                for (raw.include_read_write) |c| result.include_read_write.insert(c);
                for (raw.optional_read) |c| result.optional_read.insert(c);
                for (raw.optional_read_write) |c| result.optional_read_write.insert(c);
                for (raw.exclude) |c| result.exclude.insert(c);
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
        const QueryInfo = struct {
            include_read: ComponentSet,
            include_read_write: ComponentSet,
            optional_read: ComponentSet,
            optional_read_write: ComponentSet,
            exclude: ComponentSet,
        };

        fn View(comptime raw_view_info: RawViewInfo) type {
            return struct {
                const V = @This();
                const view_info = raw_view_info.reify();

                _ecs: *Ctx,
                _world: *World,

                /// entity lookup can fail if the key is invalid (should nil be illegal?)
                fn entity(view: V, key: Key) ?EntityView(.{}) { // TODO query from view
                    _ = view;
                    _ = key;
                }

                /// compile error queue is not in view info
                fn queue(view: V, comptime q: Queue) QueueView(.{}, QueueType(q)) { // TODO v->q
                    // todo compile error
                    _ = view;
                }

                fn resource(view: V, comptime r: Resource) ResourceType(r) {
                    comptime std.debug.assert(view_info.resource.contains(r));
                    return @field(view._ecs.resources, @tagName(r));
                }

                fn resourcePtr(view: V, comptime r: Resource) *ResourceType(r) {
                    comptime std.debug.assert(view_info.resource.contains(r));
                    return &@field(view._ecs.resources, @tagName(r));
                }

                pub fn pageIterator(
                    view: V,
                    comptime raw_query_info: RawQueryInfo,
                ) PageIterator(raw_query_info) {
                    _ = view;
                }

                fn PageIterator(comptime raw_query_info: RawQueryInfo) type {
                    return struct {
                        const PI = @This();
                        const query_info = raw_query_info.reify();
                        view: V,
                        cursor: usize,

                        pub fn next(iterator: *PI) ?PageView(raw_query_info) {
                            _ = iterator;
                            return null;
                        }
                    };
                }
            };
        }

        fn PageView(comptime raw_query_info: RawQueryInfo) type {
            return struct {
                const PV = @This();
                const query_info = raw_query_info.reify();
            };
        }

        fn EntityView(comptime raw_query_info: RawQueryInfo) type {
            return struct {
                const PV = @This();
                const query_info = raw_query_info.reify();
            };
        }

        fn QueueView(comptime raw_query_info: RawQueryInfo, comptime T: type) type {
            return struct {
                const QV = @This();
                const query_info = raw_query_info.reify();

                _queue: TypedQueue(T),

                // TODO expose functions
                // -- write only
                // push
                // ensureCapacity
                // pushAssumeCapacity
                // -- read write
                // pop
                // peek
                // reset
                // count
            };
        }

        const World = struct {
            const Page = struct {};
            const PageInfo = struct {};
            const Bucket = struct {};
            const BucketInfo = struct {};
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

            pool: *BlockPool,
            keygen: *KeyGenerator,

            pages: std.MultiArrayList(PageInfo),
            buckets: std.MultiArrayList(BucketInfo),
            depth: usize, // extendible hashing

            create_queue: TypedQueue(CreateQueueEntry),
            destroy_queue: TypedQueue(Key),
            insert_queues: std.EnumArray(UntypedQueue),
            remove_queues: std.EnumArray(TypedQueue(Key)),
        };

        pool: *BlockPool,
        queues: std.EnumArray(UntypedQueue),
        resources: ResourceSpec,

        fn create(pool: *BlockPool) !*Ctx {
            const ctx = try pool.alloc.create(Ctx);
            ctx.pool = pool;
            for (0..n_queues) |i| {
                ctx.queues[i] = UntypedQueue.init(pool);
            }
            return ctx;
        }

        fn destroy(ctx: *Ctx) void {
            ctx.pool.alloc.destroy(ctx);
        }
    };
}

pub fn main() void {}
