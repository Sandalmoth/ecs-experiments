const builtin = @import("builtin");
const std = @import("std");

const single_threaded = builtin.single_threaded;
const DummyMutex = struct {
    fn lock(_: *DummyMutex) void {}
    fn unlock(_: *DummyMutex) void {}
};
const Mutex = if (single_threaded) DummyMutex else std.Thread.Mutex;

pub const BlockPool = @import("block_pool.zig").BlockPool;

// TODO FIXME there is some error in the queues, it can fail the test
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
            insert_queues: [n_components]UntypedQueue,
            remove_queues: [n_components]TypedQueue(Key),
        };

        pool: *BlockPool,
        queues: [n_queues]UntypedQueue,
        resources: ResourceSpec,
    };
}

pub fn main() void {}
