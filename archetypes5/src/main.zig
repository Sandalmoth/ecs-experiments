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
        const Header = struct {
            head: usize,
            tail: usize,
            capacity: usize,
            _values: usize,
            next: ?*Page,
        };
        header: Header,
        bytes: [BlockPool.block_size - @sizeOf(Header)]u8,

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

test "queue edge cases" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var q = UntypedQueue.init(&pool);
    defer q.deinit();
    var a = std.ArrayList(u32).init(std.testing.allocator);
    defer a.deinit();

    for (0..10) |i| {
        a.clearRetainingCapacity();
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(u32, x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop(u32));
        }
        try std.testing.expectEqual(null, q.pop(u32));
    }

    for (0..10) |i| {
        a.clearRetainingCapacity();
        try q.ensureCapacity(u32, 4085);
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(u32, x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop(u32));
        }
        try std.testing.expectEqual(null, q.pop(u32));
        a.clearRetainingCapacity();
        for (0..4085) |j| {
            const x: u32 = @intCast(i * 1000_000 + j);
            try a.append(x);
            try q.push(u32, x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop(u32));
        }
        try std.testing.expectEqual(null, q.pop(u32));
    }
}

test "queue fuzz" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var q = UntypedQueue.init(&pool);
    defer q.deinit();
    var a = std.ArrayList(u32).init(std.testing.allocator);
    defer a.deinit();

    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..1000) |_| {
        a.clearRetainingCapacity();
        const y = rand.uintLessThan(usize, 10000);
        try q.ensureCapacity(u32, y / 2);
        for (0..y) |_| {
            const x = rand.int(u32);
            try a.append(x);
            try q.push(u32, x);
            try std.testing.expectEqual(a.items.len, q.count());
        }
        for (a.items) |x| {
            try std.testing.expectEqual(x, q.pop(u32));
        }
        try std.testing.expectEqual(null, q.pop(u32));
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

// we might want to not support multi-context setups
// (i.e. where worlds can be shared between different contexts)
// currently possible, so long as everything shares the same blockpool and keygen
// but it seems like a strange thing to allow?
// maybe we could use a context for simulation, and one for rendering? but it's definitely weird

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

            fn permissiveQueryInfo(raw: RawViewInfo) RawQueryInfo {
                return .{
                    .optional_read = raw.component_read,
                    .optional_read_write = raw.component_read_write,
                };
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

        pub fn View(comptime raw_view_info: RawViewInfo) type {
            return struct {
                const V = @This();
                const view_info = raw_view_info.reify();
                const permissive_query_info = raw_view_info.permissiveQueryInfo();

                _ecs: *Ctx,
                _world: *World,

                /// entity lookup can fail if the key is invalid (should nil be illegal?)
                pub fn entity(view: V, key: Key) ?EntityView(permissive_query_info) {
                    const location = view._world.bucketGet(key) orelse return null;
                    return .{
                        ._page = location.page,
                        .index = location.index,
                    };
                }

                /// compile error if queue is not in view info
                pub fn queue(
                    view: V,
                    comptime q: Queue,
                ) QueueView(view_info, q) {
                    comptime std.debug.assert(view_info.queue_write.contains(q) or
                        view_info.queue_read_write.contains(q));
                    return .{ ._queue = view._ecs.queues.getPtr(q) };
                }

                /// compile error if resource is not in view info
                pub fn resource(view: V, comptime r: Resource) ResourceType(r) {
                    comptime std.debug.assert(view_info.resource.contains(r));
                    return @field(view._ecs.resources, @tagName(r));
                }

                /// compile error if resource is not in view info
                pub fn resourcePtr(view: V, comptime r: Resource) *ResourceType(r) {
                    comptime std.debug.assert(view_info.resource.contains(r));
                    return &@field(view._ecs.resources, @tagName(r));
                }

                pub fn queueCreate(view: V, template: Template) !Key {
                    return view._world.queueCreate(template);
                }

                pub fn queueDestroy(view: V, key: Key) !void {
                    return view._world.queueDestroy(key);
                }

                pub fn queueInsert(
                    view: V,
                    key: Key,
                    comptime component: Component,
                    value: ComponentType(component),
                ) !void {
                    return view._world.queueInsert(key, component, value);
                }

                pub fn queueRemove(view: V, key: Key, comptime component: Component) !void {
                    return view._world.queueRemove(key, component);
                }

                pub fn pageIterator(
                    view: V,
                    comptime raw_query_info: RawQueryInfo,
                ) World.PageIterator(raw_query_info) {
                    const query_info = comptime raw_query_info.reify();
                    const query_reads = comptime query_info.include_read
                        .unionWith(query_info.optional_read);
                    const query_writes = comptime query_info.include_read_write
                        .unionWith(query_info.optional_read_write);
                    const view_reads = comptime view_info.component_read
                        .unionWith(view_info.component_read_write);
                    const view_writes = comptime view_info.component_read_write;
                    comptime std.debug.assert(query_reads.subsetOf(view_reads));
                    comptime std.debug.assert(query_writes.subsetOf(view_writes));
                    return view._world.pageIterator(raw_query_info);
                }
            };
        }

        fn PageView(comptime raw_query_info: RawQueryInfo) type {
            return struct {
                const PV = @This();
                const query_info = raw_query_info.reify();

                _page: *World.Page,

                pub fn entityIterator(view: PV) EntityIterator {
                    return .{ ._page = view._page, .cursor = 0 };
                }

                const EntityIterator = struct {
                    _page: *World.Page,
                    cursor: usize,

                    pub fn next(iterator: *EntityIterator) ?EntityView(raw_query_info) {
                        while (iterator.cursor < iterator._page.header.len) {
                            const index = iterator.cursor;
                            iterator.cursor += 1;
                            return .{ ._page = iterator._page, .index = index };
                        }
                        return null;
                    }
                };
            };
        }

        pub fn EntityView(comptime raw_query_info: RawQueryInfo) type {
            return struct {
                const EV = @This();
                const query_info = raw_query_info.reify();

                _page: *World.Page,
                index: usize,

                pub fn get(view: EV, comptime c: Component) ComponentType(c) {
                    comptime std.debug.assert(query_info.include_read.contains(c) or
                        query_info.include_read_write.contains(c));
                    return view._page.get(c, view.index);
                }

                pub fn getPtr(view: EV, comptime c: Component) *ComponentType(c) {
                    comptime std.debug.assert(query_info.include_read_write.contains(c));
                    return view._page.getPtr(c, view.index);
                }

                pub fn getOptional(view: EV, comptime c: Component) ?ComponentType(c) {
                    comptime std.debug.assert(query_info.include_read.contains(c) or
                        query_info.include_read_write.contains(c) or
                        query_info.optional_read.contains(c) or
                        query_info.optional_read_write.contains(c));
                    return view._page.getOptional(c, view.index);
                }

                pub fn getOptionalPtr(view: EV, comptime c: Component) ?*ComponentType(c) {
                    comptime std.debug.assert(query_info.include_read_write.contains(c) or
                        query_info.optional_read_write.contains(c));
                    return view._page.getOptional(c, view.index);
                }

                pub fn template(view: EV) Template {
                    const reads = comptime query_info.include_read
                        .unionWith(query_info.include_read_write)
                        .unionWith(query_info.optional_read)
                        .unionWith(query_info.optional_read_write);
                    var t = Template{};
                    inline for (0..n_components) |i| {
                        const c: Component = @enumFromInt(i);
                        if (!reads.contains(c)) continue;
                        @field(t, @tagName(c)) = view.getOptional(c);
                    }
                    return t;
                }

                pub fn key(view: EV) Key {
                    return view._page.header.keys[view.index];
                }
            };
        }

        pub fn QueueView(comptime raw_view_info: RawViewInfo, comptime q: Queue) type {
            return struct {
                const T = QueueType(q);
                const QV = @This();
                const view_info = raw_view_info.reify();

                _queue: *UntypedQueue,

                // --- write only ---
                pub fn push(view: QV, value: T) !void {
                    comptime std.debug.assert(view_info.queue_write.contains(q) or
                        view_info.queue_read_write.contains(q));
                    try view._queue.push(T, value);
                }
                pub fn ensureCapacity(view: QV, n: usize) !void {
                    comptime std.debug.assert(view_info.queue_write.contains(q) or
                        view_info.queue_read_write.contains(q));
                    try view._queue.ensureCapacity(T, n);
                }
                pub fn pushAssumeCapacity(view: QV, value: T) void {
                    comptime std.debug.assert(view_info.queue_write.contains(q) or
                        view_info.queue_read_write.contains(q));
                    view._queue.pushAssumeCapacity(T, value);
                }

                // --- read write ---
                pub fn pop(view: QV) ?T {
                    comptime std.debug.assert(view_info.queue_read_write.contains(q));
                    return view._queue.pop(T);
                }
                pub fn peek(view: QV) ?T {
                    comptime std.debug.assert(view_info.queue_read_write.contains(q));
                    return view._queue.peek(T);
                }
                pub fn reset(view: QV) void {
                    comptime std.debug.assert(view_info.queue_read_write.contains(q));
                    view._queue.reset();
                }
                pub fn count(view: QV) usize {
                    comptime std.debug.assert(view_info.queue_read_write.contains(q));
                    view._queue.count();
                }
            };
        }

        pub const World = struct {
            const cache_size = 64;

            const Page = struct {
                // pages hold the actual entity data, with one page per archetype
                const Header = struct {
                    keys: [*]Key,
                    components: [n_components]usize,
                    capacity: usize,
                    len: usize,
                };
                header: Header,
                data: [BlockPool.block_size - @sizeOf(Header)]u8,

                fn create(pool: *BlockPool, set: ComponentSet) !*Page {
                    const page = try pool.create(Page);
                    page.header.capacity = 0;
                    page.header.len = 0;

                    var sz: usize = @sizeOf(usize);
                    inline for (0..n_components) |i| {
                        const c: Component = @enumFromInt(i);
                        if (set.contains(c)) {
                            sz += @sizeOf(ComponentType(c));
                        }
                    }

                    page.header.capacity = page.data.len / sz;
                    while (true) {
                        var ptr = @intFromPtr(&page.data[0]);
                        ptr = std.mem.alignForward(usize, ptr, @alignOf(Key));
                        page.header.keys = @ptrFromInt(ptr);
                        ptr += @sizeOf(Key) * page.header.capacity;
                        inline for (0..n_components) |i| {
                            const c: Component = @enumFromInt(i);
                            if (set.contains(c)) {
                                const C = ComponentType(c);
                                ptr = std.mem.alignForward(usize, ptr, @alignOf(C));
                                page.header.components[i] = ptr;
                                ptr += @sizeOf(C) * page.header.capacity;
                            } else {
                                page.header.components[i] = 0;
                            }
                        }
                        if (ptr <= @intFromPtr(&page.data[0]) + page.data.len) break;
                        page.header.capacity -= 1;
                        std.debug.print("overestimate for archetype {}\n", .{set});
                    }

                    return page;
                }

                fn append(page: *Page, key: Key, template: Template) usize {
                    std.debug.assert(page.header.len < page.header.capacity);
                    page.header.keys[page.header.len] = key;
                    inline for (std.meta.fields(Template), 0..) |field, i| {
                        if (@field(template, field.name) != null) {
                            const c: Component = @enumFromInt(i);
                            page.component(c)[page.header.len] = @field(template, field.name).?;
                        }
                    }
                    const index = page.header.len;
                    page.header.len += 1;
                    return index;
                }

                /// returns the key to the entity that was relocated (or nil if no relocation)
                fn erase(page: *Page, index: usize) Key {
                    const end = page.header.len - 1;
                    if (index == end) {
                        // easy special case with no swap
                        page.header.len -= 1;
                        return .nil;
                    }

                    const moved = page.header.keys[end];
                    page.header.keys[index] = page.header.keys[end];
                    inline for (page.header.components, 0..) |a, i| {
                        if (a != 0) {
                            const c: Component = @enumFromInt(i);
                            const data = page.component(c);
                            data[index] = data[end];
                        }
                    }
                    page.header.len -= 1;
                    return moved;
                }

                fn componentSet(page: Page) ComponentSet {
                    var set = ComponentSet.initEmpty();
                    for (page.header.components, 0..) |a, i| {
                        if (a != 0) set.insert(@as(Component, @enumFromInt(i)));
                    }
                    return set;
                }

                fn hasComponent(page: Page, c: Component) bool {
                    return page.header.components[@intFromEnum(c)] != 0;
                }

                fn component(page: *Page, comptime c: Component) [*]ComponentType(c) {
                    const a = page.header.components[@intFromEnum(c)];
                    std.debug.assert(a != 0);
                    return @ptrFromInt(a);
                }

                fn get(page: *Page, comptime c: Component, ix: usize) ComponentType(c) {
                    return page.component(c)[ix];
                }

                fn getPtr(page: *Page, comptime c: Component, ix: usize) *ComponentType(c) {
                    return &page.component(c)[ix];
                }

                fn getOptional(page: *Page, comptime c: Component, ix: usize) ?ComponentType(c) {
                    if (page.header.components[@intFromEnum(c)] == 0) return null;
                    return page.component(c)[ix];
                }

                fn getOptionalPtr(
                    page: *Page,
                    comptime c: Component,
                    ix: usize,
                ) *?ComponentType(c) {
                    if (page.header.components[@intFromEnum(c)] == 0) return null;
                    return &page.component(c)[ix];
                }
            };
            const PageInfo = struct { page: *Page, set: ComponentSet };

            const Bucket = struct {
                // buckets hold a lookup table from an entity to it's location in a page
                const capacity = std.math.floorPowerOfTwo(
                    usize,
                    (BlockPool.block_size - @sizeOf(usize)) /
                        (@sizeOf(?*Page) + @sizeOf(usize) + @sizeOf(u8)),
                ); // power of two for speed (TODO benchmark prime size for better memory use)

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

                // we should trigger split if bucket is full
                fn full(bucket: Bucket) bool {
                    return bucket.len * 9 > capacity * 8; // TODO benchmark percentage
                }

                // we should trigger merge if both buckets are empty
                fn empty(bucket: Bucket) bool {
                    return bucket.len * 9 < capacity; // TODO benchmark percentage
                }

                fn insert(bucket: *Bucket, key: Key, page: *Page, index: usize) bool {
                    std.debug.assert(bucket.len < capacity);
                    const fingerprint = key.fingerprint();
                    var ix = key.bucketSlot(Bucket.capacity);
                    while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                        if (bucket.fingerprints[ix] == fingerprint) {
                            const k = bucket.pages[ix].?.header.keys[bucket.indices[ix]];
                            if (k == key) return false;
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
                    var ix = key.bucketSlot(Bucket.capacity);
                    while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                        if (bucket.fingerprints[ix] == fingerprint) {
                            const k = bucket.pages[ix].?.header.keys[bucket.indices[ix]];
                            if (k != key) continue;
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
                                const key_shift = page_shift.header.keys[bucket.indices[ix_shift]];
                                const key_dist =
                                    (ix_shift -% key_shift.bucketSlot(Bucket.capacity)) % capacity;
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
                    var ix = key.bucketSlot(Bucket.capacity);
                    while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                        if (bucket.fingerprints[ix] == fingerprint) {
                            const k = bucket.pages[ix].?.header.keys[bucket.indices[ix]];
                            if (k == key) {
                                bucket.pages[ix] = page;
                                bucket.indices[ix] = index;
                                bucket.fingerprints[ix] = fingerprint;
                                return true;
                            }
                        }
                    }
                    return false;
                }

                const Location = struct {
                    page: *Page,
                    index: usize,

                    pub fn template(loc: Location) Template {
                        var t = Template{};
                        inline for (0..n_components) |i| {
                            const c: Component = @enumFromInt(i);
                            @field(t, @tagName(c)) = loc.page.getOptional(c, loc.index);
                        }
                        return t;
                    }
                };
                fn get(bucket: Bucket, key: Key) ?Location {
                    const fingerprint = key.fingerprint();
                    var ix = key.bucketSlot(Bucket.capacity);
                    while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                        if (bucket.fingerprints[ix] == fingerprint) {
                            const k = bucket.pages[ix].?.header.keys[bucket.indices[ix]];
                            if (k == key) return .{
                                .page = bucket.pages[ix].?,
                                .index = bucket.indices[ix],
                            };
                        }
                    }
                    return null;
                }
            };
            const BucketInfo = struct { bucket: *Bucket, depth: usize };

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

            pages: std.MultiArrayList(PageInfo), // first cache_size slots form fifo cache
            buckets: std.MultiArrayList(BucketInfo), // extendible hashing
            depth: usize,
            cache_counter: usize,

            create_queue: UntypedQueue,
            destroy_queue: UntypedQueue,
            insert_queues: std.EnumArray(Component, UntypedQueue),
            remove_queues: std.EnumArray(Component, UntypedQueue),

            /// create a new world
            pub fn create(pool: *BlockPool, keygen: *KeyGenerator) !*World {
                const world = try pool.alloc.create(World);
                world.pool = pool;
                world.keygen = keygen;
                world.pages = std.MultiArrayList(PageInfo){};
                world.buckets = std.MultiArrayList(BucketInfo){};
                world.depth = 0;
                world.cache_counter = 0;
                const empty_queue = UntypedQueue.init(pool); // doesn't alloc, so copy replicates
                world.create_queue = empty_queue;
                world.destroy_queue = empty_queue;
                world.insert_queues = std.EnumArray(Component, UntypedQueue).initFill(empty_queue);
                world.remove_queues = std.EnumArray(Component, UntypedQueue).initFill(empty_queue);
                return world;
            }

            /// destroy the world
            pub fn destroy(world: *World) void {
                world.pages.deinit(world.pool.alloc);
                world.buckets.deinit(world.pool.alloc);
                world.create_queue.deinit();
                world.destroy_queue.deinit();
                var it_insert = world.insert_queues.iterator();
                while (it_insert.next()) |kv| kv.value.deinit();
                var it_remove = world.remove_queues.iterator();
                while (it_remove.next()) |kv| kv.value.deinit();
                world.pool.alloc.destroy(world);
            }

            /// entity lookup can fail if the key is invalid (should nil be illegal?)
            pub fn entity(world: *World, key: Key) ?EntityView(.read_write_any) {
                const location = world.bucketGet(key) orelse return null;
                return .{
                    ._page = location.page,
                    .index = location.index,
                };
            }

            pub fn pageIterator(
                world: *World,
                comptime raw_query_info: RawQueryInfo,
            ) PageIterator(raw_query_info) {
                return .{
                    ._world = world,
                    .cursor = 0,
                };
            }

            pub fn PageIterator(comptime raw_query_info: RawQueryInfo) type {
                return struct {
                    const PI = @This();
                    const query_info = raw_query_info.reify();
                    _world: *World,
                    cursor: usize,

                    pub fn next(it: *PI) ?PageView(raw_query_info) {
                        const includes = comptime query_info.include_read
                            .unionWith(query_info.include_read_write);
                        const excludes = comptime query_info.exclude;
                        while (it.cursor < it._world.pages.len) {
                            const page = it._world.pages.items(.page)[it.cursor];
                            const set = it._world.pages.items(.set)[it.cursor];
                            it.cursor += 1;
                            if (includes.subsetOf(set) and
                                excludes.intersectWith(set).count() == 0)
                            {
                                return .{ ._page = page };
                            }
                        }
                        return null;
                    }
                };
            }

            /// queue an entity to be created when World.resolveQueues is called
            /// returns the key that entity will have
            pub fn queueCreate(world: *World, template: Template) !Key {
                const key = world.keygen.next();
                try world.create_queue.push(
                    CreateQueueEntry,
                    .{ .key = key, .template = template },
                );
                return key;
            }

            /// queue an entity to be destroyed when World.resolveQueues is called
            pub fn queueDestroy(world: *World, key: Key) !void {
                std.debug.assert(key != .nil);
                try world.destroy_queue.push(Key, key);
            }

            /// queue a component to be inserted into an entity when World.resolveQueues is called
            /// if the entity already has that component, nothing happens
            pub fn queueInsert(
                world: *World,
                key: Key,
                comptime component: Component,
                value: ComponentType(component),
            ) !void {
                std.debug.assert(key != .nil);
                try world.insert_queues.getPtr(component).push(
                    InsertQueueEntry(ComponentType(component)),
                    .{ .key = key, .value = value },
                );
            }

            /// queue a component to be removed from an entity when World.resolveQueues is called
            /// if the entity doesn't have that component, nothing happens
            pub fn queueRemove(world: *World, key: Key, comptime component: Component) !void {
                std.debug.assert(key != .nil);
                try world.remove_queues.getPtr(component).push(Key, key);
            }

            /// execute all the changes from the create/destroy/insert/remove queues
            /// order is create -> destroy -> (per component insert -> remove)
            /// hence, creating and then immediately destroying before resolve is allowed
            pub fn resolveQueues(world: *World) !void {
                try world.resolveCreateQueue();
                world.resolveDestroyQueue();

                inline for (0..n_components) |i| {
                    const c: Component = @enumFromInt(i);
                    const C = ComponentType(c);
                    const insert_queue = world.insert_queues.getPtr(c);
                    const remove_queue = world.remove_queues.getPtr(c);

                    while (true) {
                        const q = insert_queue.peek(InsertQueueEntry(C)) orelse break;
                        const location = world.bucketGet(q.key) orelse continue;
                        if (location.page.hasComponent(c)) continue;
                        var set = location.page.componentSet();
                        set.insert(c);
                        const page = try world.getPage(set);
                        var template = location.template();
                        @field(template, @tagName(c)) = q.value;
                        const index = page.append(q.key, template);
                        world.bucketUpdate(q.key, page, index);
                        const moved = location.page.erase(location.index);
                        if (moved != .nil) world.bucketUpdate(moved, location.page, location.index);
                        _ = insert_queue.pop(InsertQueueEntry(C));
                    }

                    while (true) {
                        const k = remove_queue.peek(Key) orelse break;
                        const location = world.bucketGet(k) orelse continue;
                        if (!location.page.hasComponent(c)) continue;
                        var set = location.page.componentSet();
                        set.remove(c);
                        const page = try world.getPage(set);
                        var template = location.template();
                        @field(template, @tagName(c)) = null;
                        const index = page.append(k, template);
                        world.bucketUpdate(k, page, index);
                        const moved = location.page.erase(location.index);
                        if (moved != .nil) world.bucketUpdate(moved, location.page, location.index);
                        _ = remove_queue.pop(Key);
                    }
                }

                try world.bucketCompact();
            }

            fn resolveCreateQueue(world: *World) !void {
                while (true) {
                    // peek first since if bucketEnsure or getPage fails we want to be able to retry
                    const q = world.create_queue.peek(CreateQueueEntry) orelse break;
                    try world.bucketEnsure(q.key);
                    var set = ComponentSet.initEmpty();
                    inline for (std.meta.fields(Template), 0..) |field, i| {
                        if (@field(q.template, field.name) != null) set.insert(
                            @as(Component, @enumFromInt(i)),
                        );
                    }
                    const page = try world.getPage(set);
                    const index = page.append(q.key, q.template);
                    world.bucketInsert(q.key, page, index);
                    _ = world.create_queue.pop(CreateQueueEntry);
                }
            }

            fn resolveDestroyQueue(world: *World) void {
                while (world.destroy_queue.pop(Key)) |k| {
                    // NOTE trying to destroy an entity twice is legal (simpler that way imo)
                    const location = world.bucketGet(k) orelse continue;
                    world.bucketRemove(k);
                    const moved = location.page.erase(location.index);
                    if (moved != .nil) world.bucketUpdate(moved, location.page, location.index);
                }
            }

            /// find a page that has room for another entity with set components
            /// or create one if it does not exist
            fn getPage(world: *World, set: ComponentSet) !*Page {
                // TODO (extremely not acute) check assembly for this loop
                // i think we'd only want to read from pages if the set is equal
                // but the optimizer might catch that on it's own?
                for (world.pages.items(.set), world.pages.items(.page), 0..) |s, p, i| {
                    if (s.eql(set) and p.header.len < p.header.capacity) {
                        if (i >= cache_size) {
                            // not already in cache, swap with oldest
                            const pages = world.pages.items(.page);
                            const sets = world.pages.items(.set);
                            std.mem.swap(*Page, &pages[i], &pages[world.cache_counter]);
                            std.mem.swap(ComponentSet, &sets[i], &sets[world.cache_counter]);
                            world.cache_counter = (world.cache_counter + 1) % cache_size;
                        }
                        return p;
                    }
                }
                // no page that can hold the entity exists, create a new one
                try world.pages.ensureUnusedCapacity(world.pool.alloc, 1);
                const page = try Page.create(world.pool, set);
                world.pages.appendAssumeCapacity(.{ .page = page, .set = set });
                // and add to cache, since it's likely we'll need it soon
                const pages = world.pages.items(.page);
                const sets = world.pages.items(.set);
                std.mem.swap(*Page, &pages[world.pages.len - 1], &pages[world.cache_counter]);
                std.mem.swap(ComponentSet, &sets[world.pages.len - 1], &sets[world.cache_counter]);
                world.cache_counter = (world.cache_counter + 1) % cache_size;
                return page;
            }

            fn bucketGet(world: *World, key: Key) ?Bucket.Location {
                if (key == .nil) return null; // TODO consider
                if (world.buckets.len == 0) return null;
                const slot = key.indexSlot(world.depth);
                std.debug.assert(slot < world.buckets.len);
                const bucket = world.buckets.items(.bucket)[slot];
                return bucket.get(key);
            }

            /// make sure that there is space to insert the key
            fn bucketEnsure(world: *World, key: Key) !void {
                if (world.buckets.len == 0) {
                    try world.buckets.ensureUnusedCapacity(world.pool.alloc, 1);
                    const bucket = try Bucket.create(world.pool);
                    world.buckets.appendAssumeCapacity(.{ .bucket = bucket, .depth = 0 });
                    world.depth = 0;
                    return;
                }

                // loop ensures that we keep splitting for a degenerate hash distribution
                while (true) {
                    const slot = key.indexSlot(world.depth);
                    std.debug.assert(slot < world.buckets.len);
                    const bucket = world.buckets.items(.bucket)[slot];
                    if (!bucket.full()) return;

                    if (world.buckets.items(.depth)[slot] == world.depth) {
                        // expand the index
                        try world.buckets.ensureUnusedCapacity(world.pool.alloc, world.buckets.len);
                        const buckets = world.buckets.items(.bucket);
                        const depths = world.buckets.items(.depth);
                        for (0..buckets.len) |i| world.buckets.appendAssumeCapacity(
                            .{ .bucket = buckets[i], .depth = depths[i] },
                        );
                        world.depth += 1;
                    }

                    // split the bucket
                    const slot1 = key.indexSlot(world.depth - 1);
                    const slot2 = slot1 + world.buckets.len / 2;

                    const new_bucket_1 = try Bucket.create(world.pool);
                    errdefer world.pool.destroy(new_bucket_1);
                    const new_bucket_2 = try Bucket.create(world.pool);
                    errdefer world.pool.destroy(new_bucket_2);

                    const buckets = world.buckets.items(.bucket);
                    const depths = world.buckets.items(.depth);
                    buckets[slot1] = new_bucket_1;
                    depths[slot1] += 1;
                    buckets[slot2] = new_bucket_2;
                    depths[slot2] += 1;
                    for (0..Bucket.capacity) |i| {
                        const split_page = bucket.pages[i] orelse continue;
                        const split_key = split_page.header.keys[bucket.indices[i]];
                        const split_index = bucket.indices[i];
                        const split_slot = split_key.indexSlot(world.depth);
                        std.debug.assert(split_slot == slot1 or split_slot == slot2);
                        const success = buckets[split_slot].insert(split_key, split_page, split_index);
                        std.debug.assert(success);
                    }
                    world.pool.destroy(bucket);
                }
            }

            fn bucketCompact(world: *World) !void {
                // TODO
                // check each pair of buckets and merge if below threshold
                // should we shrink the indes?
                while (world.buckets.len > 1) {
                    const offset = world.buckets.len / 2;
                    var unsplit: usize = 0;
                    const buckets = world.buckets.items(.bucket);
                    const depths = world.buckets.items(.depth);
                    for (0..offset) |i| {
                        if (buckets[i] == buckets[i + offset]) {
                            unsplit += 1;
                            continue;
                        }
                        if (!buckets[i].empty() or !buckets[i + offset].empty()) continue;

                        const bucket = buckets[i + offset];
                        for (0..Bucket.capacity) |j| {
                            const page = bucket.pages[j] orelse continue;
                            const index = bucket.indices[j];
                            const key = page.header.keys[index];
                            _ = buckets[i].insert(key, page, index);
                        }
                        world.pool.destroy(bucket);
                        buckets[i + offset] = buckets[i];
                        depths[i] -= 1;
                        depths[i + offset] -= 1;

                        unsplit += 1;
                    }
                    if (unsplit < offset) return;
                    world.buckets.shrinkRetainingCapacity(offset);
                    world.depth -= 1;
                }

                if (world.buckets.len == 1 and world.buckets.items(.bucket)[0].len == 0) {
                    world.pool.destroy(world.buckets.items(.bucket)[0]);
                    world.buckets.clearRetainingCapacity();
                }
            }

            /// always call bucketEnsure before calling this
            fn bucketInsert(world: *World, key: Key, page: *Page, index: usize) void {
                const slot = key.indexSlot(world.depth);
                std.debug.assert(slot < world.buckets.len);
                const bucket = world.buckets.items(.bucket)[slot];
                const success = bucket.insert(key, page, index);
                std.debug.assert(success);
            }

            fn bucketUpdate(world: *World, key: Key, page: *Page, index: usize) void {
                const slot = key.indexSlot(world.depth);
                std.debug.assert(slot < world.buckets.len);
                const bucket = world.buckets.items(.bucket)[slot];
                const success = bucket.update(key, page, index);
                std.debug.assert(success);
            }

            fn bucketRemove(world: *World, key: Key) void {
                const slot = key.indexSlot(world.depth);
                std.debug.assert(slot < world.buckets.len);
                const bucket = world.buckets.items(.bucket)[slot];
                const success = bucket.remove(key);
                std.debug.assert(success);
            }
        };

        pool: *BlockPool,
        queues: std.EnumArray(Queue, UntypedQueue),
        resources: ResourceSpec,

        pub fn create(pool: *BlockPool) !*Ctx {
            const ctx = try pool.alloc.create(Ctx);
            ctx.pool = pool;
            for (0..n_queues) |i| {
                ctx.queues[i] = UntypedQueue.init(pool);
            }
            return ctx;
        }

        pub fn destroy(ctx: *Ctx) void {
            var it = ctx.queues.iterator();
            while (it.next()) |kv| kv.value.deinit();
            ctx.pool.alloc.destroy(ctx);
        }

        /// run a system in the current thread right now
        pub fn eval(ctx: *Ctx, world: *World, system: anytype) !void {
            // TODO validate system signature and produce good error messages
            const info = @typeInfo(@TypeOf(system));
            if (info.@"fn".return_type.? == void) {
                system(.{ ._ecs = ctx, ._world = world });
            } else {
                try system(.{ ._ecs = ctx, ._world = world });
            }
        }
    };
}

test "bucket index" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var keygen = KeyGenerator{};

    const Ctx = Context(
        struct { x: i32, y: f32 },
        struct {},
        struct {},
    );
    const World = Ctx.World;

    const w = try World.create(&pool, &keygen);
    defer w.destroy();

    var h1 = std.AutoHashMap(u64, i32).init(std.testing.allocator);
    defer h1.deinit();
    var h2 = std.AutoHashMap(u64, i32).init(std.testing.allocator);
    defer h2.deinit();

    for (0..10_000) |i| {
        const x: i32 = @intCast(i);
        const e = try w.queueCreate(.{ .x = x });
        if (i % 8 != 0) {
            try h1.put(@intFromEnum(e), x);
        } else {
            try h2.put(@intFromEnum(e), x);
        }
    }
    try w.resolveQueues();

    var it = h1.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*)) orelse unreachable;
        try std.testing.expectEqual(kv.value_ptr.*, e.getOptional(.x).?);
    }
    it = h2.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*)) orelse unreachable;
        try std.testing.expectEqual(kv.value_ptr.*, e.getOptional(.x).?);
    }

    it = h1.iterator();
    while (it.next()) |kv| try w.queueDestroy(@enumFromInt(kv.key_ptr.*));
    try w.resolveQueues();

    it = h1.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*));
        try std.testing.expectEqual(null, e);
    }
    it = h2.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*)) orelse unreachable;
        try std.testing.expectEqual(kv.value_ptr.*, e.getOptional(.x).?);
    }

    it = h2.iterator();
    while (it.next()) |kv| try w.queueDestroy(@enumFromInt(kv.key_ptr.*));
    try w.resolveQueues();

    it = h1.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*));
        try std.testing.expectEqual(null, e);
    }
    it = h2.iterator();
    while (it.next()) |kv| {
        const e = w.entity(@enumFromInt(kv.key_ptr.*));
        try std.testing.expectEqual(null, e);
    }
}

test "components" {
    var pool = BlockPool.init(std.testing.allocator);
    defer pool.deinit();
    var keygen = KeyGenerator{};

    const Ctx = Context(
        struct { x: i32, y: f32 },
        struct {},
        struct {},
    );
    const World = Ctx.World;

    const w = try World.create(&pool, &keygen);
    defer w.destroy();

    const e0 = try w.queueCreate(.{});
    const e1 = try w.queueCreate(.{ .x = 1 });
    const e2 = try w.queueCreate(.{ .y = 2.5 });
    const e3 = try w.queueCreate(.{ .x = 3, .y = 3.5 });
    try w.resolveQueues();

    try std.testing.expectEqual(null, w.entity(e0).?.getOptional(.x));
    try std.testing.expectEqual(null, w.entity(e0).?.getOptional(.y));
    try std.testing.expectEqual(1, w.entity(e1).?.getOptional(.x).?);
    try std.testing.expectEqual(null, w.entity(e1).?.getOptional(.y));
    try std.testing.expectEqual(null, w.entity(e2).?.getOptional(.x));
    try std.testing.expectEqual(2.5, w.entity(e2).?.getOptional(.y).?);
    try std.testing.expectEqual(3, w.entity(e3).?.getOptional(.x).?);
    try std.testing.expectEqual(3.5, w.entity(e3).?.getOptional(.y).?);

    try w.queueInsert(e0, .x, 99);
    try w.queueInsert(e0, .y, 99.5);
    try w.queueRemove(e1, .x);
    try w.queueInsert(e1, .y, 99.5);
    try w.queueRemove(e2, .y);
    try w.queueInsert(e2, .x, 99);
    try w.queueRemove(e3, .x);
    try w.queueRemove(e3, .y);
    try w.resolveQueues();

    try std.testing.expectEqual(99, w.entity(e0).?.getOptional(.x).?);
    try std.testing.expectEqual(99.5, w.entity(e0).?.getOptional(.y).?);
    try std.testing.expectEqual(null, w.entity(e1).?.getOptional(.x));
    try std.testing.expectEqual(99.5, w.entity(e1).?.getOptional(.y).?);
    try std.testing.expectEqual(99, w.entity(e2).?.getOptional(.x).?);
    try std.testing.expectEqual(null, w.entity(e2).?.getOptional(.y));
    try std.testing.expectEqual(null, w.entity(e3).?.getOptional(.x));
    try std.testing.expectEqual(null, w.entity(e3).?.getOptional(.y));

    try w.queueDestroy(e0);
    try w.queueDestroy(e1);
    try w.queueDestroy(e2);
    try w.queueDestroy(e3);
    try w.resolveQueues();

    try std.testing.expectEqual(null, w.entity(e0));
    try std.testing.expectEqual(null, w.entity(e1));
    try std.testing.expectEqual(null, w.entity(e2));
    try std.testing.expectEqual(null, w.entity(e3));
}

pub fn main() void {}
