const builtin = @import("builtin");
const std = @import("std");

const BlockPool = @import("block_pool.zig").BlockPool;

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
comptime {
    std.debug.assert(!builtin.single_threaded);
}

// const TypeErasedQueue = struct {
//     // TODO multi-writer support
//     // TODO segmentedlist design s.t. it can live in an arena without wasting memory
//     // (if https://github.com/ziglang/zig/issues/20491 goes through we could use that?)
//     alloc: std.mem.Allocator,
//     len: usize,
//     capacity: usize,
//     bytes: ?[*]u8,

//     fn init(alloc: std.mem.Allocator) TypeErasedQueue {
//         return .{
//             .alloc = alloc,
//             .len = 0,
//             .capacity = 0,
//             .bytes = null,
//         };
//     }

//     fn append(queue: *TypeErasedQueue, comptime T: type, value: T) !void {
//         if (queue.bytes == null) {
//             const min_capacity = 16;
//             queue.bytes = (try queue.alloc.alloc(T, min_capacity)).ptr;
//             queue.capacity = min_capacity;
//         } else if (queue.len == queue.capacity) {
//             const new_capacity = queue.capacity * 2;
//             const new_bytes: [*]u8 = (try queue.alloc.alloc(T, new_capacity)).ptr;
//             @memcpy(new_bytes, queue.bytes.?[0..queue.len]);
//             queue.alloc.free(queue.bytes);
//         }
//         _ = queue;
//         _ = value;
//     }
// };

const UntypedQueue = struct {
    // NOTE lock-free-ish possible?
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
    head: ?*Page,
    tail: ?*Page,
    mutex: std.Thread.Mutex,

    fn init(pool: *BlockPool) UntypedQueue {
        return .{
            .pool = pool,
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

    fn push(queue: *UntypedQueue, comptime T: type, value: T) !void {
        queue.mutex.lock();
        defer queue.mutex.unlock();
        if (queue.tail == null) {
            queue.tail = try Page.create(queue.pool, T);
            queue.head = queue.tail;
        } else if (queue.tail.?.tail == queue.tail.?.capacity) {
            const tail = try Page.create(queue.pool, T);
            queue.tail.?.next = tail;
            queue.tail = tail;
        }
        queue.tail.?.push(T, value);
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
        const value = head.pop(T);
        if (head.head == head.capacity) {
            queue.head = head.next;
            queue.pool.destroy(head);
        }
        return value;
    }

    fn iterator(queue: *UntypedQueue, comptime T: type) Iterator(T) {
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

        fn push(queue: *Self, value: T) !void {
            return try queue.untyped.push(T, value);
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

pub fn ECS(comptime Components: type, comptime Queues: type) type {
    return struct {
        const Self = @This();

        const Component = std.meta.FieldEnum(Components);
        const n_components = std.meta.fields(Component).len;
        fn ComponentType(comptime c: Component) type {
            return @FieldType(Components, @tagName(c));
        }

        const Queue = std.meta.FieldEnum(Queues);
        const n_queues = std.meta.fields(Queue).len;
        fn QueueType(comptime c: Queue) type {
            return @FieldType(Queues, @tagName(c));
        }

        const Entity = u64;
        const Archetype = std.StaticBitSet(n_components);
        const System = *const fn (ScheduleView) void;

        const EntityTemplate = blk: {
            // take the Components type and generate a variant where all fields are optionals
            var fields: [n_components]std.builtin.Type.StructField = undefined;
            @memcpy(&fields, std.meta.fields(Components));
            for (0..fields.len) |i| {
                fields[i].default_value = &@as(?fields[i].type, null);
                // fields[i].type = @Type(.{ .optional = .{ .child = fields[i].type } });
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

        const Bucket = struct {
            const capacity = (BlockPool.block_size - 64) /
                (@sizeOf(usize) + @sizeOf(?*Page) + @sizeOf(usize) + @sizeOf(u8));

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
                return bucket.len * 7 > capacity * 6; // TODO benchmark
            }

            fn insert(bucket: *Bucket, entity: Entity, page: *Page, index: usize) bool {
                std.debug.assert(bucket.len < capacity);

                const fingerprint: u8 = @intCast(entity >> 56);
                var ix = (entity >> 32) % capacity;

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = page.entities[bucket.indices[ix]];
                        if (e == entity) return false;
                    }
                }

                bucket.pages[ix] = page;
                bucket.indices[ix] = index;
                bucket.fingerprints[ix] = fingerprint;
                bucket.len += 1;
                return true;
            }
        };

        const Page = struct {
            entities: [*]Entity,
            components: [n_components]usize,
            capacity: usize,
            len: usize,
            data: [BlockPool.block_size - 64]u8,

            fn create(pool: *BlockPool, archetype: Archetype) !*Page {
                const page = try pool.create(Page);
                page.capacity = 0;
                page.len = 0;

                var sz: usize = @sizeOf(usize);
                inline for (0..n_components) |i| {
                    if (archetype.isSet(i)) {
                        const c: Component = @enumFromInt(i);
                        sz += @sizeOf(ComponentType(c));
                    }
                }
                std.debug.print("sz {}\n", .{sz});

                page.capacity = page.data.len / sz;
                while (true) {
                    var ptr = @intFromPtr(&page.data[0]);
                    ptr = std.mem.alignForward(usize, ptr, @alignOf(Entity));
                    page.entities = @ptrFromInt(ptr);
                    ptr += @sizeOf(Entity) * page.capacity;
                    inline for (0..n_components) |i| {
                        if (archetype.isSet(i)) {
                            const c: Component = @enumFromInt(i);
                            const C = ComponentType(c);
                            ptr = std.mem.alignForward(usize, ptr, @alignOf(C));
                            page.components[i] = ptr;
                            ptr += @sizeOf(C) * page.capacity;
                        } else {
                            page.components[i] = 0;
                        }
                    }
                    if (ptr <= @intFromPtr(&page.data[0]) + page.data.len) break;
                    page.capacity -= 1;
                    std.debug.print("overestimate for archetype {}\n", .{archetype});
                }

                std.debug.print("{any} {}\n", .{ page.components, page.capacity });

                return page;
            }

            fn append(page: *Page, entity: Entity, template: EntityTemplate) usize {
                std.debug.assert(page.len < page.capacity);
                page.entities[page.len] = entity;
                inline for (std.meta.fields(EntityTemplate), 0..) |field, i| {
                    if (@field(template, field.name) != null) {
                        const c: Component = @enumFromInt(i);
                        page.component(c)[page.len] = @field(template, field.name).?;
                    }
                }
                const index = page.len;
                page.len += 1;
                return index;
            }

            fn component(page: *Page, comptime c: Component) [*]ComponentType(c) {
                const a = page.components[@intFromEnum(c)];
                std.debug.assert(a != 0);
                return @ptrFromInt(a);
            }
        };
        const PageView = struct {
            page: *Page,
            query: QueryInfo,

            fn entityIterator(view: PageView) EntityIterator {
                return .{
                    .page_view = view,
                    .cursor = 0,
                };
            }

            const EntityIterator = struct {
                page_view: PageView,
                cursor: usize,

                pub fn next(iterator: *EntityIterator) ?EntityView {
                    while (iterator.cursor < iterator.page_view.page.len) {
                        const index = iterator.cursor;
                        iterator.cursor += 1;
                        return .{
                            .page = iterator.page_view.page,
                            .index = index,
                            .query = iterator.page_view.query,
                        };
                    }
                    return null;
                }
            };
        };
        const EntityView = struct {
            page: *Page,
            index: usize,
            query: QueryInfo,

            fn get(view: EntityView, comptime component: Component) ComponentType(component) {
                const includes = view.query.include_read
                    .unionWith(view.query.include_read_write);
                std.debug.assert(includes.isSet(@intFromEnum(component)));
                return view.page.component(component)[view.index];
            }

            // getOptional
            // getPtr
            // getPtrOptional
        };

        pub const RawScheduleInfo = struct {
            read: []const Component = &.{},
            read_write: []const Component = &.{},
        };
        pub const ScheduleInfo = struct {
            read: Archetype,
            read_write: Archetype,

            fn init(raw: RawScheduleInfo) ScheduleInfo {
                var result = ScheduleInfo{
                    .read = Archetype.initEmpty(),
                    .read_write = Archetype.initEmpty(),
                };
                for (raw.read) |c| result.read.set(@intFromEnum(c));
                for (raw.read_write) |c| result.read_write.set(@intFromEnum(c));
                std.debug.assert(result.read.intersectWith(result.read_write).count() == 0);
                return result;
            }
        };
        pub const ScheduleView = struct {
            world: *World,
            schedule: ScheduleInfo,

            pub fn pageIterator(view: ScheduleView, raw_query: RawQueryInfo) PageIterator {
                const query = QueryInfo.init(raw_query);
                std.debug.print("{}\n", .{view.schedule});
                std.debug.print("{}\n", .{query});
                std.debug.assert(query.isValid(view.schedule));
                // const qi = QueryInfo.init(q);
                // std.debug.assert(qi.isSubset(view.info));
                return .{
                    .schedule = view,
                    .query = query,
                    .cursor = 0,
                };
            }
            const PageIterator = struct {
                schedule: ScheduleView,
                query: QueryInfo,
                cursor: usize,

                fn next(iterator: *PageIterator) ?PageView {
                    const includes = iterator.query.include_read
                        .unionWith(iterator.query.include_read_write);
                    const excludes = iterator.query.exclude;
                    while (iterator.cursor < iterator.schedule.world.pages.items.len) {
                        const page = iterator.schedule.world.pages.items[iterator.cursor];
                        const archetype = iterator.schedule.world.archetypes.items[iterator.cursor];
                        iterator.cursor += 1;
                        if (isValid(archetype, includes, excludes)) return .{
                            .page = page,
                            .query = iterator.query,
                        };
                    }
                    return null;
                }

                fn isValid(archetype: Archetype, includes: Archetype, excludes: Archetype) bool {
                    std.debug.print("{} {} {}\n", .{ archetype, includes, excludes });
                    return includes.subsetOf(archetype) and
                        excludes.intersectWith(archetype).count() == 0;
                }
            };
        };

        pub const RawQueryInfo = struct {
            include_read: []const Component = &.{},
            include_read_write: []const Component = &.{},
            optional_read: []const Component = &.{},
            optional_read_write: []const Component = &.{},
            exclude: []const Component = &.{},
        };
        pub const QueryInfo = struct {
            include_read: Archetype,
            include_read_write: Archetype,
            optional_read: Archetype,
            optional_read_write: Archetype,
            exclude: Archetype,

            fn init(raw: RawQueryInfo) QueryInfo {
                var result = QueryInfo{
                    .include_read = Archetype.initEmpty(),
                    .include_read_write = Archetype.initEmpty(),
                    .optional_read = Archetype.initEmpty(),
                    .optional_read_write = Archetype.initEmpty(),
                    .exclude = Archetype.initEmpty(),
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

            /// verify that the query is allowed for this system's schedule
            fn isValid(query: QueryInfo, schedule: ScheduleInfo) bool {
                const read_or_read_write = schedule.read.unionWith(schedule.read_write);
                const include_read = query.include_read.subsetOf(read_or_read_write);
                const include_read_write = query.include_read_write.subsetOf(schedule.read_write);
                const optional_read = query.optional_read.subsetOf(read_or_read_write);
                const optional_read_write = query.optional_read_write.subsetOf(schedule.read_write);
                return include_read and include_read_write and
                    optional_read and optional_read_write;
            }
        };
        const QueryView = struct {
            page: *Page,
            query: QueryInfo,
        };

        // pub const QueryView = struct {
        //     world: *World,
        //     info: QueryInfo,

        //     pub fn pageIterator(view: QueryView, q: RawQueryInfo) PageIterator {
        //         const qi = QueryInfo.init(q);
        //         std.debug.assert(qi.isSubset(view.info));
        //         return .{};
        //     }
        //     const PageIterator = struct {};
        // };
        // pub fn Query(comptime info: QueryInfo) type {
        //     _ = info;
        //     return struct {};
        // }

        const World = struct {
            // TODO check the performance implications of linear archetype searches
            // it should primarily impact create or insert/remove-component operations i think

            const CreateQueueEntry = struct {
                entity: Entity,
                template: EntityTemplate,
            };

            ecs: *Self,

            // pages hold the actual entity data, with one page per archetype
            pages: std.ArrayList(*Page),
            archetypes: std.ArrayList(Archetype),

            // buckets hold a lookup table from an entity to it's location in a page
            depth: usize = 0,
            buckets: std.ArrayList(*Bucket), // extendible hashing
            bucket_depths: std.ArrayList(usize),

            // insert_queues:
            // remove_queues:
            create_queue: std.ArrayList(CreateQueueEntry),
            destroy_queue: std.ArrayList(Entity),

            pub fn queueCreate(world: *World, template: EntityTemplate) !Entity {
                const entity = world.ecs.newEntity();
                try world.create_queue.append(.{
                    .entity = entity,
                    .template = template,
                });
                return entity;
            }

            pub fn queueDestroy(world: *World, entity: Entity) !void {
                std.debug.assert(entity != 0);
                try world.destroy_queue.append(entity);
            }

            pub fn queueInsert(
                world: *World,
                entity: Entity,
                comptime component: Component,
                value: ComponentType(component),
            ) !void {
                std.debug.assert(entity != 0);
                _ = world;
                _ = value;
            }

            pub fn queueRemove(
                world: *World,
                entity: Entity,
                comptime component: Component,
            ) !void {
                std.debug.assert(entity != 0);
                _ = world;
                _ = component;
            }

            pub fn processQueues(world: *World) !void {
                for (world.create_queue.items) |q| {
                    var archetype = Archetype.initEmpty();
                    inline for (std.meta.fields(EntityTemplate), 0..) |field, i| {
                        if (@field(q.template, field.name) != null) archetype.set(i);
                    }
                    std.debug.print("{}\n", .{archetype});
                    std.debug.print("{s}\n", .{@typeName(@TypeOf(q.template))});
                    const page = try world.getPageMatch(archetype);
                    const index = page.append(q.entity, q.template);
                    try world.addLookup(q.entity, page, index);
                    std.debug.print("{}\n", .{page.*});
                }
            }

            fn getPageMatch(world: *World, archetype: Archetype) !*Page {
                for (world.archetypes.items, world.pages.items) |a, p| {
                    if (a.eql(archetype) and p.len < p.capacity) return p;
                }
                // no page that can hold the entity exists, create a new one
                const page = try Page.create(&world.ecs.pool, archetype);
                try world.archetypes.append(archetype);
                try world.pages.append(page);
                return page;
            }

            fn addLookup(world: *World, entity: Entity, page: *Page, index: usize) !void {
                if (world.buckets.items.len == 0) {
                    const bucket = try Bucket.create(&world.ecs.pool);
                    try world.buckets.append(bucket);
                    try world.bucket_depths.append(0);
                    world.depth = 0;
                }

                const slotmask = (@as(u64, 1) << @intCast(world.depth)) - 1;
                const slot = entity & slotmask;
                std.debug.assert(slot < world.buckets.items.len);
                const bucket = world.buckets.items[slot];

                const success = bucket.insert(entity, page, index);
                std.debug.assert(success);

                if (!bucket.full()) return;
                @panic("TODO: expand hash");
            }

            fn lookup(world: *World, entity: Entity) ?EntityView {
                _ = world;
                _ = entity;
            }

            fn eval(world: *World, schedule: ScheduleInfo, system: System) void {
                std.debug.print("{}\n", .{schedule});
                system(ScheduleView{
                    .world = world,
                    .schedule = schedule,
                });
            }
        };

        alloc: std.mem.Allocator, // used sparingly, mostly for large allocations inside BlockPool
        id_counter: u64, // any number except 0 is fine
        pool: BlockPool,

        fn init(alloc: std.mem.Allocator) Self {
            return .{
                .alloc = alloc,
                .id_counter = 1,
                .pool = BlockPool.init(alloc),
            };
        }

        fn deinit(ecs: *Self) void {
            ecs.pool.deinit();
            ecs.* = undefined;
        }

        fn newEntity(ecs: *Self) u64 {
            // xorshift* with 2^64 - 1 period (0 is fixed point, and also the null entity)
            var x = ecs.id_counter;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            ecs.id_counter = x;
            return x *% 0x2545F4914F6CDD1D;
        }

        pub fn initWorld(ecs: *Self) World {
            return .{
                .ecs = ecs,
                .pages = std.ArrayList(*Page).init(ecs.alloc),
                .archetypes = std.ArrayList(Archetype).init(ecs.alloc),
                .depth = 0,
                .buckets = std.ArrayList(*Bucket).init(ecs.alloc),
                .bucket_depths = std.ArrayList(usize).init(ecs.alloc),
                .create_queue = std.ArrayList(World.CreateQueueEntry).init(ecs.alloc),
                .destroy_queue = std.ArrayList(Entity).init(ecs.alloc),
            };
        }

        pub fn deinitWorld(ecs: *Self, world: *World) void {
            _ = ecs;
            world.pages.deinit();
            world.archetypes.deinit();
            world.buckets.deinit();
            world.bucket_depths.deinit();
            world.create_queue.deinit();
            world.destroy_queue.deinit();
        }
    };
}

const TestComponents = struct {
    a: u128,
    b: f32,
};
const TestQueues = struct {
    c: i64,
};

const Foo = struct {
    a: ?u32 = null,
};

const ecstype = ECS(TestComponents, TestQueues);

const foobar_info = ecstype.ScheduleInfo.init(.{
    .read = &[_]ecstype.Component{.a}, // can we make this less verbose?
});
fn foobar(view: ecstype.ScheduleView) void {
    std.debug.print("hello system\n", .{});
    var iterator = view.pageIterator(.{ .include_read = &[_]ecstype.Component{.a} });
    while (iterator.next()) |page| {
        std.debug.print("{}\n", .{page.page.len});
        var it = page.entityIterator();
        while (it.next()) |entity| {
            std.debug.print("{}\n", .{entity.get(.a)});
        }
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var ecs = ecstype.init(alloc);
    defer ecs.deinit();

    std.debug.print("{}\n", .{ecs});
    std.debug.print("{s}\n", .{@typeName(ecstype.ComponentType(.b))});

    const zero: u64 = 0;
    std.debug.print("{}\n", .{std.hash.XxHash3.hash(0, std.mem.asBytes(&zero))});

    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});
    std.debug.print("{}\n", .{ecs.newEntity()});

    var world = ecs.initWorld();
    defer ecs.deinitWorld(&world);

    _ = try world.queueCreate(.{});
    _ = try world.queueCreate(.{ .a = 123 });
    _ = try world.queueCreate(.{ .b = 2.0 });
    _ = try world.queueCreate(.{ .a = 1337, .b = 33.4 });
    _ = try world.queueCreate(.{ .b = 3.0 });
    try world.processQueues();

    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .a)});
    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .b)});

    world.eval(foobar_info, foobar);
}
