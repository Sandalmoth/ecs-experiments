const builtin = @import("builtin");
const std = @import("std");

const BlockPool = @import("block_pool.zig").BlockPool;

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
comptime {
    std.debug.assert(!builtin.single_threaded);
}

// generally with the view types, it would be nice if the query was comptime known
// but partially comptime structs are currently not possible
// though could be supported in the future https://github.com/ziglang/zig/issues/5675
// alternatively, we could encode the query restrictions in the type
// so that we could check compliance at compile time
// though, it's not clear how to make the syntax work well
// it is possible to let the world.eval read the query type from the scheduled functions argument
// which is kinda nice

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

    fn empty(queue: *UntypedQueue) bool {
        const head = queue.head orelse return true;
        return head.head == head.tail;
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

            fn empty(bucket: Bucket) bool {
                return bucket.len == 0; // TODO consider early merging with threshold
            }

            fn insert(bucket: *Bucket, entity: Entity, page: *Page, index: usize) bool {
                std.debug.assert(bucket.len < capacity);

                const fingerprint: u8 = @intCast(entity >> 56);
                var ix = (entity >> 32) % capacity;

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == entity) return false;
                    }
                }

                bucket.pages[ix] = page;
                bucket.indices[ix] = index;
                bucket.fingerprints[ix] = fingerprint;
                bucket.len += 1;
                return true;
            }

            fn remove(bucket: *Bucket, entity: Entity) bool {
                const fingerprint: u8 = @intCast(entity >> 56);
                var ix = (entity >> 32) % capacity;

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e != entity) continue;

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
                            const entity_shift = page_shift.entities[bucket.indices[ix_shift]];
                            const key_dist = (ix_shift -% entity_shift) % capacity;
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

            fn update(bucket: *Bucket, entity: Entity, page: *Page, index: usize) bool {
                std.debug.assert(bucket.len < capacity);

                const fingerprint: u8 = @intCast(entity >> 56);
                var ix = (entity >> 32) % capacity;

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == entity) {
                            bucket.pages[ix] = page;
                            bucket.indices[ix] = index;
                            bucket.fingerprints[ix] = fingerprint;
                            return true;
                        }
                    }
                }

                return false;
            }

            fn get(bucket: Bucket, entity: Entity) ?EntityView {
                const fingerprint: u8 = @intCast(entity >> 56);
                var ix = (entity >> 32) % capacity;

                while (bucket.pages[ix] != null) : (ix = (ix + 1) % capacity) {
                    if (bucket.fingerprints[ix] == fingerprint) {
                        const e = bucket.pages[ix].?.entities[bucket.indices[ix]];
                        if (e == entity) return .{
                            .page = bucket.pages[ix].?,
                            .index = bucket.indices[ix],
                            .query = undefined,
                        };
                    }
                }

                return null;
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
                // std.debug.print("sz {}\n", .{sz});

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

                // std.debug.print("{any} {}\n", .{ page.components, page.capacity });

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

            /// returns the entity that was relocated (or null entity if no relocation)
            fn erase(page: *Page, index: usize) Entity {
                const end = page.len - 1;
                if (index == end) {
                    // easy special case with no swap
                    page.len -= 1;
                    return 0;
                }

                const moved = page.entities[end];
                page.entities[index] = page.entities[end];
                inline for (page.components, 0..) |a, i| {
                    if (a != 0) {
                        const c: Component = @enumFromInt(i);
                        const data = page.component(c);
                        data[index] = data[end];
                    }
                }
                page.len -= 1;
                return moved;
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

            fn getOptional(
                view: EntityView,
                comptime component: Component,
            ) ?ComponentType(component) {
                const includes = view.query.include_read
                    .unionWith(view.query.include_read_write);
                std.debug.assert(includes.isSet(@intFromEnum(component)));
                if (view.page.components[@intFromEnum(component)] == 0) return null;
                return view.page.component(component)[view.index];
            }

            fn getTemplate(view: EntityView) EntityTemplate {
                var template = EntityTemplate{};
                inline for (0..n_components) |i| {
                    const c: Component = @enumFromInt(i);
                    @field(template, @tagName(c)) = view.getOptional(c);
                }
                return template;
            }

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
                // std.debug.print("{}\n", .{view.schedule});
                // std.debug.print("{}\n", .{query});
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
                    // std.debug.print("{} {} {}\n", .{ archetype, includes, excludes });
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

            const any = QueryInfo{
                .include_read = Archetype.initEmpty(),
                .include_read_write = Archetype.initFull(),
                .optional_read = Archetype.initEmpty(),
                .optional_read_write = Archetype.initEmpty(),
                .exclude = Archetype.initEmpty(),
            };

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

        const World = struct {
            // TODO check the performance implications of linear archetype searches
            // it should primarily impact create or insert/remove-component operations i think

            const CreateQueueEntry = struct {
                entity: Entity,
                template: EntityTemplate,
            };

            fn InsertQueueEntry(comptime T: type) type {
                return struct {
                    entity: Entity,
                    value: T,
                };
            }

            ecs: *Self,

            // pages hold the actual entity data, with one page per archetype
            pages: std.ArrayList(*Page),
            archetypes: std.ArrayList(Archetype),

            // buckets hold a lookup table from an entity to it's location in a page
            depth: usize = 0,
            buckets: std.ArrayList(*Bucket), // extendible hashing
            bucket_depths: std.ArrayList(usize),

            create_queue: TypedQueue(CreateQueueEntry),
            destroy_queue: TypedQueue(Entity),
            insert_queues: [n_components]UntypedQueue,
            remove_queues: [n_components]TypedQueue(Entity),
            queues: [n_queues]UntypedQueue,

            pub fn queueCreate(world: *World, template: EntityTemplate) !Entity {
                const entity = world.ecs.newEntity();
                try world.create_queue.push(.{
                    .entity = entity,
                    .template = template,
                });
                return entity;
            }

            pub fn queueCreate2(world: *World, entity: Entity, template: EntityTemplate) !void {
                try world.create_queue.push(.{
                    .entity = entity,
                    .template = template,
                });
            }

            pub fn queueDestroy(world: *World, entity: Entity) !void {
                std.debug.assert(entity != 0);
                try world.destroy_queue.push(entity);
            }

            pub fn queueInsert(
                world: *World,
                entity: Entity,
                comptime component: Component,
                value: ComponentType(component),
            ) !void {
                std.debug.assert(entity != 0);
                try world.insert_queues[@intFromEnum(component)].push(
                    InsertQueueEntry(ComponentType(component)),
                    .{ .entity = entity, .value = value },
                );
            }

            pub fn queueRemove(
                world: *World,
                entity: Entity,
                comptime component: Component,
            ) !void {
                std.debug.assert(entity != 0);
                try world.remove_queues[@intFromEnum(component)].push(entity);
            }

            fn processCreateQueue(world: *World) !void {
                while (world.create_queue.pop()) |q| {
                    var archetype = Archetype.initEmpty();
                    inline for (std.meta.fields(EntityTemplate), 0..) |field, i| {
                        if (@field(q.template, field.name) != null) archetype.set(i);
                    }
                    // std.debug.print("{}\n", .{archetype});
                    // std.debug.print("{s}\n", .{@typeName(@TypeOf(q.template))});
                    const page = try world.getPageMatch(archetype);
                    const index = page.append(q.entity, q.template);
                    try world.addLookup(q.entity, page, index);
                    // std.debug.print("{}\n", .{page.*});
                }
            }

            fn processDestroyQueue(world: *World) void {
                while (world.destroy_queue.pop()) |e| {
                    const entity = world.lookup(e) orelse continue;
                    world.removeLookup(e);
                    const moved = entity.page.erase(entity.index);
                    if (moved != 0) world.updateLookup(moved, entity.page, entity.index);
                }
            }

            pub fn processQueues(world: *World) !void {
                // NOTE we could cancel out creates with immediate destroys
                // and save some processing in those cases, but it's unclear if that's worth it
                try world.processCreateQueue();
                world.processDestroyQueue();

                inline for (0..n_components) |i| {
                    // update components on entities by destroying and recreating at a batch level
                    // alternative would be to update per modification which may be more efficent
                    // as it causes less edits to the lookup table
                    // although, batching is often faster so this method might be more predictable

                    std.debug.assert(world.create_queue.empty());
                    std.debug.assert(world.destroy_queue.empty());
                    const c: Component = @enumFromInt(i);
                    const C = ComponentType(c);
                    while (world.insert_queues[i].pop(InsertQueueEntry(C))) |q| {
                        const entity = world.lookup(q.entity) orelse continue;
                        if (entity.page.components[i] != 0) {
                            std.debug.print("entity has component, ignoring insert\n", .{});
                            continue;
                        }
                        var template = entity.getTemplate();
                        @field(template, @tagName(c)) = q.value;
                        try world.queueCreate2(q.entity, template);
                        try world.queueDestroy(q.entity);
                    }
                    world.processDestroyQueue();
                    try world.processCreateQueue();

                    std.debug.assert(world.create_queue.empty());
                    std.debug.assert(world.destroy_queue.empty());
                    while (world.remove_queues[i].pop()) |e| {
                        const entity = world.lookup(e) orelse continue;
                        if (entity.page.components[i] == 0) {
                            std.debug.print("entity lacks component, ignoring remove\n", .{});
                            continue;
                        }
                        var template = entity.getTemplate();
                        @field(template, @tagName(c)) = null;
                        try world.queueCreate2(e, template);
                        try world.queueDestroy(e);
                    }
                    world.processDestroyQueue();
                    try world.processCreateQueue();

                    // while (world.insert_queues[i].pop(InsertQueueEntry(C))) |q| {
                    //     const entity = world.lookup(q.entity) orelse continue;
                    //     if (entity.page.components[i] != 0) {
                    //         std.debug.print("entity has component, ignoring insert\n", .{});
                    //         continue;
                    //     }
                    //     var archetype = Archetype.initEmpty();
                    //     archetype.set(i);
                    //     for (entity.page.components, 0..) |a, j| {
                    //         if (a != 0) archetype.set(j);
                    //     }
                    //     const page = try world.getPageMatch(archetype);
                    //     var template = entity.getTemplate();
                    //     @field(template, @tagName(c)) = q.value;
                    //     const index = page.append(q.entity, q.template);
                    //     const moved = entity.page.erase(entity.index);
                    //     world.updateLookup(q.entity, page, index);
                    //     if (moved != 0) world.updateLookup(moved, entity.page, entity.index);
                    // }

                    // while (world.remove_queues[i].pop()) |e| {
                    //     _ = e;
                    // }
                }

                // cleanup unused archetypes
                var ix: usize = 0;
                while (ix < world.pages.items.len) {
                    if (world.pages.items[ix].len > 0) {
                        ix += 1;
                        continue;
                    }
                    std.debug.assert(world.pages.items.len == world.archetypes.items.len);
                    const page = world.pages.swapRemove(ix);
                    _ = world.archetypes.swapRemove(ix);
                    world.ecs.pool.destroy(page);
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

            fn removeLookup(world: *World, entity: Entity) void {
                // TODO defensive programming
                const slotmask = (@as(u64, 1) << @intCast(world.depth)) - 1;
                const slot = entity & slotmask;
                std.debug.assert(slot < world.buckets.items.len);
                const bucket = world.buckets.items[slot];

                const success = bucket.remove(entity);
                std.debug.assert(success);

                if (!bucket.empty()) return;
                @panic("TODO: shrink hash");
            }

            fn updateLookup(world: *World, entity: Entity, page: *Page, index: usize) void {
                const slotmask = (@as(u64, 1) << @intCast(world.depth)) - 1;
                const slot = entity & slotmask;
                std.debug.assert(slot < world.buckets.items.len);
                const bucket = world.buckets.items[slot];

                const success = bucket.update(entity, page, index);
                std.debug.assert(success);
            }

            fn lookup(world: *World, entity: Entity) ?EntityView {
                const slotmask = (@as(u64, 1) << @intCast(world.depth)) - 1;
                const slot = entity & slotmask;
                std.debug.assert(slot < world.buckets.items.len);
                const bucket = world.buckets.items[slot];
                var result = bucket.get(entity) orelse return null;
                result.query = .any;
                return result;
            }

            fn eval(world: *World, schedule: ScheduleInfo, system: System) void {
                // std.debug.print("{}\n", .{schedule});
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
            var world = World{
                .ecs = ecs,
                .pages = std.ArrayList(*Page).init(ecs.alloc),
                .archetypes = std.ArrayList(Archetype).init(ecs.alloc),
                .depth = 0,
                .buckets = std.ArrayList(*Bucket).init(ecs.alloc),
                .bucket_depths = std.ArrayList(usize).init(ecs.alloc),
                .create_queue = TypedQueue(World.CreateQueueEntry).init(&ecs.pool),
                .destroy_queue = TypedQueue(Entity).init(&ecs.pool),
                .insert_queues = undefined,
                .remove_queues = undefined,
                .queues = undefined,
            };
            for (0..n_components) |i| {
                world.insert_queues[i] = UntypedQueue.init(&ecs.pool);
                world.remove_queues[i] = TypedQueue(Entity).init(&ecs.pool);
            }
            for (0..n_queues) |i| {
                world.queues[i] = UntypedQueue.init(&ecs.pool);
            }
            return world;
        }

        pub fn deinitWorld(ecs: *Self, world: *World) void {
            _ = ecs;
            world.pages.deinit();
            world.archetypes.deinit();
            world.buckets.deinit();
            world.bucket_depths.deinit();
            world.create_queue.deinit();
            world.destroy_queue.deinit();
            for (0..n_components) |i| {
                world.insert_queues[i].deinit();
                world.remove_queues[i].deinit();
            }
            for (0..n_queues) |i| {
                world.queues[i].deinit();
            }
            world.* = undefined;
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
    const e0 = try world.queueCreate(.{ .a = 123 });
    const e1 = try world.queueCreate(.{ .b = 2.0 });
    const e2 = try world.queueCreate(.{ .a = 1337, .b = 33.4 });
    _ = try world.queueCreate(.{ .b = 3.0 });
    try world.processQueues();

    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .a)});
    std.debug.print("{}\n", .{std.meta.fieldInfo(ecstype.EntityTemplate, .b)});

    world.eval(foobar_info, foobar);

    try world.queueDestroy(e0);
    try world.processQueues();

    world.eval(foobar_info, foobar);

    try world.queueInsert(e1, .a, 999);
    try world.queueRemove(e2, .a);
    try world.processQueues();

    world.eval(foobar_info, foobar);
}
