const builtin = @import("builtin");
const std = @import("std");

const BlockPool = @import("block_pool.zig").BlockPool;

const safe_mode = builtin.mode == .Debug or builtin.mode == .ReleaseSafe;
const single_threaded = builtin.single_threaded;

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

        // pub const QueryInfo = struct {
        //     include_read: []const Component,
        //     include_modify: []const Component,
        //     optional_read: []const Component,
        //     optional_modify: []const Component,
        //     exclude: []const Component,
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
            // destroy_queue

            pub fn queueCreate(world: *World, template: EntityTemplate) !Entity {
                const entity = world.ecs.newEntity();
                try world.create_queue.append(.{
                    .entity = entity,
                    .template = template,
                });
                return entity;
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
            };
        }

        pub fn deinitWorld(ecs: *Self, world: *World) void {
            _ = ecs;
            world.pages.deinit();
            world.archetypes.deinit();
            world.buckets.deinit();
            world.bucket_depths.deinit();
            world.create_queue.deinit();
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const ecstype = ECS(TestComponents, TestQueues);
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
}
