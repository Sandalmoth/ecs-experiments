const std = @import("std");

// design thoughts
// no more than 1024*1024 total entities
// no more than 65535 component types or archetypes
// use bloom filter to quickly find archetypes that might match
// use strings to identify components
// entities are stored on fixed sized pages, a page contains only one archetype
// on each page, the components are stored in noninterleaved order
// we do this such that we can use a memorypool of pages to efficiently create/free pages

const MEMORY_PAGE_SIZE = 16 * 1024;

const MAX_ENTITIES = 1024 * 1024; // constricted by u20 id field in Entity
const MAX_ARCHETYPES = 1024; // not actually a constraint, just a reasonable cap
const MAX_COMPONENTS = 256; // not actually a constraint, just a reasonable cap

const BloomFilter = struct {
    // not exactly a bloom filter but does conceptually the same thing (slightly worse I think)
    // but on the other hand it is easy to implement with the vector type
    data: @Vector(4, usize) = @splat(0),

    fn insert(bf: *BloomFilter, x: u32) void {
        const h = std.hash.uint32(x);
        const sft = @Vector(4, usize){
            h & 63,
            (h >> 6) & 63,
            (h >> 12) & 63,
            (h >> 18) & 63,
        };
        bf.data = bf.data | (@as(@Vector(4, usize), @splat(1)) << @intCast(sft));
    }

    /// test whether the element might be in the filter
    fn contains(bf: BloomFilter, x: u32) bool {
        var ss = BloomFilter{};
        ss.insert(x);
        return bf.includes(ss);
    }

    /// test whether a filter might be a subset of another
    fn includes(bf: BloomFilter, subset: BloomFilter) bool {
        // mask off the the subset with the superset, if anything is lost then it is not a subset
        // since the subset would be completely masked by an actual superset and thus kept
        const overlap = (bf.data & subset.data) == subset.data;
        return @reduce(.And, overlap);
    }
};

test "bloom filter" {
    std.debug.print("\n", .{});
    var bf = BloomFilter{};

    bf.insert(0);
    bf.insert(1);
    bf.insert(2);
    bf.insert(3);

    try std.testing.expect(bf.contains(0));
    try std.testing.expect(bf.contains(1));
    try std.testing.expect(bf.contains(2));
    try std.testing.expect(bf.contains(3));
    try std.testing.expect(!bf.contains(12345));
    try std.testing.expect(!bf.contains(1337));

    var ss = BloomFilter{};
    ss.insert(1);
    ss.insert(2);

    var not_ss = BloomFilter{};
    not_ss.insert(12345);
    not_ss.insert(1337);

    try std.testing.expect(bf.includes(ss));
    try std.testing.expect(!bf.includes(not_ss));
}

const Entity = packed struct {
    id: u20,
    ver: u10,
    active: bool,
    _pad: u1 = 0,

    pub fn reuse(old: Entity) Entity {
        std.debug.assert(!old.active);
        return .{
            .id = old.id,
            .ver = old.ver +% 1,
            .active = true,
        };
    }
};

comptime {
    std.debug.assert(@sizeOf(Entity) == 4);
}

const Header = struct {
    n_components: usize,
    components: *[*]u32,
    entities: [*]Entity,
    data: *[*]usize, // type erased array of [*]Component in the same order
    len: usize,
    capacity: usize,
    next: ?*Page,
};

const Page = struct {
    header: Header,
    data: [MEMORY_PAGE_SIZE - @sizeOf(Header)]u8,

    fn create(alloc: std.mem.Allocator, spec: []Component) !*Page {
        const page = try alloc.create(Page);

        page.header.n_components = spec.len;
        // do the math to figure out how to fit everything in the data block
        // layout is
        // [n_components]u32
        // [n_components]usize
        // [capacity]Entity
        // [capacity]<component 0>
        // [capacity]<component 1>
        // ...
        // with padding between each block s.t. alignment is satisfied
        var p: usize = @intFromPtr(&page.data[0]);

        p = std.mem.alignForward(usize, p, @alignOf(u32));
        page.header.components = @ptrFromInt(p);
        p += @sizeOf(u32) * page.header.n_components;
        for (spec, 0..) |_, i| {
            page.header.components.*[i] = @intCast(i);
        }

        p = std.mem.alignForward(usize, p, @alignOf(usize));
        page.header.data = @ptrFromInt(p);
        p += @sizeOf(usize) * page.header.n_components;

        p = std.mem.alignForward(usize, p, @alignOf(Entity));
        page.header.entities = @ptrFromInt(p);

        // not sure how to solve it deterministically, but we can overestimate and back off
        const n_bytes = @intFromPtr(&page.data[page.data.len - 1]) - p;
        var total_size: usize = @sizeOf(Entity);
        for (spec) |c| {
            total_size += c.size;
        }
        var capacity: usize = n_bytes / total_size;
        const pstart = p;
        while (capacity > 0) {
            // attempt to layout memory and if it fails, lower capacity and retry
            p += @sizeOf(Entity) * capacity;
            for (spec, 0..) |c, i| {
                p = std.mem.alignForward(usize, p, c.alignment);
                // std.debug.print("{s}\n", .{@typeName(@TypeOf(p.header.data))});
                page.header.data.*[i] = p;
                p += c.size * capacity;
            }
            if (p < @intFromPtr(&page.data[page.data.len - 1])) {
                // we fit in the page, so we're done
                break;
            }

            p = pstart;
            std.log.debug("Failed to layout a page with capacity {}. Retrying", .{capacity});
            capacity -= 1;
        }

        if (capacity == 0) {
            std.log.err("Failed completely to layout a page with {} components", .{spec.len});
            return error.PageCannotHoldArchetype;
        }

        page.header.len = 0;
        page.header.capacity = capacity;
        page.header.next = null;

        return page;
    }

    fn destroy(page: *Page, alloc: std.mem.Allocator) void {
        alloc.destroy(page);
    }

    fn components(page: *Page) []u32 {
        return page.header.components.*[0..page.header.n_components];
    }
};

comptime {
    std.debug.assert(@sizeOf(Page) == MEMORY_PAGE_SIZE);
}

const Component = struct {
    name: []const u8,
    size: usize, // @sizeOf the type
    alignment: u8, // @alignOf the type
};

// since we have 1M of these, avoid extraenous stuff...
const Location = struct {
    entity: Entity = .{ .id = 0, .ver = 1023, .active = false },
    page: ?*Page = null,
    index: u32 = 0, // either index into archetype linked list, or part of entity free list
};

/// NOTE this struct is large (12+ MiB), and has to be heap allocated
pub const World = struct {
    alloc: std.mem.Allocator,

    // NOTE components can never be removed, only added
    n_components: u32,
    components: [MAX_COMPONENTS]Component,

    n_archetypes: u32,
    archetype_pages: [MAX_ARCHETYPES]?*Page, // a linked list of pages of the same archetype
    archetype_filters: [MAX_ARCHETYPES]BloomFilter,

    // putting every entity id into a queue ensures we only rarely reuse entities
    // however, it reduces the locality of reference for entity ids on average
    // the other option is to use a stack of free entities
    // but then we maximaly reuse ids, which increases the odds of bugs
    free_entity_head: u32, // queue head, pop from here
    free_entity_tail: u32, // queue tail, push to here
    entities: [MAX_ENTITIES]Location,

    pub fn create(alloc: std.mem.Allocator) !*World {
        const world = try alloc.create(World);

        world.alloc = alloc;
        world.n_components = 0;
        world.components = .{
            Component{ .name = "UNDEFINED_COMPONENT", .size = 0, .alignment = 0 },
        } ** MAX_COMPONENTS;
        world.n_archetypes = 0;
        world.archetype_pages = .{null} ** MAX_ARCHETYPES;
        world.archetype_filters = .{BloomFilter{}} ** MAX_ARCHETYPES;
        world.free_entity_head = 0;
        world.free_entity_tail = MAX_ENTITIES - 1;
        world.entities = .{Location{}} ** MAX_ENTITIES;

        for (0..MAX_ENTITIES) |i| {
            world.entities[i].entity.id = @intCast(i);
            world.entities[i].index = @intCast(i + 1);
        }

        return world;
    }

    pub fn destroy(world: *World) void {
        for (world.archetype_pages[0..world.n_archetypes]) |page| {
            std.debug.assert(page != null);
            var _p = page;
            while (_p) |p| {
                const next = p.header.next;
                p.destroy(world.alloc);
                _p = next;
            }
        }
        world.alloc.destroy(world);
    }

    pub fn registerComponent(world: *World, name: []const u8, comptime T: type) void {
        world.components[world.n_components] = .{
            .name = name,
            .size = @sizeOf(T),
            .alignment = @alignOf(T),
        };
    }

    pub fn newEntity(world: *World) !Entity {
        // TODO edge conditions
        const entity = world.entities[world.free_entity_head].entity.reuse();
        world.entities[world.free_entity_head].entity = entity;
        world.free_entity_head = world.entities[world.free_entity_head].index;

        try world.storeEntity(entity, &.{});

        return entity;
    }

    fn storeEntity(world: *World, entity: Entity, components: []u32) !void {
        const page = try world.ensureArchetype(components);

        world.entities[entity.id].entity = entity;
        world.entities[entity.id].page = page;
    }

    /// returns the first page of a given archetype that has room to store it
    fn ensureArchetype(world: *World, components: []u32) !*Page {
        const arch = world.findArchetype(components) orelse try world.addArchetype(components);
        std.debug.assert(world.archetype_pages[arch] != null);

        var page = world.archetype_pages[arch];
        std.debug.print("{}\n", .{page.?.header});
        while (page) |p| {
            if (p.header.len < p.header.capacity) {
                return p;
            }
            page = p.header.next;
        }
        // we found no page with room, make a new page
        page = world.archetype_pages[arch];
        var spec: [MAX_COMPONENTS]Component = undefined; // could be trouble for silly max:s?
        for (components, 0..) |c, i| {
            spec[i] = world.components[c];
        }
        std.debug.print("1\n", .{});
        world.archetype_pages[arch] = try Page.create(world.alloc, spec[0..components.len]);
        world.archetype_pages[arch].?.header.next = page;

        return world.archetype_pages[arch].?;
    }

    fn findArchetype(world: *World, components: []u32) ?u32 {
        std.debug.assert(std.sort.isSorted(u32, components, {}, std.sort.asc(u32)));

        var filter = BloomFilter{};
        for (components) |c| {
            filter.insert(c);
        }

        for (0..world.n_archetypes) |i| {
            if (!world.archetype_filters[i].includes(filter)) {
                continue;
            }

            std.debug.assert(world.archetype_pages[i] != null);
            const page = world.archetype_pages[0].?;
            if (std.mem.eql(u32, components, page.components())) {
                return @intCast(i);
            }
        }

        return null;
    }

    fn archetypeExists(world: *World, components: []u32) bool {
        return world.findArchetype(components) != null;
    }

    fn addArchetype(world: *World, components: []u32) !u32 {
        std.debug.assert(!world.archetypeExists(components));

        const arch = world.n_archetypes;
        var spec: [MAX_COMPONENTS]Component = undefined; // could be trouble for silly max:s?
        for (components, 0..) |c, i| {
            spec[i] = world.components[c];
        }
        std.debug.print("2\n", .{});
        world.archetype_pages[arch] = try Page.create(world.alloc, spec[0..components.len]);

        world.n_archetypes += 1;
        return arch;
    }

    fn componentId(world: *World, name: []const u8) !Component {
        // linear search, but probably wont be on a critical path tbh
        for (0..world.n_components) |i| {
            if (std.mem.eql(u8, name, world.components[i].name)) {
                return i;
            }
        }

        std.log.err("Tried to get the Id of an unknown component: " ++ name);
        return error.UnknownComponent;
    }
};

test "scratch" {
    std.debug.print("\nsize of empty world is {} KiB\n", .{@sizeOf(World) / 1024});

    const world = try World.create(std.testing.allocator);
    defer world.destroy();

    world.registerComponent("float1", f32);
    world.registerComponent("float2", f32);

    const e0 = world.newEntity();
    const e1 = world.newEntity();
    const e2 = world.newEntity();

    std.debug.print("{any}\n", .{e0});
    std.debug.print("{any}\n", .{e1});
    std.debug.print("{any}\n", .{e2});
}
