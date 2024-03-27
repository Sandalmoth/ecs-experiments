const std = @import("std");

pub const Entity = u32;
pub const Detail = packed struct {
    id: u16,
    version: u16,
};

pub fn detail(e: Entity) Detail {
    return @bitCast(e);
}

pub const nil: Entity = @bitCast(Detail{
    .id = undefined,
    .version = std.math.maxInt(u16),
});

pub fn isnil(e: Entity) bool {
    const d = detail(e);
    return d.version == std.math.maxInt(u16);
}

pub const Signal = enum {
    add,
    del,
};

pub const EntityIterator = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        next: *const fn (cts: *anyopaque) ?Entity,
    };

    pub fn next(ei: EntityIterator) ?Entity {
        return ei.vtable.next(ei.ctx);
    }
};

pub const Storage = struct {
    const Self = @This();

    alloc: std.mem.Allocator,
    // like, this is kinda silly
    // but how silly is it really though? It's just 400kB
    // so even with 100 components, that's just 40MB of data, which is nothing
    // and since data is packed in dense, we don't even touch most of it
    sparse: [65536]u16,
    dense: [65536]Entity,
    _data: *anyopaque, // std.ArrayList(V)
    len: usize,

    fn data(storage: *Self, comptime V: type) *std.ArrayList(V) {
        return @ptrCast(@alignCast(storage._data));
    }

    pub fn init(comptime V: type, alloc: std.mem.Allocator) !Self {
        var storage = Storage{
            .alloc = alloc,
            .sparse = undefined,
            .dense = .{nil} ** 65536,
            ._data = undefined,
            .len = 0,
        };

        if (@sizeOf(V) > 0) {
            storage._data = @ptrCast(try storage.alloc.create(std.ArrayList(V)));
            errdefer storage.alloc.destroy(storage.data(V));
            storage.data(V).* = try std.ArrayList(V).initCapacity(storage.alloc, 64);
        }

        return storage;
    }

    pub fn deinit(storage: *Self, comptime V: type) void {
        if (@sizeOf(V) > 0) {
            storage.data(V).deinit();
            storage.alloc.destroy(storage.data(V));
        }
        storage.* = undefined;
    }

    pub fn get(storage: *Self, comptime V: type, e: Entity) ?*V {
        std.debug.assert(!isnil(e));
        if (@sizeOf(V) == 0) @compileError("cannot get a void type, consider using Storage.has");

        const loc = storage.sparse[detail(e).id];
        if (storage.dense[loc] != e) return null;
        return &storage.data(V).items[loc];
    }

    pub fn has(storage: *Self, e: Entity) bool {
        std.debug.assert(!isnil(e));
        return storage.dense[storage.sparse[detail(e).id]] == e;
    }

    pub fn add(storage: *Self, comptime V: type, e: Entity, val: V) !void {
        std.debug.assert(!isnil(e));
        const loc = storage.len;
        if (@sizeOf(V) > 0) try storage.data(V).append(val);
        storage.dense[loc] = e;
        storage.sparse[detail(e).id] = @intCast(loc);
        storage.len += 1;
    }

    pub fn del(storage: *Self, comptime V: type, e: Entity) void {
        std.debug.assert(!isnil(e));
        std.debug.assert(storage.len > 0);
        const loc = storage.sparse[detail(e).id];

        const last = storage.dense[storage.len - 1];
        storage.dense[loc] = last;
        storage.sparse[detail(last).id] = @intCast(loc);
        if (@sizeOf(V) > 0) _ = storage.data(V).swapRemove(loc);
        storage.len -= 1;
    }

    /// return the first entity
    pub fn front(storage: *Self) Entity {
        std.debug.assert(storage.len > 0);
        return storage.dense[0];
    }

    const Iterator = struct {
        storage: *Storage,
        cursor: usize,

        pub fn next(ctx: *anyopaque) ?Entity {
            const it: *Iterator = @alignCast(@ptrCast(ctx));
            if (it.cursor == 0) return null;
            it.cursor -= 1;
            return it.storage.dense[it.cursor];
        }

        const vtable = EntityIterator.VTable{
            .next = &next,
        };

        pub fn iter(it: *Iterator) EntityIterator {
            return .{
                .ctx = @ptrCast(it),
                .vtable = &vtable,
            };
        }
    };

    pub fn iterator(storage: *Self) Iterator {
        return .{
            .storage = storage,
            .cursor = storage.len,
        };
    }

    fn debugPrint(storage: *Storage) void {
        std.debug.print("{any}\n", .{storage.dense[0..storage.len]});
    }
};

/// NOTE potentially a very large struct, alloc on heap
pub fn Table(comptime Vs: type) type {
    return struct {
        const Self = @This();

        pub const Component = std.meta.FieldEnum(Vs);
        const n_components = std.meta.fields(Component).len;

        fn ComponentType(comptime c: Component) type {
            return std.meta.fields(Vs)[@intFromEnum(c)].type;
        }

        alloc: std.mem.Allocator,
        arena: std.heap.ArenaAllocator, // queries need tempoary memory
        entities: Storage,
        free: Storage,
        components: std.EnumArray(Component, Storage),

        pub fn create(alloc: std.mem.Allocator) !*Self {
            const table = try alloc.create(Self);
            errdefer alloc.destroy(table);
            table.alloc = alloc;
            table.arena = std.heap.ArenaAllocator.init(alloc);
            errdefer table.arena.deinit();

            table.entities = try Storage.init(void, alloc);
            errdefer table.entities.deinit(void);
            table.free = try Storage.init(void, alloc);
            errdefer table.free.deinit(void);
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                table.components.getPtr(c).* = try Storage.init(ComponentType(c), alloc);
                errdefer table.components.getPtr(c).deinit(ComponentType(c));
            }

            var i: u16 = @truncate(@as(u64, @bitCast(std.time.microTimestamp())));
            for (0..65536) |_| {
                i +%= 40507;
                try table.free.add(void, @bitCast(Detail{ .id = i, .version = 0 }), {});
            }

            return table;
        }

        pub fn destroy(table: *Self) void {
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                table.components.getPtr(c).deinit(ComponentType(c));
            }
            table.free.deinit(void);
            table.entities.deinit(void);
            table.arena.deinit();
            table.alloc.destroy(table);
        }

        pub fn spawn(table: *Self) !Entity {
            if (table.free.len == 0) {
                std.debug.assert(table.entities.len == 65536);
                return error.MaxEntities;
            }
            const e = table.free.front();
            table.entities.add(void, e, {}) catch unreachable;
            table.free.del(void, e);
            return e;
        }

        pub fn alive(table: *Self, e: Entity) bool {
            std.debug.assert(e != nil);
            return table.entities.has(e);
        }

        pub fn kill(table: *Self, e: Entity) void {
            std.debug.assert(e != nil);

            if (!table.entities.has(e)) {
                std.debug.assert(table.free.has(e));
                std.log.debug("entity {} killed more than once", .{detail(e)});
                return;
            }

            table.free.add(void, e, {}) catch unreachable;
            inline for (0..n_components) |i| {
                const c: Component = @enumFromInt(i);
                const storage = table.components.getPtr(c);
                if (storage.has(e)) storage.del(ComponentType(c), e);
            }
            table.entities.del(void, e);
        }

        pub fn add(table: *Self, comptime c: Component, e: Entity, val: ComponentType(c)) void {
            if (table.has(c, e)) return;
            return table.components.getPtr(c).add(ComponentType(c), e, val) catch @panic("oom");
        }

        pub fn del(table: *Self, comptime c: Component, e: Entity) void {
            if (!table.has(c, e)) return;
            return table.components.getPtr(c).del(ComponentType(c), e);
        }

        pub fn get(table: *Self, comptime c: Component, e: Entity) ?*ComponentType(c) {
            return table.components.getPtr(c).get(ComponentType(c), e);
        }

        pub fn has(table: *Self, c: Component, e: Entity) bool {
            return table.components.getPtr(c).has(e);
        }

        /// careful
        pub fn data(table: *Self, c: Component) *Storage {
            return table.components.getPtr(c);
        }

        const Query = struct {
            parent: EntityIterator,

            fields: [6]?*Storage,
            // n_fields: usize,

            // there's an unfortunate aspect in this generalization
            // basically, it's very nice for ease of use, composability, etc
            // however, by only providing the entity, we have to look up each component twice
            // which more than doubles the time required

            // it might be possible to store anonymous pointers to the values in query
            // but we'd need to add a getanonymous to get them in next
            // and that's non-trivial since we don't know the type normally

            pub fn next(ctx: *anyopaque) ?Entity {
                const q: *Query = @alignCast(@ptrCast(ctx));
                blk: while (true) {
                    const e = q.parent.next() orelse return null;
                    for (q.fields) |s| {
                        // if (s != null and !s.?.has(e)) continue :blk;
                        if (s == null) break; // it's also possible this branch slows us down
                        if (!s.?.has(e)) continue :blk;
                    }
                    // for (0..q.n_fields) |i| {
                    // if (q.fields[i].?.has(e)) continue :blk;
                    // }
                    return e;
                }
            }

            const vtable = EntityIterator.VTable{
                .next = &next,
            };

            pub fn iter(q: *Query) EntityIterator {
                return .{
                    .ctx = @ptrCast(q),
                    .vtable = &vtable,
                };
            }
        };

        pub fn query(table: *Self, include: []const Component) Query {
            std.debug.assert(include.len > 0);
            std.debug.assert(include.len <= 7);

            var sorted: [7]?*Storage = .{null} ** 7;
            for (include, 0..) |c, i| {
                sorted[i] = table.data(c);
            }
            var unsorted = true;
            while (unsorted) {
                unsorted = false;
                for (1..include.len) |i| {
                    if (sorted[i - 1].?.len > sorted[i].?.len) {
                        const tmp = sorted[i];
                        sorted[i] = sorted[i - 1];
                        sorted[i - 1] = tmp;
                        unsorted = true;
                    }
                }
            }

            // for (sorted[0..include.len]) |s| {
            // std.debug.print("{}\n", .{s.?.len});
            // }

            const parent_iterator = table.arena.allocator()
                .create(Storage.Iterator) catch @panic("oom");
            parent_iterator.* = sorted[0].?.iterator();
            var q = Query{
                .parent = parent_iterator.iter(),
                .fields = .{null} ** 6,
                // .n_fields = include.len - 1,
            };
            if (include.len > 1) @memcpy(q.fields[0 .. include.len - 1], sorted[1..include.len]);
            // std.debug.print("{}\n", .{q});
            return q;
        }
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var rng = std.rand.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    var s = try Storage.init(u32, alloc);
    defer s.deinit(u32);

    var created = std.ArrayList(Entity).init(alloc);
    defer created.deinit();

    for (0..10) |_| {
        const x = rand.int(u8);
        std.debug.print("adding {}\n", .{x});
        if (!s.has(x)) {
            try s.add(u32, x, x);
            try created.append(x);
        }
        s.debugPrint();
    }

    var _it = s.iterator();
    const it = _it.iter();
    while (it.next()) |k| {
        std.debug.print("{}\n", .{k});
    }

    while (created.items.len > 0) {
        const i = rand.intRangeLessThan(usize, 0, created.items.len);
        const x = created.swapRemove(i);

        std.debug.print("deleting {}\n", .{x});
        s.del(u32, x);
        s.debugPrint();
    }
}
