const std = @import("std");

pub fn archetype(
    comptime Base: type,
    comptime components: anytype,
) std.StaticBitSet(std.meta.fields(Base).len) {
    // just a helper for testing archetype building
    const Component = std.meta.FieldEnum(Base);
    const n = std.meta.fields(Component).len;

    var bitset = std.StaticBitSet(n).initEmpty();
    inline for (components) |c| {
        bitset.set(@intFromEnum(@as(Component, c)));
    }
    return bitset;
}

fn ArchetypeImpl(
    comptime Base: type,
    comptime spec: std.StaticBitSet(std.meta.fields(Base).len),
) type {
    return typedef: {
        var fields: [spec.count()]std.builtin.Type.StructField = undefined;
        const info = @typeInfo(Base);

        var j: usize = 0;
        inline for (info.Struct.fields, 0..) |field, i| {
            if (spec.isSet(i)) {
                fields[j] = field;
                j += 1;
            }
        }

        break :typedef @Type(std.builtin.Type{ .Struct = std.builtin.Type.Struct{
            .layout = .Auto,
            .is_tuple = false,
            .backing_integer = null,
            .fields = &fields,
            .decls = &.{},
        } });
    };
}

pub fn Archetype(comptime Base: type, comptime components: anytype) type {
    return ArchetypeImpl(Base, archetype(Base, components));
}

const T1 = struct {
    int: i32,
    float: f32,
};
test "archetyping" {
    // just make sure we dont' get compile errors

    var just_int: Archetype(T1, .{.int}) = undefined;
    try std.testing.expect(std.meta.fields(@TypeOf(just_int)).len == 1);

    var just_float: Archetype(T1, .{.float}) = undefined;
    try std.testing.expect(std.meta.fields(@TypeOf(just_float)).len == 1);

    var both: Archetype(T1, .{ .int, .float }) = undefined;
    try std.testing.expect(std.meta.fields(@TypeOf(both)).len == 2);

    var neither: Archetype(T1, .{}) = undefined;
    try std.testing.expect(std.meta.fields(@TypeOf(neither)).len == 0);

    // but this is a compile error ;)
    // var yo: Archetype(T1, enumBits(T1, .{.dawg})) = undefined;
}

pub const Entity = u32;

fn expand(comptime T: type, arr: *[]T, min: usize, alloc: std.mem.Allocator) void {
    if (T == void) return;
    const len = std.math.ceilPowerOfTwoAssert(usize, min);
    const new = alloc.alloc(T, len) catch @panic("out of memory");
    std.mem.copy(T, new, arr.*);
    arr.* = new;
}

pub fn State(comptime Base: type) type {
    const Component = std.meta.FieldEnum(Base);
    const n_components = std.meta.fields(Component).len;
    const Spec = std.StaticBitSet(n_components);

    const VOID = Spec.initEmpty();

    return struct {
        const Self = @This();

        const Page = struct {
            spec: Spec,
            entities: [*]Entity,
            data: usize, // NOTE a type erased pointer to [*]Archetype(Base, spec)
            len: usize, // number of used elements above
            cap: usize, // amount of space available above
        };

        alloc: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        prev: ?*Self,

        pages: std.MultiArrayList(Page) = .{},

        next_entity: Entity = 0,

        pub fn init(alloc: std.mem.Allocator, chain_len: usize) *Self {
            var state = alloc.create(Self) catch @panic("out of memory");
            state.* = Self{
                .alloc = alloc,
                .arena = std.heap.ArenaAllocator.init(alloc),
                .prev = null,
            };

            for (1..chain_len) |_| {
                var next = alloc.create(Self) catch @panic("out of memory");
                next.* = Self{
                    .alloc = alloc,
                    .arena = std.heap.ArenaAllocator.init(alloc),
                    .prev = state,
                };
                state = next;
            }

            return state;
        }

        pub fn deinit(state: *Self) void {
            if (state.prev) |prev| {
                prev.deinit();
            }
            state.arena.deinit();
            state.alloc.destroy(state);
        }

        pub fn create(state: *Self, comptime spec: Spec) Entity {
            const aa = state.arena.allocator();
            const entity = state.next_entity;
            state.next_entity += 1;

            if (state.page(spec)) |i| {
                // make sure we have enough space
                if (state.pages.items(.len)[i] == state.pages.items(.cap)[i]) {
                    const cap = state.pages.items(.cap)[i] * 2;
                    var p = Page{
                        .spec = spec,
                        .entities = (aa.alloc(Entity, cap) catch @panic("out of memory")).ptr,
                        .data = if (comptime spec.eql(VOID)) 0 else @intFromPtr((aa.alloc(ArchetypeImpl(Base, spec), cap) catch @panic("out of memory")).ptr),
                        .len = state.pages.items(.len)[i],
                        .cap = cap,
                    };
                    state.pages.set(i, p);
                }

                const j = state.pages.items(.len)[i];
                state.pages.items(.len)[i] += 1;
                state.pages.items(.entities)[i][j] = entity;
            } else {
                // add a new page fist
                const cap = 16;
                var p = Page{
                    .spec = spec,
                    .entities = (aa.alloc(Entity, cap) catch @panic("out of memory")).ptr,
                    .data = if (comptime spec.eql(VOID)) 0 else @intFromPtr((aa.alloc(ArchetypeImpl(Base, spec), cap) catch @panic("out of memory")).ptr),
                    // .data = @intFromPtr((aa.alloc(ArchetypeImpl(Base, spec), cap) catch @panic("out of memory")).ptr),
                    .len = 1,
                    .cap = 16,
                };
                p.entities[0] = entity;
                state.pages.append(aa, p) catch @panic("out of memory");
            }

            return entity;
        }

        // CONSIDER: A function like this isn't possible unless we can know at compile time what archetype an entity has
        //           (we cannot) hence I think if we want to use the type-system for the archetypes
        //           we have to be extremely careful about the design s.t. we can compile-time know the archetype of each page
        // pub fn get(state: *Self, entity: Entity) *ArchetypeImpl(Base, k)??? {}

        // index into State.data that matches spec
        fn page(state: *Self, spec: Spec) ?usize {
            var i: usize = 0;
            for (state.pages.items(.spec)) |s| {
                if (s.eql(spec)) {
                    return i;
                }
                i += 1;
            }
            return null;
        }
    };
}

const T2 = struct {
    int: i32,
    float: f32,
};
test "state" {
    var state = State(T2).init(std.testing.allocator, 2);
    defer state.deinit();

    const INT = comptime archetype(T2, .{.int});
    const VOID = comptime archetype(T2, .{});

    const e0 = state.create(INT);
    std.debug.print("{}\n", .{e0});
    const e1 = state.create(VOID);
    std.debug.print("{}\n", .{e1});
}
