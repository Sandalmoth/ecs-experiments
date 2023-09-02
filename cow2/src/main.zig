const std = @import("std");

// I think the MemoryPool approach of the previous cow was a mistake
// as it made reference counting and handle keeping unneccessarily difficult
// so let's try a sparse-set storage instead

pub const Handle = u32;
pub const HandleDetail = packed struct {
    generation: u16,
    slot: u16,
};

pub fn newHandle(handle: Handle, slot: u16) Handle {
    var detail: HandleDetail = @bitCast(handle);
    detail.slot = slot;
    detail.generation += 1;
    return @bitCast(detail);
}

pub fn isRecent(new: Handle, old: Handle) bool {
    const new_detail: HandleDetail = @bitCast(new);
    const old_detail: HandleDetail = @bitCast(old);
    std.debug.assert(new_detail.generation <= old_detail.generation);
    return new_detail.generation == old_detail.generation;
}

pub fn isTombstone(handle: Handle) bool {
    const detail: HandleDetail = @bitCast(handle);
    return detail.slot == std.math.maxInt(u16);
}

pub fn tombstoned(handle: Handle) Handle {
    var detail: HandleDetail = @bitCast(handle);
    detail.slot = std.max.maxInt(u16);
    return @bitCast(detail);
}

pub fn State(comptime Item: type) type {
    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,

        // handles and sparse are owned by each individual State object
        // as they provide an access point to all the entities at some point in time
        handles: []Handle,
        sparse: []u16,
        n_entities: u16 = 0,
        // whereas dense aond count share ownership from the entire chain of States
        // as they are the true storage of all the entity data
        dense: []Item,
        count: []u32, // unsure what size is good, this is def. overkill
        n_items: u16 = 0,

        prev: ?*Self,

        pub fn init(alloc: std.mem.Allocator) !*Self {
            var prev = try alloc.create(Self);
            errdefer alloc.destroy(prev);
            prev.* = Self{
                .alloc = alloc,
                .handles = undefined,
                .sparse = undefined,
                .dense = undefined,
                .count = undefined,
                .prev = null,
            };

            // we initialize the handles to 0
            // because then we can just do a generational increment
            // without keeping track of if they have been used before
            prev.handles = try alloc.alloc(Handle, 16);
            for (prev.handles) |*handle| handle.* = 0;
            errdefer alloc.free(prev.handles);

            prev.sparse = try alloc.alloc(u16, 16);
            errdefer alloc.free(prev.sparse);

            prev.dense = try alloc.alloc(Item, 16);
            errdefer alloc.free(prev.dense);

            prev.count = try alloc.alloc(u32, 16);
            errdefer alloc.free(prev.count);

            var state = try alloc.create(Self);
            errdefer alloc.destroy(prev);
            state.* = Self{
                .alloc = alloc,
                .handles = undefined,
                .sparse = undefined,
                .dense = prev.dense,
                .count = prev.count,
                .prev = prev,
            };

            state.handles = try alloc.alloc(Handle, 16);
            for (state.handles) |*handle| handle.* = 0;
            errdefer alloc.free(state.handles);

            state.sparse = try alloc.alloc(u16, 16);
            errdefer alloc.free(state.sparse);

            return state;
        }

        /// the provided state should be the head (most recent)
        /// but it will deinit the entire chain
        pub fn deinit(state: *Self) void {
            state.alloc.free(state.dense);
            state.alloc.free(state.count);

            var walk = state;
            while (true) {
                const next = walk.prev;

                walk.alloc.free(walk.handles);
                walk.alloc.free(walk.sparse);
                walk.alloc.destroy(walk);

                if (next == null) {
                    break;
                }
                walk = next.?;
            }
        }

        pub fn create(state: *Self) Handle {
            // TODO array appends that expand storage

            const slot = state.n_entities; // where is the entity in the sparse array?
            const handle = newHandle(state.handles[slot], slot); // what is the handle (generation increment)
            const pos = state.n_items; // where is the storage of our entity in the dense array

            state.dense[pos] = undefined; // maybe should require an initial value?
            state.count[pos] = 1;
            state.handles[slot] = handle;
            state.sparse[slot] = pos;
            state.n_entities += 1;
            state.n_items += 1;

            return handle;
        }

        pub fn destroy(state: *Self, handle: Handle) void {
            if (!isRecent(handle, state.handles[handle.slot])) {
                std.log.warn(
                    "called destroy with old handle {}, current is {} (NOOP)",
                    .{ handle, state.handles[handle.slot] },
                );
                return;
            }

            const detail: HandleDetail = @bitCast(handle);
            const pos = state.sparse[detail.slot];

            std.debug.assert(state.count[pos] > 0);
            state.count[pos] -= 1;

            if (state.count == 0) {
                // destroy the entity
                // HOWEVER
                // this is catastrophically bad
                // as we now have to walk the entire state chain
                // and perform the same "swap with last"
                // in every single instance.

                // it seems pointer (or some kind of reference) stability is required
                // (as it pertains to the dense storage)
                // so that historical references never have to be updated
                // otherwise, we'll be stuck with a lot of operations having to act on the whole chain
                // so this current implementation is not viable
            }

            _ = state;
        }

        /// effectively creates a snapshot of the current state
        /// then returns a new state, whith the snapshot as .prev
        /// also destroys states more than 16 steps old
        pub fn step(state: *Self) *Self {
            _ = state;
        }
    };
}

const _E = struct {
    pos: @Vector(4, f32),
    hp: u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var state = try State(_E).init(alloc);
    defer state.deinit();

    const a = state.create();
    std.debug.print("{}\n", .{a});
    std.debug.print("{b}\n", .{a});
}

test "init deinit" {
    var state = try State(_E).init(std.testing.allocator);
    defer state.deinit();
}
