const std = @import("std");

// I think the MemoryPool approach of the previous cow was a mistake
// as it made reference counting and handle keeping unneccessarily difficult
// so let's try a sparse-set storage instead

pub const Handle = u32;
pub const HandleDetail = packed struct {
    generation: u16,
    slot: u16,
};

pub fn isTombstone(handle: Handle) bool {
    const detail: HandleDetail = @bitCast(handle);
    return detail.generation == std.math.maxInt(u16);
}

pub fn tombstoned(handle: Handle) Handle {
    var detail: HandleDetail = @bitCast(handle);
    detail.generation = std.max.maxInt(u16);
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
        // whereas dense aond count share ownership from the entire chain of States
        // as they are the true storage of all the entity data
        dense: []Item,
        count: []u32, // unsure what size is good, this is def. overkill

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

            prev.handles = try alloc.alloc(Handle, 16);
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
}
