const std = @import("std");

const Entity = packed struct {
    sparse_index: u20,
    version: u12,

    fn reserved() Entity {
        return .{
            .sparse_index = 0xFFFFF,
            .version = 0xFFF,
        };
    }
};

comptime {
    std.debug.assert(@sizeOf(Entity) == 4);
}

fn Storage(comptime T: type) type {
    const DENSE_PAGE_SIZE = (16384 - 8) / (4 + 2 + @sizeOf(T));
    const DensePage = struct {
        next: ?*@This() = null,
        len: u16 = 0,
        burned: u16 = 0,
        entities: [DENSE_PAGE_SIZE]Entity = .{Entity.reserved()} ** DENSE_PAGE_SIZE,
        skip: [DENSE_PAGE_SIZE]u16 = .{1} ** DENSE_PAGE_SIZE,
        data: [DENSE_PAGE_SIZE]T = undefined,
    };
    std.debug.assert(@sizeOf(DensePage) >= 8192);
    std.debug.assert(@sizeOf(DensePage) <= 16384);

    const Slot = struct {
        entity: Entity,
        dense_page: *DensePage,
        dense_index: u16,
        _placeholder: u16,
    };
    std.debug.assert(@sizeOf(Slot) <= 16);

    const SparsePage = struct {
        slots: [1024]Slot = .{Slot{
            .entity = Entity.reserved(),
            .dense_page = undefined,
            .dense_index = undefined,
            ._placeholder = undefined,
        }} ** 1024,
    };
    std.debug.assert(@sizeOf(SparsePage) == 16384);

    const Iterator = struct {
        const KV = struct { entity: Entity, ptr: *T };

        dense_page: ?*DensePage,
        cursor: usize,

        pub fn next(iter: *@This()) ?KV {
            if (iter.dense_page) |page| {
                while (iter.cursor < DENSE_PAGE_SIZE and page.skip[iter.cursor] != 0) {
                    iter.cursor += 1;
                }
                if (iter.cursor == DENSE_PAGE_SIZE) {
                    iter.dense_page = page.next;
                    iter.cursor = 0;
                    return iter.next();
                }
                std.debug.assert(page.skip[iter.cursor] == 0);
                iter.cursor += 1;
                return .{
                    .entity = page.entities[iter.cursor - 1],
                    .ptr = &page.data[iter.cursor - 1],
                };
            }
            return null;
        }
    };

    return struct {
        const S = @This();

        alloc: std.mem.Allocator,
        sparse: [1024]?*SparsePage = .{null} ** 1024,
        dense: ?*DensePage = null,
        compaction_counter: usize = 0,

        fn init(alloc: std.mem.Allocator) S {
            std.debug.print("{s} {} {}\n", .{ @typeName(T), DENSE_PAGE_SIZE, @sizeOf(DensePage) });
            return .{
                .alloc = alloc,
            };
        }

        fn deinit(storage: *S) void {
            for (storage.sparse) |p| {
                if (p != null) storage.alloc.destroy(p.?);
            }

            var dense = storage.dense;
            while (dense != null) {
                const next = dense.?.next;
                storage.alloc.destroy(dense.?);
                dense = next;
            }

            storage.* = undefined;
        }

        fn iterator(storage: *S) Iterator {
            return .{
                .dense_page = storage.dense,
                .cursor = 0,
            };
        }

        /// entity must not be in storage
        fn add(storage: *S, entity: Entity, value: T) void {
            std.debug.assert(!storage.contains(entity));

            const i = entity.sparse_index >> 10;
            if (storage.sparse[i] == null) {
                storage.sparse[i] = storage.alloc.create(SparsePage) catch @panic("out of memory");
                storage.sparse[i].?.* = .{};
            }
            const j = entity.sparse_index & 1024;
            const s = &storage.sparse[i].?.slots[j];

            if (storage.dense == null) {
                storage.dense = storage.alloc.create(DensePage) catch @panic("out of memory");
                storage.dense.?.* = .{};
            }
            var dense = storage.dense.?;
            while (dense.len == DENSE_PAGE_SIZE) {
                if (dense.next == null) {
                    dense.next = storage.alloc.create(DensePage) catch @panic("out of memory");
                    dense.next.?.* = .{};
                }
                dense = dense.next.?;
            }

            const k = dense.len;
            dense.entities[k] = entity;
            dense.skip[k] = 0;
            dense.data[k] = value;
            dense.len += 1;

            s.entity = entity;
            s.dense_page = dense;
            s.dense_index = k;
        }

        /// entity must be in storage
        fn getPtr(storage: *S, entity: Entity) *T {
            std.debug.assert(storage.contains(entity));

            const s = storage.slot(entity).?;
            return &s.dense_page.data[s.dense_index];
        }

        /// entity must be in storage
        fn del(storage: *S, entity: Entity) void {
            std.debug.assert(storage.contains(entity));

            const i = entity.sparse_index >> 10;
            const j = entity.sparse_index & 1024;
            const s = &storage.sparse[i].?.slots[j];
            s.entity = Entity.reserved();
            s.dense_page.skip[s.dense_index] = 1;
            s.dense_page.burned += 1;
        }

        fn compact(storage: *S) void {
            var new_dense = storage.alloc.create(DensePage) catch @panic("out of memory");
            new_dense.* = .{};
            var cc = (storage.compaction_counter + 1) % (1024 * 1024);
            while (cc != storage.compaction_counter and new_dense.len < DENSE_PAGE_SIZE) {
                if (storage.sparse[cc >> 10] == null) {
                    cc = (cc + 1024) % 1024;
                } else {
                    const s = &storage.sparse[cc >> 10].?.slots[cc & 1024];
                    std.debug.print("did a copy {} {} {}\n", .{ cc, s.entity, s.dense_index });
                    if (std.meta.eql(s.entity, Entity.reserved())) {
                        cc = (cc + 1) % (1024 * 1024);
                        continue;
                    }

                    const k = new_dense.len;
                    std.debug.assert(std.meta.eql(s.entity, s.dense_page.entities[s.dense_index]));
                    new_dense.entities[k] = s.dense_page.entities[s.dense_index];
                    new_dense.skip[k] = s.dense_page.skip[s.dense_index];
                    new_dense.data[k] = s.dense_page.data[s.dense_index];
                    new_dense.len += 1;

                    s.dense_page = new_dense;
                    s.dense_index = k;
                    cc = (cc + 1) % (1024 * 1024);
                }
            }
            storage.compaction_counter = cc;
            new_dense.next = storage.dense.?;
            storage.dense = new_dense;

            // FIXME
            // // remove all burnt pages
            // var dense = storage.dense;
            // var parent = &storage.dense;
            // while (dense != null) {
            //     const next = dense.?.next;
            //     if (dense.?.burned == DENSE_PAGE_SIZE) {
            //         parent.* = next;
            //         storage.alloc.destroy(dense.?);
            //         std.debug.print("destroyed a page\n", .{});
            //         dense = next;
            //     } else {
            //         parent = &dense.?.next;
            //         dense = next;
            //     }
            // }
        }

        inline fn contains(storage: *S, entity: Entity) bool {
            const s = storage.slot(entity);
            return s != null and std.meta.eql(s.?.entity, entity);
        }

        inline fn slot(storage: *S, entity: Entity) ?Slot {
            const i = entity.sparse_index >> 10;
            if (storage.sparse[i] == null) return null;

            const j = entity.sparse_index & 1024;
            return storage.sparse[i].?.slots[j];
        }
    };
}

test "scratch" {
    std.debug.print("\n", .{});

    var s = Storage(f32).init(std.testing.allocator);
    defer s.deinit();

    s.add(.{ .sparse_index = 0, .version = 0 }, 12.34);
    try std.testing.expect(s.contains(.{ .sparse_index = 0, .version = 0 }));
    try std.testing.expect(!s.contains(.{ .sparse_index = 0, .version = 1 }));
    try std.testing.expect(!s.contains(.{ .sparse_index = 1, .version = 0 }));
    try std.testing.expect(!s.contains(.{ .sparse_index = 1, .version = 1 }));
    try std.testing.expectEqual(12.34, s.getPtr(.{ .sparse_index = 0, .version = 0 }).*);
    const p0 = s.getPtr(.{ .sparse_index = 0, .version = 0 });
    s.del(.{ .sparse_index = 0, .version = 0 });
    try std.testing.expect(!s.contains(.{ .sparse_index = 0, .version = 0 }));
    s.add(.{ .sparse_index = 0, .version = 0 }, 23.45);
    const p1 = s.getPtr(.{ .sparse_index = 0, .version = 0 });
    try std.testing.expectEqual(23.45, s.getPtr(.{ .sparse_index = 0, .version = 0 }).*);
    try std.testing.expect(p0 != p1);
    s.compact();
    const p2 = s.getPtr(.{ .sparse_index = 0, .version = 0 });
    try std.testing.expectEqual(23.45, s.getPtr(.{ .sparse_index = 0, .version = 0 }).*);
    try std.testing.expect(p1 != p2);

    var iter = s.iterator();
    var i: usize = 0;
    while (iter.next()) |kv| {
        i += 1;
        std.debug.print("{} {}\n", .{ kv.entity, kv.ptr.* });
    }
    try std.testing.expectEqual(1, i);
}
