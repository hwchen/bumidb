// TODO projections and joins
//
// # Row
//
// Layout is header then the values, all sequentially. Header is indexes of value slices,
// from the header start (not header end).
//
// This layout is better than encoding len next to value, so you don't have to iterate through
// the entire row to get a projection.
//
// This layout is worse than working in blocks from top/bottom, since adding columns requires
// shifting the entire slice. But I probably won't be implementing add columns anyways.

const std = @import("std");
const Allocator = std.Allocator;
const RocksDb = @import("rocksdb.zig").RocksDb;

pub const Storage = struct {
    rdb: RocksDb,
    alloc: Allocator,

    const tableMetadataPrefix = "_tbl_";

    pub fn init(db_path: [:0]const u8, alloc: Allocator) !Storage {
        return .{
            .rdb = try RocksDb.open(db_path, .{}),
            .alloc = alloc,
        };
    }
    pub fn deinit(self: Storage) void {
        self.rdb.close();
    }
    pub fn createTable() void {}
    pub fn tableMetadata() void {}
    pub fn scanTable() void {}
};

pub const TableMetadata = struct {
    id: u32,
    name: []const u8,
    columns_metadata: []const ColumnMetadata,
};

pub const ColumnMetadata = struct {
    name: []const u8,
    kind: Value.Kind,
};

pub const Row = struct {
    bytes: []const u8,

    // passed in from Table
    columns_metadata: []const ColumnMetadata,

    pub fn get(self: Row, target_idx: usize) ?Value {
        if (target_idx >= self.columns_metadata.len) {
            return null;
        }

        // index into bytes "header" to get the index of the value.
        const value_start = self.bytes[target_idx];
        const value_bytes = blk: {
            if (target_idx == self.columns_metadata.len - 1) {
                break :blk self.bytes[value_start..];
            } else {
                const value_end = self.bytes[target_idx + 1];
                break :blk self.bytes[value_start..value_end];
            }
        };

        return Value{
            .bytes = value_bytes,
            .kind = self.columns_metadata[target_idx].kind,
        };
    }

    pub fn get_by_name(self: Row, target_name: []const u8) ?Value {
        const col_idx = blk: {
            for (self.columns_metadata) |col_meta, i| {
                if (std.mem.eql(u8, col_meta.name, target_name)) {
                    break :blk i;
                }
            }
            return null;
        };
        return self.get(col_idx);
    }

    pub fn iter(self: Row) Iter {
        return Iter{ .row = self };
    }

    pub const Iter = struct {
        row: Row,
        column_idx: usize = 0,

        pub fn next(self: *Iter) ?Value {
            const res = self.row.get(self.column_idx);
            self.column_idx += 1;
            return res;
        }
    };
};

pub const Value = struct {
    kind: Kind,
    bytes: []const u8,

    // TODO null
    pub const Kind = enum {
        boolean,
        integer, // u8 for now
        text,
    };

    pub fn as(self: Value, comptime T: type) !T {
        return switch (self.kind) {
            .boolean => if (T == bool) self.bytes[0] == 1 else error.ValueNotBoolean,
            .integer => if (T == u8) self.bytes[0] else error.ValueNotInteger,
            .text => if (T == []const u8) self.bytes[0..] else error.ValueNotText,
        };
    }
};

test "row deserialize" {
    // hand-construct bytes for row (all cols nullable):
    // - header: is_test, num_test, text_test
    // - kinds: bool, integer, text

    const columns_metadata = [_]ColumnMetadata{
        ColumnMetadata{
            .name = "is_test",
            .kind = .boolean,
        },
        ColumnMetadata{
            .name = "num_test",
            .kind = .integer,
        },
        ColumnMetadata{
            .name = "text_test",
            .kind = .text,
        },
    };

    // deserialize row 1
    // values: (true, 0, 'foo')
    {
        const bytes = [_]u8{ 3, 4, 5, 1, 0, 'f', 'o', 'o' };
        const row = Row{
            .bytes = &bytes,
            .columns_metadata = &columns_metadata,
        };

        // check iter
        var row_iter = row.iter();
        try std.testing.expectEqual(try row_iter.next().?.as(bool), true);
        try std.testing.expectEqual(try row_iter.next().?.as(u8), 0);
        try std.testing.expectEqualStrings(try row_iter.next().?.as([]const u8), "foo");

        // check get
        try std.testing.expectEqual(try row.get(0).?.as(bool), true);
        try std.testing.expectEqual(try row.get_by_name("is_test").?.as(bool), true);
        try std.testing.expectEqual(try row.get(1).?.as(u8), 0);
        try std.testing.expectEqual(try row.get_by_name("num_test").?.as(u8), 0);
        try std.testing.expectEqualStrings(try row.get(2).?.as([]const u8), "foo");
        try std.testing.expectEqualStrings(try row.get_by_name("text_test").?.as([]const u8), "foo");
    }
    // deserialize row 2
    // values: (false, 1, 'bar)
    {
        const bytes = [_]u8{ 3, 4, 5, 0, 1, 'b', 'a', 'r' };
        const row = Row{
            .bytes = &bytes,
            .columns_metadata = &columns_metadata,
        };
        // check iter
        var row_iter = row.iter();
        try std.testing.expectEqual(try row_iter.next().?.as(bool), false);
        try std.testing.expectEqual(try row_iter.next().?.as(u8), 1);
        try std.testing.expectEqualStrings(try row_iter.next().?.as([]const u8), "bar");

        // check get
        try std.testing.expectEqual(try row.get(0).?.as(bool), false);
        try std.testing.expectEqual(try row.get_by_name("is_test").?.as(bool), false);
        try std.testing.expectEqual(try row.get(1).?.as(u8), 1);
        try std.testing.expectEqual(try row.get_by_name("num_test").?.as(u8), 1);
        try std.testing.expectEqualStrings(try row.get(2).?.as([]const u8), "bar");
        try std.testing.expectEqualStrings(try row.get_by_name("text_test").?.as([]const u8), "bar");
    }
}

test "row serialize" {
    // Row.fromValues, takes a slice of Values. Then you can write the bytes field.
    // Actually it'sa bit awkward because Row holds a slice, not ArrayList, and I don't want Row to
    // own its slice of bytes. So, I should just do a simple fn of writing a slice of Values to a
    // resusable buffer (passed in as arg) and then RocksDb.set.
    //
    // serializeValuesToRowBytes(values: []const Value, buf: *ArrayList(u8), db: RocksDb) !void {}
    //
    // Row has to hold state, so it still makes more sense as a struct than as a fn, yes?
    //
    // Anyways, try this first, think later about whether the serialization/deserialization should
    // look more parallel
}
