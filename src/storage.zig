// TODO projections ans joins

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
    column_names: []const []const u8,
    column_kinds: []Value.Kind,
};

pub const Row = struct {
    bytes: []const u8,

    // passed in from Table
    column_names: []const []const u8,
    column_kinds: []const Value.Kind,

    /// Performs a linear scan to get value at column[idx] using iter
    /// Getting multiple columns w/out allocating should manually use
    /// iter.
    pub fn get(self: Row, target_idx: []const usize) ?Value {
        var it = self.iter();
        var idx: usize = 0;

        // skip
        while (target_idx < idx) {
            it.next();
        }
        return it.next();
    }

    pub fn iter(self: Row) Iter {
        return Iter{ .row = self };
    }

    pub const Iter = struct {
        row: Row,
        bytes_idx: usize = 0,
        column_idx: usize = 0,

        pub fn next(self: *Iter) ?Value {
            const value_len = self.row.bytes[self.bytes_idx];
            const value_bytes = self.row.bytes[self.bytes_idx + 1 .. self.bytes_idx + value_len + 1];
            const kind = self.row.column_kinds[self.column_idx];

            self.bytes_idx += value_len + 1;
            self.column_idx += 1;

            return Value{
                .kind = kind,
                .bytes = value_bytes,
            };
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

    const column_names = [_][]const u8{ "is_test", "num_test", "text_test" };
    const column_kinds = [_]Value.Kind{ .boolean, .integer, .text };

    // deserialize row 1
    // values: (true, 0, 'foo')
    {
        const bytes = [_]u8{ 1, 1, 1, 0, 3, 'f', 'o', 'o' };
        const row = Row{
            .bytes = &bytes,
            .column_names = &column_names,
            .column_kinds = &column_kinds,
        };
        var row_iter = row.iter();
        try std.testing.expectEqual(try row_iter.next().?.as(bool), true);
        try std.testing.expectEqual(try row_iter.next().?.as(u8), 0);
        try std.testing.expectEqualStrings(try row_iter.next().?.as([]const u8), "foo");
    }
    // deserialize row 2
    // values: (false, 1, 'bar)
    {
        const bytes = [_]u8{ 1, 0, 1, 1, 3, 'b', 'a', 'r' };
        const row = Row{
            .bytes = &bytes,
            .column_names = &column_names,
            .column_kinds = &column_kinds,
        };
        var row_iter = row.iter();
        try std.testing.expectEqual(try row_iter.next().?.as(bool), false);
        try std.testing.expectEqual(try row_iter.next().?.as(u8), 1);
        try std.testing.expectEqualStrings(try row_iter.next().?.as([]const u8), "bar");
    }
}
