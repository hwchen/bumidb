// TODO
// - projections and joins
// - consolidate serialize/deserialize test to round-trip.

//! # Row
//!
//! Layout is header then the values, all sequentially. Header is indexes of value slices,
//! from the header start (not header end).
//!
//! This layout is better than encoding len next to value, so you don't have to iterate through
//! the entire row to get a projection.
//!
//! This layout is worse than working in blocks from top/bottom, since adding columns requires
//! shifting the entire slice. But I probably won't be implementing add columns anyways.
//!
//! To keep things simple, Rows only work when 256 bytes or less (addressable by a u8 index)

const std = @import("std");
const Allocator = std.Allocator;
const ArrayList = std.ArrayList;
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

    /// Not for general use, should prefer `get`
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

// Does it make more sense to have it as a fn here or as a method on Row somehow? Doesn't really matter,
// can change later if necessary.
pub fn serializeValuesToRowBytes(values: []const Value, buf: *ArrayList(u8)) !void {
    buf.clearRetainingCapacity();

    // write header
    // only allow indexes to be u8 for now, to simplify things
    var value_bytes_idx = @intCast(u8, values.len);
    for (values) |value| {
        try buf.append(value_bytes_idx);
        value_bytes_idx += @intCast(u8, value.bytes.len);
    }

    // write values
    for (values) |value| {
        try buf.appendSlice(value.bytes);
    }
}

/// Originally thought to input a slice of Values and map that onto the buffer. However, using a slice of
/// Values as an intermediate representation would require an allocation step since each Value would need
/// an allocated slice.
///
/// However, allocation is more straightforward if we can manage to just allocate for the output buffer.
///
/// For the input, we'll read directly from src (and probably transferred from Token). This could be named
/// a `Value` as we'll need to know the Kind and bytes, but we dont need to do so explicitly.
///
/// Since I don't want to take in a list of Tokens explicitly here, we just need to know the number of cols
/// in the row, to reserve that space for the header. (They can be zeroed, w/ 0 representing null). (Taking
/// Tokens as an input is only slightly more convenient for logic, but creates more coupling; just passing
/// slices, kinds, and header_len is more "fundamental", and perhaps more flexible (it is easier to test)).
/// I guess it's flexible enough to serialize from Tokens or Values.
///
/// Also, curious what this control is called; the caller controls each write call, instead of just passing
/// in a list and letting the method take care of it. This way is more manual, and I think I've seen it in
/// more low level code.
const RowToBytes = struct {
    /// Allocated outside to allow reuse across rows
    buf: *ArrayList(u8),
    /// u8 for now, not allowing rows longer than 256
    value_bytes_idx: u8,

    header_len: u8,
    col_idx: u8,

    const Self = @This();

    fn init(header_len: u8, buf: *ArrayList(u8)) !Self {
        buf.clearRetainingCapacity();
        var i: usize = 0;
        while (i < header_len) : (i += 1) {
            try buf.append(0);
        }
        return .{
            .buf = buf,
            .value_bytes_idx = header_len,
            .header_len = header_len,
            .col_idx = 0,
        };
    }

    // TODO should the bytes be from the src? Then conversion needs to take place w/in this method
    fn write(self: *Self, kind: ?Value.Kind, bytes: []const u8) !void {
        _ = kind;
        if (self.col_idx >= self.header_len) {
            return error.SerializeTooManyWrites;
        }
        self.buf.items[self.col_idx] = self.value_bytes_idx;

        self.value_bytes_idx += @intCast(u8, bytes.len);
        self.col_idx += 1;

        try self.buf.appendSlice(bytes);
    }
};

test "row serialize" {
    var buf = ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    const values = [_]Value{
        .{
            .kind = .boolean,
            .bytes = &[_]u8{1}, // true
        },
        .{
            .kind = .integer,
            .bytes = &[_]u8{0}, // 0
        },
        .{
            .kind = .text,
            .bytes = "foo",
        },
    };

    var ser = try RowToBytes.init(3, &buf);
    for (values) |value| {
        try ser.write(null, value.bytes);
    }

    try std.testing.expectEqualSlices(u8, buf.items, &[_]u8{ 3, 4, 5, 1, 0, 'f', 'o', 'o' });

    try std.testing.expectError(error.SerializeTooManyWrites, ser.write(null, ""));
}
