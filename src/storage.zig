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
    column_names: [][]const u8,
    column_kinds: [][]Value.Kind,
};

pub const Row = struct {
    bytes: []const u8,

    // passed in from Table
    column_names: [][]const u8,
    column_kinds: []Value.Kind,

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
        return Iter{ .bytes = self.bytes };
    }

    pub const Iter = struct {
        row: Row,
        bytes_idx: usize = 0,
        column_idx: usize = 0,

        pub fn next(self: Iter) ?Value {
            const value_len = self.row.bytes[self.bytes_idx];
            const value_bytes = self.row.bytes[self.bytes_idx + 1 .. value_len];
            const kind = self.row.column_kinds[self.column_idx];

            self.bytes_idx = value_len + 1;
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

    pub const Kind = enum {
        boolean,
        integer, //u64
        text,
    };
};
