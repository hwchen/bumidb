const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub fn main() anyerror!void {
    var db = try RocksDb.open("data");
    defer db.close();
    std.debug.print("Opened db\n", .{});

    try db.set("test_key", "test_value");
    const test_val = try db.get("test_key");
    std.debug.print("get value: {s}\n", .{test_val});
}

pub const RocksDb = struct {
    db: *rdb.rocksdb_t,

    const Self = @This();

    pub fn open(dir: []const u8) !Self {
        var options = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);

        var err: ?[*:0]u8 = null;
        var db = rdb.rocksdb_open(options, dir.ptr, &err);
        if (err) |e| {
            std.log.err("Error: {s}", .{e});
            return error.RocksDbOpen;
        }

        return .{ .db = db orelse return error.RocksDbFail };
    }

    pub fn close(self: Self) void {
        rdb.rocksdb_close(self.db);
    }

    pub fn get(self: Self, key: [:0]const u8) ![]const u8 {
        const read_options = rdb.rocksdb_readoptions_create();
        var err: ?[*:0]u8 = null;
        var val_len: usize = 0;
        const val = rdb.rocksdb_get(
            self.db,
            read_options,
            key.ptr,
            key.len,
            &val_len,
            &err,
        );
        if (err) |e| {
            std.log.err("Error: {s}", .{e});
            return error.RocksDbGet;
        }

        return val[0..val_len];
    }

    pub fn set(self: Self, key: [:0]const u8, value: [:0]const u8) !void {
        const write_options = rdb.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_put(
            self.db,
            write_options,
            key.ptr,
            key.len,
            value.ptr,
            value.len,
            &err,
        );
        if (err) |e| {
            std.log.err("Error: {s}", .{e});
            return error.RocksDbSet;
        }
    }
};
