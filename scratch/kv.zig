// TODO Should rocksdb options be created/destroyed on each method get/set call?
// Or can they be initialized w/ the rocksdb struct.

const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));
const m = std.heap.c_allocator;

pub fn main() anyerror!void {
    var db = try RocksDb.open("/tmp/bumidb");
    defer db.close();
    std.debug.print("Opened db\n", .{});

    try db.set("test_key", "test_value");
    const test_val = try db.get("test_key");
    defer RocksDb.deinit_string(test_val);
    std.debug.print("get value: {s}\n", .{test_val});
}

pub const RocksDb = struct {
    db: *rdb.rocksdb_t,
    read_options: ?*rdb.rocksdb_readoptions_t,
    write_options: ?*rdb.rocksdb_writeoptions_t,

    const Self = @This();

    /// A string allocated by rocksdb. Must be freed by caller.
    const String = []const u8;

    pub fn deinit_string(str: String) void {
        m.free(str);
    }

    pub fn open(dir: []const u8) !Self {
        const options = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        defer rdb.rocksdb_options_destroy(options);

        var err: ?[*:0]u8 = null;
        var db = rdb.rocksdb_open(options, dir.ptr, &err);
        if (err) |e| {
            std.log.err("Error: {s}", .{e});
            return error.RocksDbOpen;
        }

        return .{
            .db = db orelse return error.RocksDbFail,
            .read_options = rdb.rocksdb_readoptions_create(),
            .write_options = rdb.rocksdb_writeoptions_create(),
        };
    }

    pub fn close(self: Self) void {
        rdb.rocksdb_readoptions_destroy(self.read_options);
        rdb.rocksdb_writeoptions_destroy(self.write_options);
        rdb.rocksdb_close(self.db);
    }

    pub fn get(self: Self, key: [:0]const u8) !String {
        var err: ?[*:0]u8 = null;
        var val_len: usize = 0;
        const val = rdb.rocksdb_get(
            self.db,
            self.read_options,
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
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_put(
            self.db,
            self.write_options,
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
