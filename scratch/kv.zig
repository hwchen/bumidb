const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub fn main() anyerror!void {
    var db = try RocksDb.open("data");
    defer db.close();
    std.debug.print("Opened db\n", .{});
}

pub const RocksDb = struct {
    db: *rdb.rocksdb_t,

    pub fn open(dir: []const u8) !RocksDb {
        var options = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);

        var err: ?[*:0]u8 = null;
        var db = rdb.rocksdb_open(options, dir.ptr, &err);
        if (err) |e| {
            std.debug.print("Error: {s}", .{e});
            return error.RocksDbOpen;
        }

        return .{ .db = db orelse return error.RocksDbFail };
    }

    pub fn close(self: RocksDb) void {
        rdb.rocksdb_close(self.db);
    }
};
