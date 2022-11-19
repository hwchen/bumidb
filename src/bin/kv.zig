const std = @import("std");
const RocksDb = @import("bumi").rocksdb.RocksDb;

pub fn main() anyerror!void {
    var db = try RocksDb.open("/tmp/bumidb", .{ .prefix_extractor = .{ .kind = .fixed, .len = 3 } });
    defer db.close();
    std.debug.print("Opened db\n", .{});

    try db.set("test_key_1", "test_value_1");
    const test_val = (try db.get("test_key_1")).?;
    defer RocksDb.deinit_string(test_val);
    std.debug.print("get value: {s}\n", .{test_val});
    std.debug.print("try get missing entry: {any}\n", .{try db.get("missing")});

    try db.set("test_key_2", "test_value_2");
    try db.set("test_key_3", "test_value_3");

    var it = try db.iter();
    defer it.deinit();
    it.seek_to_first();
    while (it.valid()) : (it.next()) {
        const entry = it.current_entry();
        std.debug.print("iter: {s}: {s}\n", .{ entry.key, entry.value });
    }
}

// TODO parse cli
