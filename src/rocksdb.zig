const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));
const m = std.heap.c_allocator;
const tmpDir = std.testing.tmpDir;

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

    /// Enforces null-terminated slice for dir path, so that we don't have to allocate
    /// another slice to convert to null-terminated.
    pub fn open(dir: [:0]const u8, open_options: OpenOptions) !Self {
        const options = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        defer rdb.rocksdb_options_destroy(options);

        // prefix extractor setup
        // When only the prefix extractor is set, order is guaranteed only for keys of the same
        // prefix, not for total order. We do _not_ set `ReadOption.total_order_seek=true`.
        //
        // Also, looks like this doesn't need to be freed? Does that mean the `db.close()` destroys?
        // Don't see any leak in valgrind.
        const slice_transform = blk: {
            if (open_options.prefix_extractor) |extractor| {
                switch (extractor.kind) {
                    .fixed => {
                        break :blk rdb.rocksdb_slicetransform_create_fixed_prefix(extractor.len);
                    },
                }
            } else {
                break :blk rdb.rocksdb_slicetransform_create_noop();
            }
        };
        rdb.rocksdb_options_set_prefix_extractor(options, slice_transform);

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

    pub const OpenOptions = struct {
        prefix_extractor: ?PrefixExtractor = null,

        pub const PrefixExtractor = struct {
            kind: Kind,
            len: usize,

            pub const Kind = enum {
                // C api only allows fixed, not capped?
                fixed,
            };
        };
    };

    pub fn close(self: Self) void {
        rdb.rocksdb_readoptions_destroy(self.read_options);
        rdb.rocksdb_writeoptions_destroy(self.write_options);
        rdb.rocksdb_close(self.db);
    }

    // Return value is malloc'd, need to call `free` on it.
    pub fn get(self: Self, key: []const u8) !?String {
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
        if (val == null) {
            return null;
        }

        return val[0..val_len];
    }

    pub fn set(self: Self, key: []const u8, value: []const u8) !void {
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

    // Does not need to free key/value, they are not malloc'd
    pub const IterEntry = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const Iter = struct {
        iter: *rdb.rocksdb_iterator_t,

        pub fn seek(self: Iter, target: []const u8) void {
            rdb.rocksdb_iter_seek(self.iter, target.ptr, target.len);
        }

        pub fn seek_to_first(self: Iter) void {
            rdb.rocksdb_iter_seek_to_first(self.iter);
        }

        pub fn valid(self: Iter) bool {
            return rdb.rocksdb_iter_valid(self.iter) == 1;
        }

        pub fn current_entry(self: Iter) IterEntry {
            // rdb no malloc for iterator `get`
            var key_size: usize = 0;
            const key = rdb.rocksdb_iter_key(self.iter, &key_size);
            var value_size: usize = 0;
            const value = rdb.rocksdb_iter_value(self.iter, &value_size);

            const res = IterEntry{
                .key = key[0..key_size],
                .value = value[0..value_size],
            };

            return res;
        }

        pub fn next(self: Iter) void {
            rdb.rocksdb_iter_next(self.iter);
        }

        pub fn current_key_starts_with(self: Iter, prefix: []const u8) bool {
            var key_size: usize = 0;
            const key = rdb.rocksdb_iter_key(self.iter, &key_size);
            if (prefix.len < key_size) {
                return std.mem.startsWith(u8, key[0..key_size], prefix);
            }
            return false;
        }

        pub fn deinit(self: Iter) void {
            rdb.rocksdb_iter_destroy(self.iter);
        }
    };

    pub fn iter(self: Self) !Iter {
        return Iter{
            .iter = rdb.rocksdb_create_iterator(self.db, self.read_options) orelse return error.IteratorCreationFail,
        };
    }
};

test "get" {
    const alloc = std.testing.allocator;

    var tmp = tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fs.path.joinZ(alloc, &[_][]const u8{ "zig-cache", "tmp", tmp.sub_path[0..] });
    defer alloc.free(db_path);
    var db = try RocksDb.open(db_path, .{});
    defer db.close();

    try db.set("test_key_1", "test_value_1");
    const test_val = (try db.get("test_key_1")).?;
    defer RocksDb.deinit_string(test_val);
    try std.testing.expectEqualSlices(u8, "test_value_1", test_val);
    try std.testing.expectEqual(try db.get("missing"), null);
}

test "iterator" {
    const alloc = std.testing.allocator;

    var tmp = tmpDir(.{});
    defer tmp.cleanup();

    // NOTE: wrestled w/ this a long time. Kept getting "file too long" when using just `join`. I finally
    // read my `open` code and realized that just a ptr (and no length) was being passed as the path to
    // open. `join` doesn't have null terminator, so it would just read forever.
    //
    // Because I don't want to recreate an array w/in `open` that's null terminated, I have the path parameter
    // require a null-terminated slice. Then it's up to the caller to create it correctly in the first place.
    // In this case, that means using `joinz` instead of `join`.
    //
    // realPath doesn't appear to output null-terminated slices.
    const db_path = try std.fs.path.joinZ(alloc, &[_][]const u8{ "zig-cache", "tmp", tmp.sub_path[0..] });
    defer alloc.free(db_path);
    var db = try RocksDb.open(db_path, .{});
    defer db.close();

    // insert out of order
    try db.set("test_key_3", "test_value_3");
    try db.set("test_key_2", "test_value_2");
    try db.set("different_prefix_test_key_1", "test_value_1");

    // iterator should be in order
    // and iterate over all prefixes (total)
    // this is an unrolled loop
    var it = try db.iter();
    defer it.deinit();
    it.seek_to_first();
    {
        try std.testing.expect(it.valid());
        const entry = it.current_entry();
        try std.testing.expectEqualSlices(u8, "different_prefix_test_key_1", entry.key);
        try std.testing.expectEqualSlices(u8, "test_value_1", entry.value);
    }
    {
        it.next();
        try std.testing.expect(it.valid());
        const entry = it.current_entry();
        try std.testing.expectEqualSlices(u8, "test_key_2", entry.key);
        try std.testing.expectEqualSlices(u8, "test_value_2", entry.value);
    }
    {
        it.next();
        try std.testing.expect(it.valid());
        const entry = it.current_entry();
        try std.testing.expectEqualSlices(u8, "test_key_3", entry.key);
        try std.testing.expectEqualSlices(u8, "test_value_3", entry.value);
    }
    it.next();
    try std.testing.expect(!it.valid());
}

// TODO benchmark to see when it's faster than w/out prefix iterator.
test "prefix iterator" {
    const alloc = std.testing.allocator;

    var tmp = tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fs.path.joinZ(alloc, &[_][]const u8{ "zig-cache", "tmp", tmp.sub_path[0..] });
    defer alloc.free(db_path);
    // This should also work w/out the prefix extractor.
    var db = try RocksDb.open(db_path, .{ .prefix_extractor = .{ .kind = .fixed, .len = 3 } });
    defer db.close();

    // foo prefix
    try db.set("foo_1", "1");
    try db.set("foo_2", "2");

    // bar prefix
    try db.set("bar_1", "1");
    try db.set("bar_2", "2");

    // should iterate for foo prefix only
    {
        var it = try db.iter();
        defer it.deinit();
        it.seek("foo");
        {
            try std.testing.expect(it.valid());
            const entry = it.current_entry();
            try std.testing.expectEqualSlices(u8, "foo_1", entry.key);
            try std.testing.expectEqualSlices(u8, "1", entry.value);
        }
        {
            it.next();
            try std.testing.expect(it.valid());
            const entry = it.current_entry();
            try std.testing.expectEqualSlices(u8, "foo_2", entry.key);
            try std.testing.expectEqualSlices(u8, "2", entry.value);
        }
        it.next();
        try std.testing.expect(!it.valid());
    }
    // should iterate for bar prefix only
    {
        var it = try db.iter();
        defer it.deinit();
        it.seek("bar");
        {
            try std.testing.expect(it.valid());
            const entry = it.current_entry();
            try std.testing.expectEqualSlices(u8, "bar_1", entry.key);
            try std.testing.expectEqualSlices(u8, "1", entry.value);
        }
        {
            it.next();
            try std.testing.expect(it.valid());
            const entry = it.current_entry();
            try std.testing.expectEqualSlices(u8, "bar_2", entry.key);
            try std.testing.expectEqualSlices(u8, "2", entry.value);
        }
        it.next();
        try std.testing.expect(it.valid());
        // `next` can go across the boundary to a different prefix, so
        // check the end condition
        try std.testing.expect(!it.current_key_starts_with("bar"));
    }
}
