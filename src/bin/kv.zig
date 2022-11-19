const std = @import("std");
const Allocator = std.mem.Allocator;
const RocksDb = @import("bumi").rocksdb.RocksDb;

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var db = try RocksDb.open("/tmp/bumidb", .{});
    defer db.close();

    const opts = try Opts.parse(alloc); // let arena allocator clean up
    switch (opts.command) {
        .set => |s| try db.set(s.key, s.value),
        .get => |g| {
            const value = (try db.get(g.target)) orelse return error.KeyNotFound;
            std.debug.print("{s}\n", .{value});
            defer RocksDb.deinit_string(value);
        },
        .list => |l| {
            var it = try db.iter();
            if (l.prefix) |prefix| {
                it.seek(prefix);
                while (it.valid() and it.current_key_starts_with(prefix)) : (it.next()) {
                    const entry = it.current_entry();
                    std.debug.print("{s}: {s}\n", .{ entry.key, entry.value });
                }
            } else {
                it.seek_to_first();
                while (it.valid()) : (it.next()) {
                    const entry = it.current_entry();
                    std.debug.print("{s}: {s}\n", .{ entry.key, entry.value });
                }
            }
        },
    }
}

const Opts = struct {
    arg_iter: std.process.ArgIterator,
    command: Command,

    const Command = union(enum) {
        get: Get,
        set: Set,
        list: List,

        const Get = struct {
            target: [:0]const u8,
        };

        const Set = struct {
            key: [:0]const u8,
            value: [:0]const u8,
        };

        const List = struct {
            prefix: ?[:0]const u8,
        };
    };

    fn parse(alloc: Allocator) !Opts {
        // alloc only necessary for cross-platform compatibility, windows and wasi
        var args = try std.process.argsWithAllocator(alloc);
        _ = args.skip();

        const command = args.next() orelse return error.NoCommand;
        if (std.ascii.eqlIgnoreCase("get", command)) {
            const target = args.next() orelse return error.GetNoTarget;
            return .{ .command = .{ .get = .{ .target = target } }, .arg_iter = args };
        } else if (std.ascii.eqlIgnoreCase("set", command)) {
            const key = args.next() orelse return error.SetNoKey;
            const value = args.next() orelse return error.SetNoValue;
            return .{ .command = .{ .set = .{ .key = key, .value = value } }, .arg_iter = args };
        } else if (std.ascii.eqlIgnoreCase("list", command)) {
            return .{ .command = .{ .list = .{ .prefix = args.next() } }, .arg_iter = args };
        } else {
            return error.CommandNotFound;
        }
    }

    fn deinit(self: Opts) void {
        // Noop unles windows or wasi
        self.arg_iter.deinit();
    }
};
