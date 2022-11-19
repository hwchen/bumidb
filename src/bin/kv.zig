const std = @import("std");
const Allocator = std.mem.Allocator;
const RocksDb = @import("bumi").rocksdb.RocksDb;

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const opts = try Opts.parse(alloc); // let arena allocator clean up

    const db_path = opts.db_path orelse "/tmp/bumidb";
    std.debug.print("Opening database at {s}:\n", .{db_path});
    var db = try RocksDb.open(db_path, .{});
    defer db.close();

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
    db_path: ?[:0]const u8,
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

        var command: [:0]const u8 = undefined;
        const command_or_pathflag = args.next() orelse return error.NoCommandOrPathFlag;

        // first check if it's --db-path, which has to come before the command.
        const db_path = blk: {
            if (std.mem.eql(u8, "--db-path", command_or_pathflag)) {
                const path = args.next() orelse return error.NoDbPath;
                command = args.next() orelse return error.NoCommand;
                break :blk path;
            } else {
                command = command_or_pathflag;
                break :blk null;
            }
        };

        if (std.ascii.eqlIgnoreCase("get", command)) {
            const target = args.next() orelse return error.GetNoTarget;
            return .{
                .arg_iter = args,
                .db_path = db_path,
                .command = .{ .get = .{ .target = target } },
            };
        } else if (std.ascii.eqlIgnoreCase("set", command)) {
            const key = args.next() orelse return error.SetNoKey;
            const value = args.next() orelse return error.SetNoValue;
            return .{
                .arg_iter = args,
                .db_path = db_path,
                .command = .{ .set = .{ .key = key, .value = value } },
            };
        } else if (std.ascii.eqlIgnoreCase("list", command)) {
            return .{
                .arg_iter = args,
                .db_path = db_path,
                .command = .{ .list = .{ .prefix = args.next() } },
            };
        } else {
            return error.CommandNotFound;
        }
    }

    fn deinit(self: Opts) void {
        // Noop unles windows or wasi
        self.arg_iter.deinit();
    }
};
