const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const main_tests = b.addTest("src/main.zig");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // kv executable
    const kv_exe = b.addExecutable("kv", "./scratch/kv.zig");
    kv_exe.addPackagePath("bumidb", "./src/main.zig");
    kv_exe.linkLibC();
    kv_exe.linkSystemLibraryName("rocksdb");
    kv_exe.addLibraryPath("./rocksdb");
    kv_exe.addIncludePath("./rocksdb/include");
    kv_exe.setBuildMode(mode);
    kv_exe.install();
    const kv_step = b.step("kv", "Build kv cli");
    kv_step.dependOn(&kv_exe.step);
}
