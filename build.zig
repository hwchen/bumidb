const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    // Tests
    {
        const main_tests = b.addTest("src/main.zig");
        main_tests.setBuildMode(mode);
        main_tests.linkLibC();
        main_tests.linkSystemLibraryName("rocksdb");
        main_tests.addLibraryPath("./rocksdb");
        main_tests.addIncludePath("./rocksdb/include");

        const test_step = b.step("test", "Run library tests");
        test_step.dependOn(&main_tests.step);
    }

    // kv executable
    const kv_exe = b.addExecutable("kv", "./src/bin/kv.zig");
    kv_exe.addPackagePath("bumi", "./src/main.zig");
    kv_exe.linkLibC();
    kv_exe.linkSystemLibraryName("rocksdb");
    kv_exe.addLibraryPath("./rocksdb");
    kv_exe.addIncludePath("./rocksdb/include");
    kv_exe.setBuildMode(mode);
    kv_exe.install(); // TODO this is not installing to zig-out?
    const kv_step = b.step("kv", "Build kv cli");
    kv_step.dependOn(&kv_exe.step);

    const run_kv_cmd = kv_exe.run();
    run_kv_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_kv_cmd.addArgs(args);
    }

    const run_kv_step = b.step("run-kv", "Build and run kv cli");
    run_kv_step.dependOn(&run_kv_cmd.step);
}
