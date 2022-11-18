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

    // kv tests
    const kv_tests = b.addTest("scratch/kv.zig");
    kv_tests.setBuildMode(mode);
    kv_tests.linkLibC();
    kv_tests.linkSystemLibraryName("rocksdb");
    kv_tests.addLibraryPath("./rocksdb");
    kv_tests.addIncludePath("./rocksdb/include");

    const kv_test_step = b.step("test-kv", "Run kv tests");
    kv_test_step.dependOn(&kv_tests.step);
}
