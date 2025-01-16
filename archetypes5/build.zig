const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "scratch",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const zjobs = b.dependency("zjobs", .{});
    exe.root_module.addImport("zjobs", zjobs.module("root"));

    b.installArtifact(exe);
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const bench = b.addExecutable(.{
        .name = "benchmark",
        .root_source_file = b.path("src/benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });

    bench.root_module.addImport("zjobs", zjobs.module("root"));

    b.installArtifact(bench);
    const bench_cmd = b.addRunArtifact(bench);
    bench_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const bench_step = b.step("benchmark", "Run the benchmark");
    bench_step.dependOn(&bench_cmd.step);

    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    unit_tests.root_module.addImport("zjobs", zjobs.module("root"));

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
