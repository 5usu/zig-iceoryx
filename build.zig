const std = @import("std");

// Zig 0.15.2. Builds the same benchmark against both library versions:
//   bench-pubsub     -> src/zc.zig     (v2: sequence-tagged ring connection)
//   bench-pubsub-v1  -> src/zc_v1.zig  (v1: index queues, per-send handshake)
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    // the library, for packages that depend on this one: @import("zc")
    _ = b.addModule("zc", .{ .root_source_file = b.path("src/zc.zig"), .target = target, .optimize = optimize });

    const variants = [_]struct { exe: []const u8, lib: []const u8 }{
        .{ .exe = "bench-pubsub", .lib = "src/zc.zig" },
        .{ .exe = "bench-pubsub-v1", .lib = "src/zc_v1.zig" },
    };
    const test_step = b.step("test", "Run unit tests");
    for (variants) |v| {
        const lib = b.createModule(.{ .root_source_file = b.path(v.lib), .target = target, .optimize = optimize });
        const exe = b.addExecutable(.{
            .name = v.exe,
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/bench_pubsub.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "zc", .module = lib }},
            }),
        });
        b.installArtifact(exe);

        const tests = b.addTest(.{
            .root_module = b.createModule(.{ .root_source_file = b.path(v.lib), .target = target, .optimize = .ReleaseSafe }),
        });
        test_step.dependOn(&b.addRunArtifact(tests).step);
    }

    const run = b.addRunArtifact(b.addExecutable(.{
        .name = "bench-pubsub-run",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_pubsub.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zc", .module = b.createModule(.{ .root_source_file = b.path("src/zc.zig"), .target = target, .optimize = optimize }) }},
        }),
    }));
    if (b.args) |a| run.addArgs(a);
    b.step("bench", "Run the pub/sub latency benchmark (v2)").dependOn(&run.step);
}
