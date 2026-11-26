//! Port of iceoryx2/benchmarks/publish-subscribe/src/main.rs
//! (original: Copyright (c) 2023 Contributors to the Eclipse Foundation, Apache-2.0 OR MIT)
//!
//! Two participants ping-pong a sample over services "a2b" and "b2a"; each
//! busy-waits on receive. Latency = total time / (2 * iterations).
//! Default: two threads in one process (exactly like the Rust benchmark).
//! --processes: two forked processes instead.

const std = @import("std");
const zc = @import("zc");
const linux = std.os.linux;
const Atomic = std.atomic.Value;

const Args = struct {
    iterations: u64 = 10_000_000,
    payload_size: usize = 8192,
    core1: usize = 0,
    core2: usize = 1,
    send_copy: bool = false,
    processes: bool = false,
};

fn parseArgs() !Args {
    var a = Args{};
    var it = std.process.args();
    _ = it.next();
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "-i") or std.mem.eql(u8, arg, "--iterations")) {
            a.iterations = try std.fmt.parseInt(u64, it.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "-p") or std.mem.eql(u8, arg, "--payload-size")) {
            a.payload_size = try std.fmt.parseInt(usize, it.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--cpu-core-participant-1")) {
            a.core1 = try std.fmt.parseInt(usize, it.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--cpu-core-participant-2")) {
            a.core2 = try std.fmt.parseInt(usize, it.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--send-copy")) {
            a.send_copy = true;
        } else if (std.mem.eql(u8, arg, "--processes")) {
            a.processes = true;
        } else if (std.mem.eql(u8, arg, "--bench-all") or std.mem.eql(u8, arg, "--bench-ipc")) {
            // accepted for CLI compatibility; only the IPC variant exists
        } else {
            std.debug.print(
                \\usage: bench-pubsub [-i N] [-p BYTES] [--cpu-core-participant-1 C]
                \\                    [--cpu-core-participant-2 C] [--send-copy] [--processes]
                \\
            , .{});
            return error.BadArgs;
        }
    }
    return a;
}

/// one-shot spin barrier usable across threads and forked processes (lives in shared memory)
const Barrier = extern struct {
    count: Atomic(u32),
    fn wait(self: *Barrier, n: u32) void {
        _ = self.count.fetchAdd(1, .acq_rel);
        while (self.count.load(.acquire) < n) std.atomic.spinLoopHint();
    }
};
const Barriers = extern struct { startup: Barrier, start: Barrier };

fn pin(core: usize) void {
    var set: linux.cpu_set_t = @splat(0);
    set[core / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(core % @bitSizeOf(usize));
    linux.sched_setaffinity(0, &set) catch |e| std.debug.print("warning: affinity failed: {}\n", .{e});
}

const Ctx = struct {
    args: Args,
    a2b: *zc.Service,
    b2a: *zc.Service,
    bar: *Barriers,
};

fn participant1(ctx: *const Ctx) void {
    pin(ctx.args.core1);
    const alloc = std.heap.page_allocator;
    var sender = ctx.a2b.publisher(alloc, ctx.args.payload_size) catch unreachable;
    defer sender.deinit();
    var receiver = ctx.b2a.subscriber(alloc) catch unreachable;
    defer receiver.deinit();

    ctx.bar.startup.wait(3);
    ctx.bar.start.wait(3);

    var sample = sender.loanSliceUninit(ctx.args.payload_size) catch unreachable;
    if (ctx.args.send_copy) @memset(sample.payload, 0);

    var i: u64 = 0;
    while (i < ctx.args.iterations) : (i += 1) {
        _ = sender.send(sample);
        sample = sender.loanSliceUninit(ctx.args.payload_size) catch unreachable;
        while (true) {
            if (receiver.receive() catch unreachable) |s| {
                receiver.release(s);
                break;
            }
        }
    }
    sender.release(sample);
}

fn participant2(ctx: *const Ctx) void {
    pin(ctx.args.core2);
    const alloc = std.heap.page_allocator;
    var sender = ctx.b2a.publisher(alloc, ctx.args.payload_size) catch unreachable;
    defer sender.deinit();
    var receiver = ctx.a2b.subscriber(alloc) catch unreachable;
    defer receiver.deinit();

    ctx.bar.startup.wait(3);
    ctx.bar.start.wait(3);

    var i: u64 = 0;
    while (i < ctx.args.iterations) : (i += 1) {
        const sample = sender.loanSliceUninit(ctx.args.payload_size) catch unreachable;
        if (ctx.args.send_copy) @memset(sample.payload, 0);
        while (true) {
            if (receiver.receive() catch unreachable) |s| {
                receiver.release(s);
                break;
            }
        }
        _ = sender.send(sample);
    }
}

pub fn main() !void {
    const args = try parseArgs();
    zc.removeStale(zc.prefix ++ "bench_");

    const cfg = zc.ServiceConfig{
        .max_publishers = 1,
        .max_subscribers = 1,
        .subscriber_max_buffer_size = 1,
        .enable_safe_overflow = true,
    };
    var a2b = try zc.Service.create("bench_a2b", cfg);
    defer a2b.deinit();
    var b2a = try zc.Service.create("bench_b2a", cfg);
    defer b2a.deinit();

    // barriers in anonymous shared memory so they also work across fork()
    const bmem = try std.posix.mmap(null, @sizeOf(Barriers), std.posix.PROT.READ | std.posix.PROT.WRITE, .{ .TYPE = .SHARED, .ANONYMOUS = true }, -1, 0);
    defer std.posix.munmap(bmem);
    const bar: *Barriers = @ptrCast(@alignCast(bmem.ptr));
    bar.* = std.mem.zeroes(Barriers);

    const ctx = Ctx{ .args = args, .a2b = &a2b, .b2a = &b2a, .bar = bar };

    var elapsed_ns: u64 = undefined;
    if (args.processes) {
        const p1 = try std.posix.fork();
        if (p1 == 0) {
            participant1(&ctx);
            std.posix.exit(0);
        }
        const p2 = try std.posix.fork();
        if (p2 == 0) {
            participant2(&ctx);
            std.posix.exit(0);
        }
        bar.startup.wait(3);
        const start = try std.time.Instant.now();
        bar.start.wait(3);
        _ = std.posix.waitpid(p1, 0);
        _ = std.posix.waitpid(p2, 0);
        elapsed_ns = (try std.time.Instant.now()).since(start);
    } else {
        const t1 = try std.Thread.spawn(.{}, participant1, .{&ctx});
        const t2 = try std.Thread.spawn(.{}, participant2, .{&ctx});
        bar.startup.wait(3);
        const start = try std.time.Instant.now();
        bar.start.wait(3);
        t1.join();
        t2.join();
        elapsed_ns = (try std.time.Instant.now()).since(start);
    }

    var buf: [256]u8 = undefined;
    const line = try std.fmt.bufPrint(&buf, "zig-zc::{s} ::: Iterations: {d}, Time: {d:.6} s, Latency: {d} ns, Sample Size: {d}\n", .{
        if (args.processes) "ipc(2 processes)" else "ipc(2 threads)",
        args.iterations,
        @as(f64, @floatFromInt(elapsed_ns)) / 1e9,
        elapsed_ns / (args.iterations * 2),
        args.payload_size,
    });
    try std.fs.File.stdout().writeAll(line);
}
