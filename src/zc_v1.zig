//! zc — a minimal zero-copy shared-memory publish/subscribe IPC library in Zig,
//! modelled on the architecture of eclipse-iceoryx/iceoryx2:
//!
//!   * Service segment  (/dev/shm/<prefix><svc>.service)
//!       static config + dynamic config (publisher / subscriber port slots) +
//!       a change counter that tells ports to update their connections.
//!   * Data segment per publisher (/dev/shm/<prefix><svc>_p<id>.data)
//!       fixed-size chunks; only the owning publisher allocates (local free list,
//!       local ref counts), subscribers map it read-only.
//!   * Zero-copy connection per publisher/subscriber pair
//!       (/dev/shm/<prefix><svc>_p<id>_s<id>.conn)
//!       - submission queue  publisher -> subscriber (chunk indices), optionally
//!         "safely overflowing": when full the publisher reclaims the oldest entry
//!       - completion queue  subscriber -> publisher (returned chunk indices)
//!
//! Not implemented (vs. iceoryx2): nodes / dead-node cleanup, events & waitsets,
//! request-response, history, user headers, dynamic (re)allocation of slices,
//! process-local service variants, typed payloads.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Atomic = std.atomic.Value;

pub const prefix = "iox2zig_";
pub const max_ports = 16;
const cache_line = 64;
const ready_magic: u32 = 0x1CE0_2216;

// ---------------------------------------------------------------------------
// shared memory
// ---------------------------------------------------------------------------

pub const Shm = struct {
    mem: []align(std.heap.page_size_min) u8,
    path_buf: [256]u8,
    path_len: usize,
    owner: bool,

    fn makePath(buf: *[256]u8, name: []const u8) ![:0]const u8 {
        return std.fmt.bufPrintZ(buf, "/dev/shm/{s}", .{name});
    }

    pub fn create(name: []const u8, size: usize) !Shm {
        var self: Shm = undefined;
        const p = try makePath(&self.path_buf, name);
        self.path_len = p.len;
        const fd = try posix.openZ(p, .{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true, .CLOEXEC = true }, 0o600);
        defer posix.close(fd);
        errdefer posix.unlinkZ(p) catch {};
        try posix.ftruncate(fd, size);
        self.mem = try posix.mmap(null, size, posix.PROT.READ | posix.PROT.WRITE, .{ .TYPE = .SHARED }, fd, 0);
        self.owner = true;
        return self;
    }

    /// Opens an existing segment. Returns error.NotReady if the file exists but
    /// the creator has not sized it yet.
    pub fn open(name: []const u8, writable: bool) !Shm {
        var self: Shm = undefined;
        const p = try makePath(&self.path_buf, name);
        self.path_len = p.len;
        const fd = try posix.openZ(p, .{ .ACCMODE = if (writable) .RDWR else .RDONLY, .CLOEXEC = true }, 0);
        defer posix.close(fd);
        const st = try posix.fstat(fd);
        if (st.size == 0) return error.NotReady;
        const prot: u32 = if (writable) posix.PROT.READ | posix.PROT.WRITE else posix.PROT.READ;
        self.mem = try posix.mmap(null, @intCast(st.size), prot, .{ .TYPE = .SHARED }, fd, 0);
        self.owner = false;
        return self;
    }

    pub fn deinit(self: *Shm) void {
        posix.munmap(self.mem);
        if (self.owner) {
            const p: [:0]const u8 = self.path_buf[0..self.path_len :0];
            posix.unlinkZ(p) catch {};
        }
    }
};

/// Removes leftover segments of earlier (crashed) runs whose name starts with `name_prefix`.
pub fn removeStale(name_prefix: []const u8) void {
    var dir = std.fs.openDirAbsolute("/dev/shm", .{ .iterate = true }) catch return;
    defer dir.close();
    var it = dir.iterate();
    while (it.next() catch null) |e| {
        if (std.mem.startsWith(u8, e.name, name_prefix)) dir.deleteFile(e.name) catch {};
    }
}

fn spinOpen(name: []const u8, writable: bool) !Shm {
    var tries: usize = 0;
    while (true) : (tries += 1) {
        if (Shm.open(name, writable)) |s| return s else |err| switch (err) {
            error.NotReady => {},
            else => return err,
        }
        if (tries > 100_000) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

fn waitReady(ready: *const Atomic(u32)) !void {
    var tries: usize = 0;
    while (ready.load(.acquire) != ready_magic) : (tries += 1) {
        if (tries > 10_000_000) return error.Timeout;
        std.atomic.spinLoopHint();
    }
}

// ---------------------------------------------------------------------------
// service
// ---------------------------------------------------------------------------

const PortSlot = extern struct {
    id: Atomic(u64), // 0 == free
};

const ServiceHeader = extern struct {
    ready: Atomic(u32),
    max_publishers: u32,
    max_subscribers: u32,
    subscriber_buffer_size: u32,
    subscriber_max_borrowed: u32,
    publisher_max_loaned: u32,
    safe_overflow: u32,
    _pad0: u32,
    port_id_counter: Atomic(u64),
    _pad1: [cache_line - 40]u8,
    change_counter: Atomic(u64),
    _pad2: [cache_line - 8]u8,
    publishers: [max_ports]PortSlot,
    subscribers: [max_ports]PortSlot,
};

pub const ServiceConfig = struct {
    max_publishers: u32 = 2,
    max_subscribers: u32 = 8,
    subscriber_max_buffer_size: u32 = 2,
    subscriber_max_borrowed_samples: u32 = 2,
    publisher_max_loaned_samples: u32 = 2,
    enable_safe_overflow: bool = true,
};

pub const Service = struct {
    shm: Shm,
    name_buf: [128]u8,
    name_len: usize,

    pub fn name(self: *const Service) []const u8 {
        return self.name_buf[0..self.name_len];
    }

    fn hdr(self: *const Service) *ServiceHeader {
        return @ptrCast(@alignCast(self.shm.mem.ptr));
    }

    pub fn create(svc_name: []const u8, cfg: ServiceConfig) !Service {
        if (cfg.max_publishers > max_ports or cfg.max_subscribers > max_ports) return error.TooManyPorts;
        var self: Service = undefined;
        self.name_len = (try std.fmt.bufPrint(&self.name_buf, "{s}{s}", .{ prefix, svc_name })).len;
        var nbuf: [160]u8 = undefined;
        const seg = try std.fmt.bufPrint(&nbuf, "{s}.service", .{self.name()});
        self.shm = try Shm.create(seg, @sizeOf(ServiceHeader));
        const h = self.hdr();
        h.* = std.mem.zeroes(ServiceHeader);
        h.max_publishers = cfg.max_publishers;
        h.max_subscribers = cfg.max_subscribers;
        h.subscriber_buffer_size = cfg.subscriber_max_buffer_size;
        h.subscriber_max_borrowed = cfg.subscriber_max_borrowed_samples;
        h.publisher_max_loaned = cfg.publisher_max_loaned_samples;
        h.safe_overflow = @intFromBool(cfg.enable_safe_overflow);
        h.port_id_counter.store(1, .monotonic);
        h.ready.store(ready_magic, .release);
        return self;
    }

    pub fn open(svc_name: []const u8) !Service {
        var self: Service = undefined;
        self.name_len = (try std.fmt.bufPrint(&self.name_buf, "{s}{s}", .{ prefix, svc_name })).len;
        var nbuf: [160]u8 = undefined;
        const seg = try std.fmt.bufPrint(&nbuf, "{s}.service", .{self.name()});
        self.shm = try spinOpen(seg, true);
        try waitReady(&self.hdr().ready);
        return self;
    }

    pub fn deinit(self: *Service) void {
        self.shm.deinit();
    }

    fn register(self: *Service, slots: []PortSlot, limit: u32) !u64 {
        const h = self.hdr();
        const id = h.port_id_counter.fetchAdd(1, .monotonic);
        for (slots[0..limit]) |*s| {
            if (s.id.cmpxchgStrong(0, id, .acq_rel, .monotonic) == null) {
                _ = h.change_counter.fetchAdd(1, .release);
                return id;
            }
        }
        return error.ExceedsMaxPorts;
    }

    fn unregister(self: *Service, slots: []PortSlot, id: u64) void {
        for (slots) |*s| {
            if (s.id.cmpxchgStrong(id, 0, .acq_rel, .monotonic) == null) break;
        }
        _ = self.hdr().change_counter.fetchAdd(1, .release);
    }

    pub fn publisher(self: *Service, alloc: std.mem.Allocator, max_slice_len: usize) !Publisher {
        return Publisher.init(self, alloc, max_slice_len);
    }

    pub fn subscriber(self: *Service, alloc: std.mem.Allocator) !Subscriber {
        return Subscriber.init(self, alloc);
    }
};

// ---------------------------------------------------------------------------
// data segment
// ---------------------------------------------------------------------------

const DataHeader = extern struct {
    ready: Atomic(u32),
    num_chunks: u32,
    chunk_stride: u64,
    chunks_offset: u64,
};

pub const ChunkHeader = extern struct {
    publisher_id: u64,
    sequence: u64,
    payload_len: u64,
    _pad: [cache_line - 24]u8,
};

// ---------------------------------------------------------------------------
// zero-copy connection
// ---------------------------------------------------------------------------

const ConnHeader = extern struct {
    ready: Atomic(u32),
    sub_cap: u32,
    comp_cap: u32,
    sender_closed: Atomic(u32),
    _p0: [cache_line - 16]u8,
    // submission queue: producer = publisher, consumers = subscriber (+ publisher on overflow)
    sub_write: Atomic(u64),
    _p1: [cache_line - 8]u8,
    sub_read: Atomic(u64),
    _p2: [cache_line - 8]u8,
    // completion queue: producer = subscriber, consumer = publisher
    comp_write: Atomic(u64),
    _p3: [cache_line - 8]u8,
    comp_read: Atomic(u64),
    _p4: [cache_line - 8]u8,
    // followed by: sub slots [sub_cap]Atomic(u64), comp slots [comp_cap]Atomic(u64)
};

const Conn = struct {
    shm: Shm,

    fn deinit(self: *Conn) void {
        self.shm.deinit();
    }

    fn size(sub_cap: u32, comp_cap: u32) usize {
        return @sizeOf(ConnHeader) + (@as(usize, sub_cap) + comp_cap) * 8;
    }
    inline fn hdr(self: *const Conn) *ConnHeader {
        return @ptrCast(@alignCast(self.shm.mem.ptr));
    }
    inline fn subSlots(self: *const Conn) [*]Atomic(u64) {
        return @ptrCast(@alignCast(self.shm.mem.ptr + @sizeOf(ConnHeader)));
    }
    inline fn compSlots(self: *const Conn) [*]Atomic(u64) {
        return self.subSlots() + self.hdr().sub_cap;
    }

    /// Pop the oldest entry of the submission queue. Safe to be called concurrently
    /// by the subscriber and (on overflow) the publisher.
    inline fn subPop(self: *const Conn) ?u64 {
        const h = self.hdr();
        const slots = self.subSlots();
        var r = h.sub_read.load(.acquire);
        while (true) {
            const w = h.sub_write.load(.acquire);
            if (r == w) return null;
            const v = slots[r % h.sub_cap].load(.monotonic);
            r = h.sub_read.cmpxchgWeak(r, r + 1, .acq_rel, .acquire) orelse return v;
        }
    }

    /// Publisher only. Returns the evicted chunk index if the queue overflowed.
    inline fn subPush(self: *const Conn, v: u64, overflow: bool) union(enum) { ok, full, evicted: u64 } {
        const h = self.hdr();
        const w = h.sub_write.raw; // only the publisher writes sub_write
        var evicted: ?u64 = null;
        while (w - h.sub_read.load(.acquire) >= h.sub_cap) {
            if (!overflow) return .full;
            if (self.subPop()) |old| evicted = old;
        }
        self.subSlots()[w % h.sub_cap].store(v, .monotonic);
        h.sub_write.store(w + 1, .release);
        return if (evicted) |e| .{ .evicted = e } else .ok;
    }

    /// Subscriber only.
    inline fn compPush(self: *const Conn, v: u64) bool {
        const h = self.hdr();
        const w = h.comp_write.raw;
        if (w - h.comp_read.load(.acquire) >= h.comp_cap) return false;
        self.compSlots()[w % h.comp_cap].store(v, .monotonic);
        h.comp_write.store(w + 1, .release);
        return true;
    }

    /// Publisher only.
    inline fn compPop(self: *const Conn) ?u64 {
        const h = self.hdr();
        const r = h.comp_read.raw;
        if (r == h.comp_write.load(.acquire)) return null;
        const v = self.compSlots()[r % h.comp_cap].load(.monotonic);
        h.comp_read.store(r + 1, .release);
        return v;
    }
};

fn connName(buf: []u8, svc: []const u8, pub_id: u64, sub_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}_p{d}_s{d}.conn", .{ svc, pub_id, sub_id });
}
fn dataName(buf: []u8, svc: []const u8, pub_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}_p{d}.data", .{ svc, pub_id });
}

// ---------------------------------------------------------------------------
// publisher
// ---------------------------------------------------------------------------

pub const SampleMut = struct {
    chunk: u32,
    payload: []u8,
};

pub const Publisher = struct {
    service: *Service,
    alloc: std.mem.Allocator,
    id: u64,
    data: Shm,
    num_chunks: u32,
    stride: u64,
    chunks_base: [*]u8,
    max_slice_len: usize,
    sequence: u64 = 0,
    loaned: u32 = 0,
    max_loaned: u32,
    safe_overflow: bool,
    sub_cap: u32,
    comp_cap: u32,

    // publisher-local bookkeeping (no atomics needed: only the publisher touches it)
    free: []u32,
    free_len: u32,
    refcount: []u32,

    seen_change: u64 = std.math.maxInt(u64),
    conns: [max_ports]?PubConn = @splat(null),

    const PubConn = struct {
        sub_id: u64,
        conn: Conn,
        in_flight: std.DynamicBitSetUnmanaged, // chunks delivered to this subscriber, not yet returned
    };

    fn init(service: *Service, alloc: std.mem.Allocator, max_slice_len: usize) !Publisher {
        const h = service.hdr();
        const id = try service.register(h.publishers[0..], h.max_publishers);
        errdefer service.unregister(h.publishers[0..], id);

        // same sizing rule as iceoryx2:
        // subscribers * (buffer + borrowed) + loaned + 1
        const num_chunks: u32 = h.max_subscribers * (h.subscriber_buffer_size + h.subscriber_max_borrowed) +
            h.publisher_max_loaned + 1;
        const stride = std.mem.alignForward(u64, @sizeOf(ChunkHeader) + max_slice_len, cache_line);
        const chunks_offset: u64 = std.heap.page_size_min;
        var nbuf: [200]u8 = undefined;
        var data = try Shm.create(try dataName(&nbuf, service.name(), id), chunks_offset + stride * num_chunks);
        errdefer data.deinit();
        const dh: *DataHeader = @ptrCast(@alignCast(data.mem.ptr));
        dh.num_chunks = num_chunks;
        dh.chunk_stride = stride;
        dh.chunks_offset = chunks_offset;
        dh.ready.store(ready_magic, .release);

        const free = try alloc.alloc(u32, num_chunks);
        errdefer alloc.free(free);
        const refcount = try alloc.alloc(u32, num_chunks);
        @memset(refcount, 0);
        for (free, 0..) |*f, i| f.* = @intCast(num_chunks - 1 - i);

        return .{
            .service = service,
            .alloc = alloc,
            .id = id,
            .data = data,
            .num_chunks = num_chunks,
            .stride = stride,
            .chunks_base = data.mem.ptr + chunks_offset,
            .max_slice_len = max_slice_len,
            .max_loaned = h.publisher_max_loaned,
            .safe_overflow = h.safe_overflow != 0,
            .sub_cap = h.subscriber_buffer_size,
            .comp_cap = h.subscriber_buffer_size + h.subscriber_max_borrowed,
            .free = free,
            .free_len = num_chunks,
            .refcount = refcount,
        };
    }

    pub fn deinit(self: *Publisher) void {
        for (&self.conns) |*c| if (c.*) |*pc| self.dropConn(pc, c);
        self.service.unregister(self.service.hdr().publishers[0..], self.id);
        self.data.deinit();
        self.alloc.free(self.free);
        self.alloc.free(self.refcount);
    }

    inline fn chunkPtr(self: *const Publisher, idx: u32) [*]u8 {
        return self.chunks_base + @as(usize, idx) * self.stride;
    }

    inline fn releaseChunk(self: *Publisher, idx: u32) void {
        self.refcount[idx] -= 1;
        if (self.refcount[idx] == 0) {
            self.free[self.free_len] = idx;
            self.free_len += 1;
        }
    }

    fn dropConn(self: *Publisher, pc: *PubConn, slot: *?PubConn) void {
        pc.conn.hdr().sender_closed.store(1, .release);
        var it = pc.in_flight.iterator(.{});
        while (it.next()) |i| self.releaseChunk(@intCast(i));
        pc.in_flight.deinit(self.alloc);
        pc.conn.deinit();
        slot.* = null;
    }

    fn updateConnections(self: *Publisher) void {
        const h = self.service.hdr();
        const cc = h.change_counter.load(.acquire);
        if (cc == self.seen_change) return;
        self.seen_change = cc;
        var created = false;

        for (&self.conns, 0..) |*slot, i| {
            const sub_id = h.subscribers[i].id.load(.acquire);
            if (slot.*) |*pc| {
                if (pc.sub_id == sub_id) continue;
                self.dropConn(pc, slot);
            }
            if (sub_id == 0) continue;
            var nbuf: [200]u8 = undefined;
            const n = connName(&nbuf, self.service.name(), self.id, sub_id) catch continue;
            var shm = Shm.create(n, Conn.size(self.sub_cap, self.comp_cap)) catch continue;
            const conn = Conn{ .shm = shm };
            const ch = conn.hdr();
            ch.sub_cap = self.sub_cap;
            ch.comp_cap = self.comp_cap;
            ch.ready.store(ready_magic, .release);
            const bits = std.DynamicBitSetUnmanaged.initEmpty(self.alloc, self.num_chunks) catch {
                shm.deinit();
                continue;
            };
            slot.* = .{ .sub_id = sub_id, .conn = conn, .in_flight = bits };
            created = true;
        }
        // tell subscribers that new connections exist
        if (created) self.seen_change = h.change_counter.fetchAdd(1, .acq_rel) + 1;
    }

    fn reclaim(self: *Publisher) void {
        for (&self.conns) |*c| if (c.*) |*pc| {
            while (pc.conn.compPop()) |idx| {
                pc.in_flight.unset(@intCast(idx));
                self.releaseChunk(@intCast(idx));
            }
        };
    }

    pub fn loanSliceUninit(self: *Publisher, len: usize) !SampleMut {
        if (len > self.max_slice_len) return error.SliceTooLarge;
        if (self.loaned >= self.max_loaned) return error.ExceedsMaxLoans;
        self.reclaim();
        if (self.free_len == 0) return error.OutOfMemory;
        self.free_len -= 1;
        const idx = self.free[self.free_len];
        self.refcount[idx] = 1;
        self.loaned += 1;
        const p = self.chunkPtr(idx);
        return .{ .chunk = idx, .payload = (p + @sizeOf(ChunkHeader))[0..len] };
    }

    /// Returns a loaned sample without sending it.
    pub fn release(self: *Publisher, s: SampleMut) void {
        self.loaned -= 1;
        self.releaseChunk(s.chunk);
    }

    /// Sends the sample to all connected subscribers; returns the number of recipients.
    pub fn send(self: *Publisher, s: SampleMut) usize {
        self.updateConnections();
        self.reclaim();
        const ch: *ChunkHeader = @ptrCast(@alignCast(self.chunkPtr(s.chunk)));
        ch.publisher_id = self.id;
        ch.sequence = self.sequence;
        ch.payload_len = s.payload.len;
        self.sequence += 1;
        self.loaned -= 1;

        var recipients: usize = 0;
        for (&self.conns) |*c| if (c.*) |*pc| {
            switch (pc.conn.subPush(s.chunk, self.safe_overflow)) {
                .full => continue,
                .ok => {},
                .evicted => |old| {
                    pc.in_flight.unset(@intCast(old));
                    self.releaseChunk(@intCast(old));
                },
            }
            pc.in_flight.set(s.chunk);
            self.refcount[s.chunk] += 1;
            recipients += 1;
        };
        // drop the "loan" reference
        self.releaseChunk(s.chunk);
        return recipients;
    }
};

// ---------------------------------------------------------------------------
// subscriber
// ---------------------------------------------------------------------------

pub const Sample = struct {
    conn_slot: u32,
    chunk: u32,
    header: *const ChunkHeader,
    payload: []const u8,
};

pub const Subscriber = struct {
    service: *Service,
    id: u64,
    max_borrowed: u32,
    borrowed: u32 = 0,
    seen_change: u64 = std.math.maxInt(u64),
    next: u32 = 0,
    conns: [max_ports]?SubConn = @splat(null),

    const SubConn = struct {
        pub_id: u64,
        conn: Conn,
        data: Shm,
        chunks_base: [*]const u8,
        stride: u64,
    };

    fn init(service: *Service, alloc: std.mem.Allocator) !Subscriber {
        _ = alloc;
        const h = service.hdr();
        const id = try service.register(h.subscribers[0..], h.max_subscribers);
        return .{ .service = service, .id = id, .max_borrowed = h.subscriber_max_borrowed };
    }

    pub fn deinit(self: *Subscriber) void {
        for (&self.conns) |*c| if (c.*) |*sc| {
            sc.conn.deinit();
            sc.data.deinit();
            c.* = null;
        };
        self.service.unregister(self.service.hdr().subscribers[0..], self.id);
    }

    fn connect(self: *Subscriber, pub_id: u64) ?SubConn {
        var nbuf: [200]u8 = undefined;
        const cn = connName(&nbuf, self.service.name(), pub_id, self.id) catch return null;
        var conn_shm = Shm.open(cn, true) catch return null;
        const conn = Conn{ .shm = conn_shm };
        waitReady(&conn.hdr().ready) catch {
            conn_shm.deinit();
            return null;
        };
        const dn = dataName(&nbuf, self.service.name(), pub_id) catch return null;
        var data = Shm.open(dn, false) catch {
            conn_shm.deinit();
            return null;
        };
        const dh: *const DataHeader = @ptrCast(@alignCast(data.mem.ptr));
        waitReady(&dh.ready) catch {
            data.deinit();
            conn_shm.deinit();
            return null;
        };
        return .{
            .pub_id = pub_id,
            .conn = conn,
            .data = data,
            .chunks_base = data.mem.ptr + dh.chunks_offset,
            .stride = dh.chunk_stride,
        };
    }

    fn updateConnections(self: *Subscriber) void {
        const h = self.service.hdr();
        const cc = h.change_counter.load(.acquire);
        if (cc == self.seen_change) return;
        self.seen_change = cc;
        for (&self.conns, 0..) |*slot, i| {
            const pub_id = h.publishers[i].id.load(.acquire);
            if (slot.*) |*sc| {
                if (sc.pub_id == pub_id) continue;
                sc.conn.deinit();
                sc.data.deinit();
                slot.* = null;
            }
            if (pub_id == 0) continue;
            // may fail if the publisher has not created the connection yet; the
            // publisher bumps the change counter once it did, so we retry then.
            slot.* = self.connect(pub_id);
        }
    }

    pub fn receive(self: *Subscriber) !?Sample {
        self.updateConnections();
        if (self.borrowed >= self.max_borrowed) return error.ExceedsMaxBorrows;
        var n: u32 = 0;
        while (n < max_ports) : (n += 1) {
            const i = (self.next + n) % max_ports;
            if (self.conns[i]) |*sc| {
                if (sc.conn.subPop()) |idx| {
                    self.next = i + 1;
                    self.borrowed += 1;
                    const p = sc.chunks_base + idx * sc.stride;
                    const ch: *const ChunkHeader = @ptrCast(@alignCast(p));
                    return .{
                        .conn_slot = i,
                        .chunk = @intCast(idx),
                        .header = ch,
                        .payload = (p + @sizeOf(ChunkHeader))[0..ch.payload_len],
                    };
                }
            }
        }
        return null;
    }

    pub fn release(self: *Subscriber, s: Sample) void {
        self.borrowed -= 1;
        if (self.conns[s.conn_slot]) |*sc| _ = sc.conn.compPush(s.chunk);
    }
};

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

test "pub/sub roundtrip, overflow and chunk recycling" {
    removeStale(prefix ++ "test_");
    const a = std.testing.allocator;
    var svc = try Service.create("test_svc", .{ .max_publishers = 1, .max_subscribers = 2, .subscriber_max_buffer_size = 1 });
    defer svc.deinit();
    var p = try svc.publisher(a, 64);
    defer p.deinit();
    var s1 = try svc.subscriber(a);
    defer s1.deinit();
    var s2 = try svc.subscriber(a);
    defer s2.deinit();

    // many sends with nobody reading must never exhaust the pool (safe overflow)
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        const m = try p.loanSliceUninit(8);
        std.mem.writeInt(u64, m.payload[0..8], i, .little);
        try std.testing.expectEqual(@as(usize, 2), p.send(m));
    }
    // only the newest sample survives (buffer size 1)
    const r1 = (try s1.receive()).?;
    try std.testing.expectEqual(@as(u64, 999), std.mem.readInt(u64, r1.payload[0..8], .little));
    try std.testing.expect((try s1.receive()) == null);
    s1.release(r1);
    const r2 = (try s2.receive()).?;
    try std.testing.expectEqual(@as(u64, 999), r2.header.sequence);
    s2.release(r2);

    // all chunks must come back
    _ = try p.loanSliceUninit(1);
    try std.testing.expectEqual(p.num_chunks - 1, p.free_len);
}
