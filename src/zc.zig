//! zc (v2) — a zero-copy shared-memory publish/subscribe IPC library in Zig,
//! modelled on the architecture of eclipse-iceoryx/iceoryx2:
//!
//!   * Service segment  (/dev/shm/<prefix><svc>.service)
//!       static config + dynamic config (publisher / subscriber port slots) +
//!       a change counter that tells ports to update their connections.
//!   * Data segment per publisher (/dev/shm/<prefix><svc>_p<id>.data)
//!       fixed-size chunks written once and read in place by every subscriber;
//!       only the owning publisher allocates (local free list, local ref counts).
//!   * Connection per publisher/subscriber pair (/dev/shm/<prefix><svc>_p<id>_s<id>.conn)
//!       - delivery ring: sequence-tagged cells {seq, chunk|len}. A cell is ready
//!         for position p when seq == p+1, so the subscriber polls the cell itself
//!         and one cache line carries both the "ready" signal and the reference.
//!       - return ring: chunk indices the subscriber is done with.
//!
//! v2 vs v1 — built to move as few cache lines between cores as possible:
//!   * no shared write index: readiness lives in the cell (v1: separate index line)
//!   * the publisher keeps its write position and a cached copy of the reader's
//!     position locally; it re-reads the shared read index only when the ring
//!     looks full (every `ring_size` sends at most, instead of every send)
//!   * iceoryx2's subscriber buffer size ("keep the newest N, drop older") is
//!     enforced by the subscriber on receive, so the publisher needs no
//!     per-send handshake; the ring is deeper than the buffer to make that work
//!   * returned chunks are collected lazily, in a batch, only when the
//!     publisher's free list is empty (v1: polled on every loan and send)
//!   * the payload length travels in the cell, so receiving never touches the
//!     chunk header line the publisher writes
//!
//! Not implemented (vs. iceoryx2): nodes / dead-node cleanup, events & waitsets,
//! request-response, history, user headers, dynamic (re)allocation of slices,
//! process-local service variants, typed payloads.

const std = @import("std");
const posix = std.posix;
const Atomic = std.atomic.Value;

pub const prefix = "iox2zig_";
pub const max_ports = 16;
const cache_line = 64;
const ready_magic: u32 = 0x1CE0_2217;

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
    ring_size: u32,
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
    /// Depth of each connection's delivery ring (rounded up to a power of two,
    /// at least 2 * buffer size). Deeper = fewer cross-core syncs, more chunks.
    ring_size: u32 = 32,
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
        if (cfg.subscriber_max_buffer_size == 0) return error.InvalidBufferSize;
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
        h.ring_size = std.math.ceilPowerOfTwoAssert(u32, @max(cfg.ring_size, 2 * cfg.subscriber_max_buffer_size));
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
// connection
// ---------------------------------------------------------------------------

/// One delivery-ring cell. Ready for ring position p when seq == p + 1.
const Cell = extern struct {
    seq: Atomic(u64),
    val: Atomic(u64), // chunk index | payload_len << 32
};

inline fn pack(idx: u32, len: usize) u64 {
    return @as(u64, idx) | (@as(u64, @intCast(len)) << 32);
}

const ConnHeader = extern struct {
    ready: Atomic(u32),
    ring_size: u32, // delivery cells (power of two)
    ret_size: u32, // return slots (power of two)
    buffer_size: u32,
    overflow: u32,
    sender_closed: Atomic(u32),
    _p0: [cache_line - 24]u8,
    // subscriber-owned (publisher reads it only when the ring looks full,
    // and CASes it only to evict on overflow)
    read: Atomic(u64),
    _p1: [cache_line - 8]u8,
    // return ring: producer = subscriber, consumer = publisher
    ret_write: Atomic(u64),
    _p2: [cache_line - 8]u8,
    ret_read: Atomic(u64),
    _p3: [cache_line - 8]u8,
    // followed by: cells [ring_size]Cell, return slots [ret_size]Atomic(u64)
};

const Conn = struct {
    shm: Shm,
    cells: [*]Cell,
    ret: [*]Atomic(u64),
    ring_mask: u64,
    ret_mask: u64,

    fn size(ring_size: u32, ret_size: u32) usize {
        return @sizeOf(ConnHeader) + @as(usize, ring_size) * @sizeOf(Cell) + @as(usize, ret_size) * 8;
    }

    fn fromShm(shm: Shm) Conn {
        const h: *ConnHeader = @ptrCast(@alignCast(shm.mem.ptr));
        const cells: [*]Cell = @ptrCast(@alignCast(shm.mem.ptr + @sizeOf(ConnHeader)));
        return .{
            .shm = shm,
            .cells = cells,
            .ret = @ptrCast(@alignCast(cells + h.ring_size)),
            .ring_mask = h.ring_size - 1,
            .ret_mask = h.ret_size - 1,
        };
    }

    fn deinit(self: *Conn) void {
        self.shm.deinit();
    }

    inline fn hdr(self: *const Conn) *ConnHeader {
        return @ptrCast(@alignCast(self.shm.mem.ptr));
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
    ring_size: u32,
    ret_size: u32,
    buffer_size: u32,

    // publisher-local bookkeeping (no atomics: only the publisher touches it)
    free: []u32,
    free_len: u32,
    refcount: []u32,

    seen_change: u64 = std.math.maxInt(u64),
    conns: [max_ports]?PubConn = @splat(null),

    const PubConn = struct {
        sub_id: u64,
        conn: Conn,
        write: u64 = 0, // next ring position (local: no shared write index)
        cached_read: u64 = 0, // last seen subscriber read position
        ret_read: u64 = 0, // return ring position (local copy, published on drain)
        in_flight: std.DynamicBitSetUnmanaged, // delivered to this subscriber, not yet returned
    };

    fn init(service: *Service, alloc: std.mem.Allocator, max_slice_len: usize) !Publisher {
        const h = service.hdr();
        const id = try service.register(h.publishers[0..], h.max_publishers);
        errdefer service.unregister(h.publishers[0..], id);

        // every subscriber can hold a full ring plus its borrowed samples
        const per_sub = h.ring_size + h.subscriber_max_borrowed;
        const num_chunks: u32 = h.max_subscribers * per_sub + h.publisher_max_loaned + 1;
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
            .ring_size = h.ring_size,
            .ret_size = std.math.ceilPowerOfTwoAssert(u32, per_sub + 1),
            .buffer_size = h.subscriber_buffer_size,
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
            var shm = Shm.create(n, Conn.size(self.ring_size, self.ret_size)) catch continue;
            const ch: *ConnHeader = @ptrCast(@alignCast(shm.mem.ptr));
            ch.ring_size = self.ring_size;
            ch.ret_size = self.ret_size;
            ch.buffer_size = self.buffer_size;
            ch.overflow = @intFromBool(self.safe_overflow);
            ch.ready.store(ready_magic, .release);
            const bits = std.DynamicBitSetUnmanaged.initEmpty(self.alloc, self.num_chunks) catch {
                shm.deinit();
                continue;
            };
            slot.* = .{ .sub_id = sub_id, .conn = Conn.fromShm(shm), .in_flight = bits };
            created = true;
        }
        // tell subscribers that new connections exist
        if (created) self.seen_change = h.change_counter.fetchAdd(1, .acq_rel) + 1;
    }

    /// Collects every chunk the subscribers handed back. Only called when the
    /// free list runs dry, so the return rings are read in batches.
    fn reclaim(self: *Publisher) void {
        for (&self.conns) |*c| if (c.*) |*pc| {
            const h = pc.conn.hdr();
            const w = h.ret_write.load(.acquire);
            var r = pc.ret_read;
            if (r == w) continue;
            while (r != w) : (r += 1) {
                const idx: u32 = @intCast(pc.conn.ret[r & pc.conn.ret_mask].load(.monotonic));
                pc.in_flight.unset(idx);
                self.releaseChunk(idx);
            }
            pc.ret_read = r;
            h.ret_read.store(r, .release);
        };
    }

    pub fn loanSliceUninit(self: *Publisher, len: usize) !SampleMut {
        if (len > self.max_slice_len) return error.SliceTooLarge;
        if (self.loaned >= self.max_loaned) return error.ExceedsMaxLoans;
        if (self.free_len == 0) {
            self.reclaim();
            if (self.free_len == 0) return error.OutOfMemory;
        }
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

    /// Places `val` at the ring's next position. On a full ring: evicts the
    /// oldest entry (safe overflow) or reports .full.
    inline fn push(self: *Publisher, pc: *PubConn, val: u64) bool {
        const h = pc.conn.hdr();
        const w = pc.write;
        // without overflow the publisher must honour the subscriber's buffer size itself
        const limit: u64 = if (self.safe_overflow) self.ring_size else self.buffer_size;
        if (w - pc.cached_read >= limit) {
            pc.cached_read = h.read.load(.acquire);
            while (w - pc.cached_read >= limit) {
                if (!self.safe_overflow) return false;
                // the subscriber is not keeping up: take the oldest entry back
                const r = pc.cached_read;
                const old = pc.conn.cells[r & pc.conn.ring_mask].val.load(.monotonic);
                if (h.read.cmpxchgStrong(r, r + 1, .acq_rel, .acquire)) |actual| {
                    pc.cached_read = actual; // the subscriber took it first
                } else {
                    const idx: u32 = @truncate(old);
                    pc.in_flight.unset(idx);
                    self.releaseChunk(idx);
                    pc.cached_read = r + 1;
                }
            }
        }
        const cell = &pc.conn.cells[w & pc.conn.ring_mask];
        cell.val.store(val, .monotonic);
        cell.seq.store(w + 1, .release);
        pc.write = w + 1;
        return true;
    }

    /// Sends the sample to all connected subscribers; returns the number of recipients.
    pub fn send(self: *Publisher, s: SampleMut) usize {
        self.updateConnections();
        const ch: *ChunkHeader = @ptrCast(@alignCast(self.chunkPtr(s.chunk)));
        ch.publisher_id = self.id;
        ch.sequence = self.sequence;
        ch.payload_len = s.payload.len;
        self.sequence += 1;
        self.loaned -= 1;

        const val = pack(s.chunk, s.payload.len);
        var recipients: usize = 0;
        for (&self.conns) |*c| if (c.*) |*pc| {
            // count the reference first: an eviction inside push() may release it
            self.refcount[s.chunk] += 1;
            if (!self.push(pc, val)) {
                self.refcount[s.chunk] -= 1;
                continue;
            }
            pc.in_flight.set(s.chunk);
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
    /// Pointer only; nothing reads it unless the caller does.
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
        buffer_size: u64,
        overflow: bool,
        ret_write: u64 = 0, // local copy (only the subscriber writes it)
        cached_ret_read: u64 = 0,

        /// Hands a chunk back to the publisher.
        inline fn giveBack(sc: *SubConn, idx: u64) void {
            const h = sc.conn.hdr();
            const w = sc.ret_write;
            if (w - sc.cached_ret_read > sc.conn.ret_mask) {
                // sized to hold every chunk this connection can own; only spins if the
                // publisher has not drained yet
                while (true) {
                    sc.cached_ret_read = h.ret_read.load(.acquire);
                    if (w - sc.cached_ret_read <= sc.conn.ret_mask) break;
                    std.atomic.spinLoopHint();
                }
            }
            sc.conn.ret[w & sc.conn.ret_mask].store(idx, .monotonic);
            sc.ret_write = w + 1;
            h.ret_write.store(w + 1, .release);
        }

        /// Takes the next entry, first discarding entries older than the newest
        /// `buffer_size` ones (iceoryx2's safe-overflow semantics).
        inline fn take(sc: *SubConn) ?u64 {
            const h = sc.conn.hdr();
            const mask = sc.conn.ring_mask;
            var r = h.read.load(.monotonic);
            while (true) {
                const cell = &sc.conn.cells[r & mask];
                const seq = cell.seq.load(.acquire);
                if (seq != r + 1) {
                    if (seq > r + 1) { // the publisher evicted past us
                        r = h.read.load(.acquire);
                        continue;
                    }
                    return null;
                }
                const val = cell.val.load(.monotonic);
                const drop = sc.overflow and
                    sc.conn.cells[(r + sc.buffer_size) & mask].seq.load(.acquire) == r + sc.buffer_size + 1;
                if (h.read.cmpxchgWeak(r, r + 1, .acq_rel, .monotonic)) |actual| {
                    r = actual;
                    continue;
                }
                if (!drop) return val;
                sc.giveBack(@as(u32, @truncate(val)));
                r += 1;
            }
        }
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
        const ch: *const ConnHeader = @ptrCast(@alignCast(conn_shm.mem.ptr));
        waitReady(&ch.ready) catch {
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
            .conn = Conn.fromShm(conn_shm),
            .data = data,
            .chunks_base = data.mem.ptr + dh.chunks_offset,
            .stride = dh.chunk_stride,
            .buffer_size = ch.buffer_size,
            .overflow = ch.overflow != 0,
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
                if (sc.take()) |val| {
                    self.next = i;
                    self.borrowed += 1;
                    const idx: u32 = @truncate(val);
                    const len: usize = @intCast(val >> 32);
                    const p = sc.chunks_base + idx * sc.stride;
                    return .{
                        .conn_slot = i,
                        .chunk = idx,
                        .header = @ptrCast(@alignCast(p)),
                        .payload = (p + @sizeOf(ChunkHeader))[0..len],
                    };
                }
            }
        }
        return null;
    }

    pub fn release(self: *Subscriber, s: Sample) void {
        self.borrowed -= 1;
        if (self.conns[s.conn_slot]) |*sc| sc.giveBack(s.chunk);
    }
};

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn readU64(s: Sample) u64 {
    return std.mem.readInt(u64, s.payload[0..8], .little);
}

fn sendU64(p: *Publisher, v: u64) !usize {
    const m = try p.loanSliceUninit(8);
    std.mem.writeInt(u64, m.payload[0..8], v, .little);
    return p.send(m);
}

test "safe overflow keeps the newest sample and recycles every chunk" {
    removeStale(prefix ++ "test_");
    const a = testing.allocator;
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
    while (i < 1000) : (i += 1) try testing.expectEqual(@as(usize, 2), try sendU64(&p, i));

    // only the newest sample survives (buffer size 1)
    const r1 = (try s1.receive()).?;
    try testing.expectEqual(@as(u64, 999), readU64(r1));
    try testing.expect((try s1.receive()) == null);
    s1.release(r1);
    const r2 = (try s2.receive()).?;
    try testing.expectEqual(@as(u64, 999), r2.header.sequence);
    s2.release(r2);

    // all chunks must come back
    _ = try p.loanSliceUninit(1);
    p.reclaim();
    try testing.expectEqual(p.num_chunks - 1, p.free_len);
}

test "buffer size N delivers the newest N in order" {
    removeStale(prefix ++ "test2_");
    const a = testing.allocator;
    var svc = try Service.create("test2_svc", .{ .max_publishers = 1, .max_subscribers = 1, .subscriber_max_buffer_size = 3 });
    defer svc.deinit();
    var p = try svc.publisher(a, 64);
    defer p.deinit();
    var s = try svc.subscriber(a);
    defer s.deinit();

    var i: u64 = 0;
    while (i < 100) : (i += 1) _ = try sendU64(&p, i);
    for ([_]u64{ 97, 98, 99 }) |want| {
        const r = (try s.receive()).?;
        try testing.expectEqual(want, readU64(r));
        s.release(r);
    }
    try testing.expect((try s.receive()) == null);
}

test "without safe overflow a full buffer refuses delivery" {
    removeStale(prefix ++ "test3_");
    const a = testing.allocator;
    var svc = try Service.create("test3_svc", .{ .max_publishers = 1, .max_subscribers = 1, .subscriber_max_buffer_size = 2, .enable_safe_overflow = false });
    defer svc.deinit();
    var p = try svc.publisher(a, 64);
    defer p.deinit();
    var s = try svc.subscriber(a);
    defer s.deinit();

    try testing.expectEqual(@as(usize, 1), try sendU64(&p, 1));
    try testing.expectEqual(@as(usize, 1), try sendU64(&p, 2));
    try testing.expectEqual(@as(usize, 0), try sendU64(&p, 3));
    for ([_]u64{ 1, 2 }) |want| {
        const r = (try s.receive()).?;
        try testing.expectEqual(want, readU64(r));
        s.release(r);
    }
    try testing.expectEqual(@as(usize, 1), try sendU64(&p, 4));
}

test "concurrent stress: ordered, no loss without overflow, no leaks" {
    removeStale(prefix ++ "test4_");
    const a = std.heap.page_allocator;
    var svc = try Service.create("test4_svc", .{ .max_publishers = 1, .max_subscribers = 1, .subscriber_max_buffer_size = 4, .enable_safe_overflow = false });
    defer svc.deinit();
    var s = try svc.subscriber(a);
    defer s.deinit();
    const n: u64 = 200_000;

    const Producer = struct {
        fn run(sv: *Service) void {
            var p = sv.publisher(std.heap.page_allocator, 64) catch unreachable;
            defer p.deinit();
            var v: u64 = 0;
            while (v < n) {
                const m = p.loanSliceUninit(8) catch |e| switch (e) {
                    error.OutOfMemory => continue,
                    else => unreachable,
                };
                std.mem.writeInt(u64, m.payload[0..8], v, .little);
                if (p.send(m) == 1) v += 1;
            }
            // wait until the subscriber gave everything back
            while (true) {
                p.reclaim();
                if (p.free_len == p.num_chunks) break;
                std.atomic.spinLoopHint();
            }
        }
    };
    const t = try std.Thread.spawn(.{}, Producer.run, .{&svc});
    var expect: u64 = 0;
    while (expect < n) {
        if (try s.receive()) |r| {
            try testing.expectEqual(expect, readU64(r));
            s.release(r);
            expect += 1;
        }
    }
    t.join();
}

test "concurrent stress with overflow: monotonic, newest arrives, no leaks" {
    removeStale(prefix ++ "test5_");
    const a = std.heap.page_allocator;
    var svc = try Service.create("test5_svc", .{ .max_publishers = 1, .max_subscribers = 1, .subscriber_max_buffer_size = 1, .ring_size = 4 });
    defer svc.deinit();
    var s = try svc.subscriber(a);
    defer s.deinit();
    const n: u64 = 300_000;
    var done = Atomic(bool).init(false);

    const Producer = struct {
        fn run(sv: *Service, d: *Atomic(bool)) void {
            var p = sv.publisher(std.heap.page_allocator, 64) catch unreachable;
            defer p.deinit();
            var v: u64 = 0;
            while (v < n) : (v += 1) _ = sendU64(&p, v) catch unreachable;
            d.store(true, .release);
            while (true) { // the last sample may still be in the ring
                p.reclaim();
                if (p.free_len + 1 >= p.num_chunks) break;
                std.atomic.spinLoopHint();
            }
        }
    };
    const t = try std.Thread.spawn(.{}, Producer.run, .{ &svc, &done });
    var last: ?u64 = null;
    while (true) {
        if (try s.receive()) |r| {
            const v = readU64(r);
            if (last) |l| try testing.expect(v > l);
            last = v;
            s.release(r);
            if (v == n - 1) break;
        } else if (done.load(.acquire) and last != null and last.? == n - 1) break;
    }
    t.join();
}
