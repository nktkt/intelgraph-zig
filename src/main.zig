const std = @import("std");

const Allocator = std.mem.Allocator;
const default_db_path = ".intelgraph/events.tsv";
const max_line_bytes = 1024 * 1024;
const max_store_bytes = 256 * 1024 * 1024;

const Entity = struct {
    kind: []const u8,
    value: []const u8,
};

const Event = struct {
    id: u64,
    timestamp: []const u8,
    source: []const u8,
    text: []const u8,
    entities: []Entity,
};

const EventSet = struct {
    arena: std.heap.ArenaAllocator,
    events: []Event,

    fn deinit(self: *EventSet) void {
        self.arena.deinit();
    }
};

const IngestStats = struct {
    ingested: u64 = 0,
    with_entities: u64 = 0,
};

const CountItem = struct {
    key: []const u8,
    count: u64,
};

const Cli = struct {
    db_path: []const u8 = default_db_path,
    command_index: usize = 1,
};

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = debug_allocator.deinit();
    const gpa = debug_allocator.allocator();

    const args = try std.process.argsAlloc(gpa);
    defer std.process.argsFree(gpa, args);

    const cli = try parseGlobalArgs(args);
    if (cli.command_index >= args.len) {
        try printUsage();
        return;
    }

    const command = args[cli.command_index];
    const rest = args[cli.command_index + 1 ..];

    if (std.mem.eql(u8, command, "help") or std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h")) {
        try printUsage();
    } else if (std.mem.eql(u8, command, "init")) {
        try cmdInit(cli.db_path);
    } else if (std.mem.eql(u8, command, "ingest")) {
        try cmdIngest(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "stats")) {
        try cmdStats(gpa, cli.db_path);
    } else if (std.mem.eql(u8, command, "search")) {
        try cmdSearch(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "entity")) {
        try cmdEntity(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "timeline")) {
        try cmdTimeline(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "path")) {
        try cmdPath(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "rank")) {
        try cmdRank(gpa, cli.db_path, rest);
    } else if (std.mem.eql(u8, command, "export")) {
        try cmdExport(gpa, cli.db_path, rest);
    } else {
        try fail("unknown command: {s}", .{command});
    }
}

fn parseGlobalArgs(args: []const []const u8) !Cli {
    var cli = Cli{};
    var i: usize = 1;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--db")) {
            if (i + 1 >= args.len) return error.MissingDbPath;
            cli.db_path = args[i + 1];
            i += 2;
            continue;
        }
        cli.command_index = i;
        return cli;
    }
    cli.command_index = i;
    return cli;
}

fn printUsage() !void {
    var buf: [4096]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;
    try out.writeAll(
        \\intel - local-first intelligence graph
        \\
        \\Usage:
        \\  intel [--db PATH] init
        \\  intel [--db PATH] ingest <file> [--source NAME]
        \\  intel [--db PATH] search <text>
        \\  intel [--db PATH] entity <value|kind:value>
        \\  intel [--db PATH] timeline [--entity <value|kind:value>]
        \\  intel [--db PATH] path <from> <to>
        \\  intel [--db PATH] rank entities [--kind KIND] [--limit N]
        \\  intel [--db PATH] rank edges [--limit N]
        \\  intel [--db PATH] export graph [--format dot] [--out file.dot]
        \\  intel [--db PATH] stats
        \\
        \\Examples:
        \\  intel ingest examples/access.log
        \\  intel entity ip:10.0.0.12
        \\  intel rank entities --kind domain --limit 10
        \\  intel path alice@example.com suspicious.example
        \\  intel export graph --out graph.dot
        \\
    );
    try out.flush();
}

fn cmdInit(db_path: []const u8) !void {
    try ensureStore(db_path);
    var buf: [1024]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;
    try out.print("initialized store at {s}\n", .{db_path});
    try out.flush();
}

fn cmdIngest(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 1) return fail("ingest requires a file path", .{});
    const input_path = args[0];
    var source = input_path;

    var i: usize = 1;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--source")) {
            if (i + 1 >= args.len) return fail("--source requires a value", .{});
            source = args[i + 1];
            i += 2;
        } else {
            return fail("unknown ingest option: {s}", .{args[i]});
        }
    }

    try ensureStore(db_path);

    var input_file = try std.fs.cwd().openFile(input_path, .{});
    defer input_file.close();

    var file = try std.fs.cwd().createFile(db_path, .{ .read = true, .truncate = false });
    defer file.close();
    try file.seekFromEnd(0);
    var out_buf: [8192]u8 = undefined;
    var file_writer = file.writer(&out_buf);
    const out = &file_writer.interface;

    var next_id = try countExistingEvents(db_path) + 1;
    var stats = IngestStats{};

    var read_buf: [64 * 1024]u8 = undefined;
    var line_buf: std.ArrayList(u8) = .empty;
    defer line_buf.deinit(gpa);

    while (true) {
        const n = try input_file.read(&read_buf);
        if (n == 0) break;
        for (read_buf[0..n]) |byte| {
            if (byte == '\n') {
                try ingestLine(gpa, out, &next_id, source, line_buf.items, &stats);
                line_buf.clearRetainingCapacity();
            } else {
                if (line_buf.items.len >= max_line_bytes) {
                    return fail("input line exceeds {d} bytes", .{max_line_bytes});
                }
                try line_buf.append(gpa, byte);
            }
        }
    }
    if (line_buf.items.len > 0) {
        try ingestLine(gpa, out, &next_id, source, line_buf.items, &stats);
    }
    try out.flush();

    var buf: [1024]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const stdout = &writer.interface;
    try stdout.print("ingested {d} events ({d} with entities) into {s}\n", .{ stats.ingested, stats.with_entities, db_path });
    try stdout.flush();
}

fn ingestLine(
    gpa: Allocator,
    writer: *std.Io.Writer,
    next_id: *u64,
    source: []const u8,
    raw_line: []const u8,
    stats: *IngestStats,
) !void {
    const line = std.mem.trimRight(u8, raw_line, "\r");
    if (std.mem.trim(u8, line, " \t").len == 0) return;

    const timestamp = try extractTimestamp(gpa, line);
    defer gpa.free(timestamp);

    const entities = try extractEntities(gpa, line);
    defer freeEntities(gpa, entities);

    if (entities.len > 0) stats.with_entities += 1;

    try appendEvent(writer, next_id.*, timestamp, source, line, entities);
    next_id.* += 1;
    stats.ingested += 1;
}

fn cmdStats(gpa: Allocator, db_path: []const u8) !void {
    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var unique_entities = std.StringHashMap(void).init(gpa);
    defer unique_entities.deinit();
    var by_kind = std.StringHashMap(u64).init(gpa);
    defer by_kind.deinit();

    for (set.events) |event| {
        for (event.entities) |entity| {
            const key = try entityKeyAlloc(gpa, entity);
            if (unique_entities.contains(key)) {
                gpa.free(key);
            } else {
                try unique_entities.put(key, {});
            }

            if (by_kind.getPtr(entity.kind)) |count| {
                count.* += 1;
            } else {
                try by_kind.put(entity.kind, 1);
            }
        }
    }

    defer {
        var keys = unique_entities.keyIterator();
        while (keys.next()) |key| gpa.free(key.*);
    }

    var buf: [4096]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;
    try out.print("events: {d}\nunique_entities: {d}\n", .{ set.events.len, unique_entities.count() });

    var iter = by_kind.iterator();
    while (iter.next()) |entry| {
        try out.print("{s}: {d}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
    try out.flush();
}

fn cmdSearch(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 1) return fail("search requires text", .{});
    const query = args[0];

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var buf: [8192]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    var shown: usize = 0;
    for (set.events) |event| {
        if (!eventContains(event, query)) continue;
        try printEventSummary(out, event);
        shown += 1;
    }
    try out.print("matches: {d}\n", .{shown});
    try out.flush();
}

fn cmdEntity(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 1) return fail("entity requires a value or kind:value", .{});
    const query = args[0];

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var neighbors = std.StringHashMap(u64).init(gpa);
    defer {
        var keys = neighbors.keyIterator();
        while (keys.next()) |key| gpa.free(key.*);
        neighbors.deinit();
    }

    var buf: [8192]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    var hits: usize = 0;
    for (set.events) |event| {
        if (!eventHasEntity(event, query)) continue;
        hits += 1;
        for (event.entities) |entity| {
            if (entityMatches(entity, query)) continue;
            const key = try entityKeyAlloc(gpa, entity);
            if (neighbors.getPtr(key)) |count| {
                count.* += 1;
                gpa.free(key);
            } else {
                try neighbors.put(key, 1);
            }
        }
        try printEventSummary(out, event);
    }

    try out.print("\nevents: {d}\nneighbors:\n", .{hits});
    var iter = neighbors.iterator();
    while (iter.next()) |entry| {
        try out.print("  {s} ({d})\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
    try out.flush();
}

fn cmdTimeline(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    var entity_filter: ?[]const u8 = null;
    var i: usize = 0;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--entity")) {
            if (i + 1 >= args.len) return fail("--entity requires a value", .{});
            entity_filter = args[i + 1];
            i += 2;
        } else {
            return fail("unknown timeline option: {s}", .{args[i]});
        }
    }

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    std.mem.sort(Event, set.events, {}, eventLessThan);

    var buf: [8192]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    var shown: usize = 0;
    for (set.events) |event| {
        if (entity_filter) |filter| {
            if (!eventHasEntity(event, filter)) continue;
        }
        try printEventSummary(out, event);
        shown += 1;
    }
    try out.print("events: {d}\n", .{shown});
    try out.flush();
}

fn cmdPath(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 2) return fail("path requires <from> <to>", .{});
    const from_query = args[0];
    const to_query = args[1];

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var graph = std.StringHashMap(std.ArrayList([]const u8)).init(gpa);
    defer {
        var iter = graph.iterator();
        while (iter.next()) |entry| entry.value_ptr.deinit(gpa);
        graph.deinit();
    }

    var canonical_from: ?[]const u8 = null;
    var canonical_to: ?[]const u8 = null;

    for (set.events) |event| {
        for (event.entities) |entity| {
            const key = try entityKeyAlloc(set.arena.allocator(), entity);
            if (canonical_from == null and entityMatches(entity, from_query)) canonical_from = key;
            if (canonical_to == null and entityMatches(entity, to_query)) canonical_to = key;
            _ = try graph.getOrPutValue(key, .empty);
        }

        var i: usize = 0;
        while (i < event.entities.len) : (i += 1) {
            var j = i + 1;
            while (j < event.entities.len) : (j += 1) {
                const a = try entityKeyAlloc(set.arena.allocator(), event.entities[i]);
                const b = try entityKeyAlloc(set.arena.allocator(), event.entities[j]);
                try addAdjacency(gpa, &graph, a, b);
                try addAdjacency(gpa, &graph, b, a);
            }
        }
    }

    if (canonical_from == null) return fail("from entity not found: {s}", .{from_query});
    if (canonical_to == null) return fail("to entity not found: {s}", .{to_query});

    const path = try shortestPath(gpa, &graph, canonical_from.?, canonical_to.?);
    defer gpa.free(path);

    var buf: [4096]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    if (path.len == 0) {
        try out.print("no path between {s} and {s}\n", .{ canonical_from.?, canonical_to.? });
    } else {
        for (path, 0..) |node, idx| {
            if (idx > 0) try out.writeAll(" -> ");
            try out.writeAll(node);
        }
        try out.writeByte('\n');
    }
    try out.flush();
}

fn cmdRank(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 1) return fail("rank requires entities or edges", .{});
    if (std.mem.eql(u8, args[0], "entities")) {
        try cmdRankEntities(gpa, db_path, args[1..]);
    } else if (std.mem.eql(u8, args[0], "edges")) {
        try cmdRankEdges(gpa, db_path, args[1..]);
    } else {
        return fail("unknown rank target: {s}", .{args[0]});
    }
}

fn cmdRankEntities(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    var limit: usize = 20;
    var kind_filter: ?[]const u8 = null;

    var i: usize = 0;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--limit")) {
            if (i + 1 >= args.len) return fail("--limit requires a value", .{});
            limit = try parseLimit(args[i + 1]);
            i += 2;
        } else if (std.mem.eql(u8, args[i], "--kind")) {
            if (i + 1 >= args.len) return fail("--kind requires a value", .{});
            kind_filter = args[i + 1];
            i += 2;
        } else {
            return fail("unknown rank entities option: {s}", .{args[i]});
        }
    }

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var counts = std.StringHashMap(u64).init(gpa);
    defer {
        var keys = counts.keyIterator();
        while (keys.next()) |key| gpa.free(key.*);
        counts.deinit();
    }

    for (set.events) |event| {
        for (event.entities) |entity| {
            if (kind_filter) |kind| {
                if (!std.ascii.eqlIgnoreCase(entity.kind, kind)) continue;
            }

            const key = try entityKeyAlloc(gpa, entity);
            if (counts.getPtr(key)) |count| {
                count.* += 1;
                gpa.free(key);
            } else {
                try counts.put(key, 1);
            }
        }
    }

    var items: std.ArrayList(CountItem) = .empty;
    defer items.deinit(gpa);

    var iter = counts.iterator();
    while (iter.next()) |entry| {
        try items.append(gpa, .{ .key = entry.key_ptr.*, .count = entry.value_ptr.* });
    }
    std.mem.sort(CountItem, items.items, {}, countItemGreater);

    var buf: [8192]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    const shown = @min(limit, items.items.len);
    for (items.items[0..shown], 0..) |item, idx| {
        try out.print("{d}. {s} ({d})\n", .{ idx + 1, item.key, item.count });
    }
    try out.print("ranked: {d}\n", .{items.items.len});
    try out.flush();
}

fn cmdRankEdges(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    var limit: usize = 20;

    var i: usize = 0;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--limit")) {
            if (i + 1 >= args.len) return fail("--limit requires a value", .{});
            limit = try parseLimit(args[i + 1]);
            i += 2;
        } else {
            return fail("unknown rank edges option: {s}", .{args[i]});
        }
    }

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    var counts = std.StringHashMap(u64).init(gpa);
    defer {
        var keys = counts.keyIterator();
        while (keys.next()) |key| gpa.free(key.*);
        counts.deinit();
    }

    for (set.events) |event| {
        var left_index: usize = 0;
        while (left_index < event.entities.len) : (left_index += 1) {
            var right_index = left_index + 1;
            while (right_index < event.entities.len) : (right_index += 1) {
                const key = try edgeKeyAlloc(gpa, event.entities[left_index], event.entities[right_index]);
                if (counts.getPtr(key)) |count| {
                    count.* += 1;
                    gpa.free(key);
                } else {
                    try counts.put(key, 1);
                }
            }
        }
    }

    var items: std.ArrayList(CountItem) = .empty;
    defer items.deinit(gpa);

    var iter = counts.iterator();
    while (iter.next()) |entry| {
        try items.append(gpa, .{ .key = entry.key_ptr.*, .count = entry.value_ptr.* });
    }
    std.mem.sort(CountItem, items.items, {}, countItemGreater);

    var buf: [8192]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    const out = &writer.interface;

    const shown = @min(limit, items.items.len);
    for (items.items[0..shown], 0..) |item, idx| {
        try out.print("{d}. ", .{idx + 1});
        try writeEdgeLabel(out, item.key);
        try out.print(" ({d})\n", .{item.count});
    }
    try out.print("ranked: {d}\n", .{items.items.len});
    try out.flush();
}

fn cmdExport(gpa: Allocator, db_path: []const u8, args: []const []const u8) !void {
    if (args.len < 1 or !std.mem.eql(u8, args[0], "graph")) {
        return fail("export currently supports: export graph [--format dot] [--out file.dot]", .{});
    }

    var format: []const u8 = "dot";
    var out_path: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--format")) {
            if (i + 1 >= args.len) return fail("--format requires a value", .{});
            format = args[i + 1];
            i += 2;
        } else if (std.mem.eql(u8, args[i], "--out")) {
            if (i + 1 >= args.len) return fail("--out requires a path", .{});
            out_path = args[i + 1];
            i += 2;
        } else {
            return fail("unknown export option: {s}", .{args[i]});
        }
    }
    if (!std.mem.eql(u8, format, "dot")) return fail("unsupported export format: {s}", .{format});

    var set = try loadEvents(gpa, db_path);
    defer set.deinit();

    if (out_path) |path| {
        var file = try std.fs.cwd().createFile(path, .{});
        defer file.close();
        var buf: [8192]u8 = undefined;
        var writer = file.writer(&buf);
        try writeDot(gpa, &writer.interface, set.events);
        try writer.interface.flush();
    } else {
        var buf: [8192]u8 = undefined;
        var writer = std.fs.File.stdout().writer(&buf);
        try writeDot(gpa, &writer.interface, set.events);
        try writer.interface.flush();
    }
}

fn ensureStore(db_path: []const u8) !void {
    if (std.fs.path.dirname(db_path)) |dir| {
        if (dir.len > 0) try std.fs.cwd().makePath(dir);
    }
    var file = try std.fs.cwd().createFile(db_path, .{ .read = true, .truncate = false });
    file.close();
}

fn countExistingEvents(db_path: []const u8) !u64 {
    var file = std.fs.cwd().openFile(db_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer file.close();

    var buf: [64 * 1024]u8 = undefined;
    var count: u64 = 0;
    var non_empty_line = false;

    while (true) {
        const n = try file.read(&buf);
        if (n == 0) break;
        for (buf[0..n]) |byte| {
            if (byte == '\n') {
                if (non_empty_line) count += 1;
                non_empty_line = false;
            } else if (byte != ' ' and byte != '\t' and byte != '\r') {
                non_empty_line = true;
            }
        }
    }
    if (non_empty_line) count += 1;
    return count;
}

fn loadEvents(parent_allocator: Allocator, db_path: []const u8) !EventSet {
    var arena = std.heap.ArenaAllocator.init(parent_allocator);
    const allocator = arena.allocator();

    const data = std.fs.cwd().readFileAlloc(allocator, db_path, max_store_bytes) catch |err| switch (err) {
        error.FileNotFound => return .{ .arena = arena, .events = &.{} },
        else => return err,
    };

    var events: std.ArrayList(Event) = .empty;
    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        if (try parseEventLine(allocator, line)) |event| {
            try events.append(allocator, event);
        }
    }

    return .{
        .arena = arena,
        .events = try events.toOwnedSlice(allocator),
    };
}

fn parseEventLine(allocator: Allocator, line: []const u8) !?Event {
    var fields = std.mem.splitScalar(u8, line, '\t');
    const id_text = fields.next() orelse return null;
    const timestamp_text = fields.next() orelse return null;
    const source_text = fields.next() orelse return null;
    const event_text = fields.next() orelse return null;
    const entities_text = fields.next() orelse "";

    const id = std.fmt.parseInt(u64, id_text, 10) catch return null;
    const timestamp = try unescapeField(allocator, timestamp_text);
    const source = try unescapeField(allocator, source_text);
    const text = try unescapeField(allocator, event_text);
    const entities_raw = try unescapeField(allocator, entities_text);

    var entities: std.ArrayList(Entity) = .empty;
    var parts = std.mem.splitScalar(u8, entities_raw, '|');
    while (parts.next()) |part| {
        if (part.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, part, '=') orelse continue;
        const kind = try allocator.dupe(u8, part[0..eq]);
        const value = try allocator.dupe(u8, part[eq + 1 ..]);
        try entities.append(allocator, .{ .kind = kind, .value = value });
    }

    return .{
        .id = id,
        .timestamp = timestamp,
        .source = source,
        .text = text,
        .entities = try entities.toOwnedSlice(allocator),
    };
}

fn appendEvent(
    writer: *std.Io.Writer,
    id: u64,
    timestamp: []const u8,
    source: []const u8,
    text: []const u8,
    entities: []const Entity,
) !void {
    try writer.print("{d}\t", .{id});
    try writeEscapedField(writer, timestamp);
    try writer.writeByte('\t');
    try writeEscapedField(writer, source);
    try writer.writeByte('\t');
    try writeEscapedField(writer, text);
    try writer.writeByte('\t');
    for (entities, 0..) |entity, idx| {
        if (idx > 0) try writer.writeByte('|');
        try writer.print("{s}={s}", .{ entity.kind, entity.value });
    }
    try writer.writeByte('\n');
}

fn writeEscapedField(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |byte| {
        switch (byte) {
            '\t' => try writer.writeAll("\\t"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\\' => try writer.writeAll("\\\\"),
            else => try writer.writeByte(byte),
        }
    }
}

fn unescapeField(allocator: Allocator, value: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < value.len) : (i += 1) {
        if (value[i] == '\\' and i + 1 < value.len) {
            i += 1;
            switch (value[i]) {
                't' => try out.append(allocator, '\t'),
                'n' => try out.append(allocator, '\n'),
                'r' => try out.append(allocator, '\r'),
                '\\' => try out.append(allocator, '\\'),
                else => try out.append(allocator, value[i]),
            }
        } else {
            try out.append(allocator, value[i]);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn extractTimestamp(allocator: Allocator, line: []const u8) ![]u8 {
    var i: usize = 0;
    while (i + 10 <= line.len) : (i += 1) {
        if (std.ascii.isDigit(line[i]) and
            std.ascii.isDigit(line[i + 1]) and
            std.ascii.isDigit(line[i + 2]) and
            std.ascii.isDigit(line[i + 3]) and
            line[i + 4] == '-' and
            std.ascii.isDigit(line[i + 5]) and
            std.ascii.isDigit(line[i + 6]) and
            line[i + 7] == '-' and
            std.ascii.isDigit(line[i + 8]) and
            std.ascii.isDigit(line[i + 9]))
        {
            var end = i + 10;
            while (end < line.len and !isTimestampTerminator(line[end])) end += 1;
            const ts = std.mem.trim(u8, line[i..end], "[]");
            return allocator.dupe(u8, ts);
        }
    }
    return std.fmt.allocPrint(allocator, "unix:{d}", .{std.time.timestamp()});
}

fn isTimestampTerminator(byte: u8) bool {
    return byte == ' ' or byte == '\t' or byte == ',' or byte == ']';
}

fn extractEntities(allocator: Allocator, line: []const u8) ![]Entity {
    var entities: std.ArrayList(Entity) = .empty;
    var seen = std.StringHashMap(void).init(allocator);
    defer {
        var keys = seen.keyIterator();
        while (keys.next()) |key| allocator.free(key.*);
        seen.deinit();
    }

    var tokens = std.mem.tokenizeAny(u8, line, " \t\r\n,\"'()[]{}<>");
    while (tokens.next()) |raw| {
        const token = trimToken(raw);
        if (token.len == 0) continue;

        if (std.mem.indexOfScalar(u8, token, '=')) |eq| {
            const key = token[0..eq];
            const value = trimToken(token[eq + 1 ..]);
            try classifyPrefixedToken(allocator, &entities, &seen, key, value);
            try classifyToken(allocator, &entities, &seen, value);
            continue;
        }

        try classifyToken(allocator, &entities, &seen, token);
    }

    return entities.toOwnedSlice(allocator);
}

fn classifyPrefixedToken(
    allocator: Allocator,
    entities: *std.ArrayList(Entity),
    seen: *std.StringHashMap(void),
    key: []const u8,
    value: []const u8,
) !void {
    if (value.len == 0) return;

    if (isOneOfIgnoreCase(key, &.{ "user", "username", "uid", "account", "principal" })) {
        try addEntity(allocator, entities, seen, "user", value, true);
    } else if (isOneOfIgnoreCase(key, &.{ "host", "hostname", "domain" })) {
        if (validDomain(stripPort(value))) try addEntity(allocator, entities, seen, "domain", stripPort(value), true);
    } else if (isOneOfIgnoreCase(key, &.{ "src", "dst", "src_ip", "dst_ip", "ip" })) {
        const clean = stripPort(value);
        if (validIpv4(clean)) {
            try addEntity(allocator, entities, seen, "ip", clean, false);
        } else if (validDomain(clean)) {
            try addEntity(allocator, entities, seen, "domain", clean, true);
        }
    } else if (isOneOfIgnoreCase(key, &.{ "url", "uri" })) {
        if (isUrl(value)) try addUrlEntities(allocator, entities, seen, value);
    } else if (isOneOfIgnoreCase(key, &.{ "hash", "sha256", "sha1", "md5" })) {
        if (validHash(value)) try addEntity(allocator, entities, seen, "hash", value, true);
    }
}

fn classifyToken(
    allocator: Allocator,
    entities: *std.ArrayList(Entity),
    seen: *std.StringHashMap(void),
    token: []const u8,
) !void {
    if (token.len == 0) return;
    if (isUrl(token)) {
        try addUrlEntities(allocator, entities, seen, token);
        return;
    }
    if (validEmail(token)) {
        try addEntity(allocator, entities, seen, "email", token, true);
        if (std.mem.indexOfScalar(u8, token, '@')) |at| {
            const domain = token[at + 1 ..];
            if (validDomain(domain)) try addEntity(allocator, entities, seen, "domain", domain, true);
        }
        return;
    }
    const hostish = stripPort(token);
    if (validIpv4(hostish)) {
        try addEntity(allocator, entities, seen, "ip", hostish, false);
        return;
    }
    if (validHash(token)) {
        try addEntity(allocator, entities, seen, "hash", token, true);
        return;
    }
    if (validDomain(hostish)) {
        try addEntity(allocator, entities, seen, "domain", hostish, true);
    }
}

fn addUrlEntities(
    allocator: Allocator,
    entities: *std.ArrayList(Entity),
    seen: *std.StringHashMap(void),
    url: []const u8,
) !void {
    try addEntity(allocator, entities, seen, "url", url, true);
    if (hostFromUrl(url)) |host| {
        const clean = stripPort(host);
        if (validIpv4(clean)) {
            try addEntity(allocator, entities, seen, "ip", clean, false);
        } else if (validDomain(clean)) {
            try addEntity(allocator, entities, seen, "domain", clean, true);
        }
    }
}

fn addEntity(
    allocator: Allocator,
    entities: *std.ArrayList(Entity),
    seen: *std.StringHashMap(void),
    kind: []const u8,
    value: []const u8,
    lower: bool,
) !void {
    const normalized_value = if (lower)
        try std.ascii.allocLowerString(allocator, value)
    else
        try allocator.dupe(u8, value);
    errdefer allocator.free(normalized_value);

    const normalized_kind = try allocator.dupe(u8, kind);
    errdefer allocator.free(normalized_kind);

    const key = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ normalized_kind, normalized_value });
    if (seen.contains(key)) {
        allocator.free(key);
        allocator.free(normalized_kind);
        allocator.free(normalized_value);
        return;
    }
    try seen.put(key, {});
    try entities.append(allocator, .{ .kind = normalized_kind, .value = normalized_value });
}

fn freeEntities(allocator: Allocator, entities: []Entity) void {
    for (entities) |entity| {
        allocator.free(entity.kind);
        allocator.free(entity.value);
    }
    allocator.free(entities);
}

fn trimToken(token: []const u8) []const u8 {
    return std.mem.trim(u8, token, " \t\r\n'\"`.,;()[]{}<>");
}

fn isOneOfIgnoreCase(value: []const u8, options: []const []const u8) bool {
    for (options) |option| {
        if (std.ascii.eqlIgnoreCase(value, option)) return true;
    }
    return false;
}

fn isUrl(value: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(value, "http://") or std.ascii.startsWithIgnoreCase(value, "https://");
}

fn hostFromUrl(url: []const u8) ?[]const u8 {
    const scheme = std.mem.indexOf(u8, url, "://") orelse return null;
    var start = scheme + 3;
    if (start >= url.len) return null;
    if (std.mem.indexOfScalarPos(u8, url, start, '@')) |at| {
        const slash = std.mem.indexOfScalarPos(u8, url, start, '/') orelse url.len;
        if (at < slash) start = at + 1;
    }
    var end = start;
    while (end < url.len) : (end += 1) {
        switch (url[end]) {
            '/', '?', '#', '&' => break,
            else => {},
        }
    }
    if (end <= start) return null;
    return url[start..end];
}

fn stripPort(value: []const u8) []const u8 {
    const colon = std.mem.lastIndexOfScalar(u8, value, ':') orelse return value;
    if (colon == 0 or colon + 1 >= value.len) return value;
    for (value[colon + 1 ..]) |byte| {
        if (!std.ascii.isDigit(byte)) return value;
    }
    return value[0..colon];
}

fn validEmail(value: []const u8) bool {
    const at = std.mem.indexOfScalar(u8, value, '@') orelse return false;
    if (at == 0 or at + 1 >= value.len) return false;
    if (std.mem.indexOfScalarPos(u8, value, at + 1, '@') != null) return false;
    return validDomain(value[at + 1 ..]);
}

fn validHash(value: []const u8) bool {
    if (!(value.len == 32 or value.len == 40 or value.len == 64)) return false;
    for (value) |byte| {
        if (!std.ascii.isHex(byte)) return false;
    }
    return true;
}

fn validIpv4(value: []const u8) bool {
    var parts = std.mem.splitScalar(u8, value, '.');
    var count: usize = 0;
    while (parts.next()) |part| {
        if (part.len == 0 or part.len > 3) return false;
        for (part) |byte| {
            if (!std.ascii.isDigit(byte)) return false;
        }
        const octet = std.fmt.parseInt(u8, part, 10) catch return false;
        _ = octet;
        count += 1;
    }
    return count == 4;
}

fn validDomain(value: []const u8) bool {
    if (value.len < 4 or value.len > 253) return false;
    if (std.mem.indexOfScalar(u8, value, '.') == null) return false;
    if (std.mem.startsWith(u8, value, ".") or std.mem.endsWith(u8, value, ".")) return false;

    var labels = std.mem.splitScalar(u8, value, '.');
    var last: []const u8 = "";
    while (labels.next()) |label| {
        if (label.len == 0 or label.len > 63) return false;
        if (label[0] == '-' or label[label.len - 1] == '-') return false;
        for (label) |byte| {
            if (!(std.ascii.isAlphanumeric(byte) or byte == '-')) return false;
        }
        last = label;
    }
    if (last.len < 2) return false;
    for (last) |byte| {
        if (!std.ascii.isAlphabetic(byte)) return false;
    }
    return true;
}

fn eventLessThan(_: void, lhs: Event, rhs: Event) bool {
    if (std.mem.eql(u8, lhs.timestamp, rhs.timestamp)) return lhs.id < rhs.id;
    return std.mem.lessThan(u8, lhs.timestamp, rhs.timestamp);
}

fn eventContains(event: Event, query: []const u8) bool {
    if (std.ascii.indexOfIgnoreCase(event.text, query) != null) return true;
    for (event.entities) |entity| {
        if (entityMatches(entity, query)) return true;
    }
    return false;
}

fn eventHasEntity(event: Event, query: []const u8) bool {
    for (event.entities) |entity| {
        if (entityMatches(entity, query)) return true;
    }
    return false;
}

fn entityMatches(entity: Entity, query: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(entity.value, query)) return true;
    if (std.ascii.eqlIgnoreCase(entity.kind, query)) return true;
    if (std.mem.indexOfScalar(u8, query, ':')) |colon| {
        return std.ascii.eqlIgnoreCase(entity.kind, query[0..colon]) and
            std.ascii.eqlIgnoreCase(entity.value, query[colon + 1 ..]);
    }
    return false;
}

fn entityKeyAlloc(allocator: Allocator, entity: Entity) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}:{s}", .{ entity.kind, entity.value });
}

fn edgeKeyAlloc(allocator: Allocator, left: Entity, right: Entity) ![]u8 {
    const left_key = try entityKeyAlloc(allocator, left);
    defer allocator.free(left_key);
    const right_key = try entityKeyAlloc(allocator, right);
    defer allocator.free(right_key);
    return canonicalEdgeKeyAlloc(allocator, left_key, right_key);
}

fn canonicalEdgeKeyAlloc(allocator: Allocator, left: []const u8, right: []const u8) ![]u8 {
    if (std.mem.lessThan(u8, left, right)) {
        return std.fmt.allocPrint(allocator, "{s}\t{s}", .{ left, right });
    }
    return std.fmt.allocPrint(allocator, "{s}\t{s}", .{ right, left });
}

fn writeEdgeLabel(writer: *std.Io.Writer, edge_key: []const u8) !void {
    const tab = std.mem.indexOfScalar(u8, edge_key, '\t') orelse {
        try writer.writeAll(edge_key);
        return;
    };
    try writer.print("{s} <-> {s}", .{ edge_key[0..tab], edge_key[tab + 1 ..] });
}

fn countItemGreater(_: void, lhs: CountItem, rhs: CountItem) bool {
    if (lhs.count == rhs.count) return std.mem.lessThan(u8, lhs.key, rhs.key);
    return lhs.count > rhs.count;
}

fn parseLimit(value: []const u8) !usize {
    const parsed = std.fmt.parseInt(usize, value, 10) catch {
        try fail("invalid --limit: {s}", .{value});
        unreachable;
    };
    if (parsed == 0) {
        try fail("--limit must be greater than zero", .{});
        unreachable;
    }
    return parsed;
}

fn printEventSummary(writer: *std.Io.Writer, event: Event) !void {
    try writer.print("#{d} {s} [{s}] {s}\n", .{ event.id, event.timestamp, event.source, event.text });
    if (event.entities.len > 0) {
        try writer.writeAll("  ");
        for (event.entities, 0..) |entity, idx| {
            if (idx > 0) try writer.writeAll(", ");
            try writer.print("{s}:{s}", .{ entity.kind, entity.value });
        }
        try writer.writeByte('\n');
    }
}

fn addAdjacency(
    allocator: Allocator,
    graph: *std.StringHashMap(std.ArrayList([]const u8)),
    a: []const u8,
    b: []const u8,
) !void {
    var result = try graph.getOrPutValue(a, .empty);
    try result.value_ptr.append(allocator, b);
}

fn shortestPath(
    allocator: Allocator,
    graph: *std.StringHashMap(std.ArrayList([]const u8)),
    from: []const u8,
    to: []const u8,
) ![][]const u8 {
    var queue: std.ArrayList([]const u8) = .empty;
    defer queue.deinit(allocator);

    var parents = std.StringHashMap([]const u8).init(allocator);
    defer parents.deinit();

    try queue.append(allocator, from);
    try parents.put(from, "");

    var head: usize = 0;
    while (head < queue.items.len) : (head += 1) {
        const node = queue.items[head];
        if (std.mem.eql(u8, node, to)) break;
        const neighbors = graph.get(node) orelse continue;
        for (neighbors.items) |neighbor| {
            if (parents.contains(neighbor)) continue;
            try parents.put(neighbor, node);
            try queue.append(allocator, neighbor);
            if (std.mem.eql(u8, neighbor, to)) break;
        }
    }

    if (!parents.contains(to)) return allocator.alloc([]const u8, 0);

    var reversed: std.ArrayList([]const u8) = .empty;
    defer reversed.deinit(allocator);

    var current = to;
    while (true) {
        try reversed.append(allocator, current);
        const parent = parents.get(current).?;
        if (parent.len == 0) break;
        current = parent;
    }

    const path = try allocator.alloc([]const u8, reversed.items.len);
    for (reversed.items, 0..) |node, idx| {
        path[reversed.items.len - 1 - idx] = node;
    }
    return path;
}

fn writeDot(allocator: Allocator, writer: *std.Io.Writer, events: []const Event) !void {
    var node_ids = std.StringHashMap(u64).init(allocator);
    defer {
        var keys = node_ids.keyIterator();
        while (keys.next()) |key| allocator.free(key.*);
        node_ids.deinit();
    }

    var edges = std.StringHashMap(u64).init(allocator);
    defer {
        var keys = edges.keyIterator();
        while (keys.next()) |key| allocator.free(key.*);
        edges.deinit();
    }

    var next_id: u64 = 0;
    for (events) |event| {
        for (event.entities) |entity| {
            const key = try entityKeyAlloc(allocator, entity);
            if (node_ids.contains(key)) {
                allocator.free(key);
            } else {
                try node_ids.put(key, next_id);
                next_id += 1;
            }
        }

        var i: usize = 0;
        while (i < event.entities.len) : (i += 1) {
            var j = i + 1;
            while (j < event.entities.len) : (j += 1) {
                const edge_key = try edgeKeyAlloc(allocator, event.entities[i], event.entities[j]);
                if (edges.getPtr(edge_key)) |count| {
                    count.* += 1;
                    allocator.free(edge_key);
                } else {
                    try edges.put(edge_key, 1);
                }
            }
        }
    }

    try writer.writeAll("graph intel {\n  overlap=false;\n  splines=true;\n");
    var nodes = node_ids.iterator();
    while (nodes.next()) |entry| {
        try writer.print("  n{d} [label=\"", .{entry.value_ptr.*});
        try writeDotString(writer, entry.key_ptr.*);
        try writer.writeAll("\"];\n");
    }

    var edge_iter = edges.iterator();
    while (edge_iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const tab = std.mem.indexOfScalar(u8, key, '\t') orelse continue;
        const left = key[0..tab];
        const right = key[tab + 1 ..];
        const left_id = node_ids.get(left) orelse continue;
        const right_id = node_ids.get(right) orelse continue;
        try writer.print("  n{d} -- n{d} [label=\"{d}\"];\n", .{ left_id, right_id, entry.value_ptr.* });
    }
    try writer.writeAll("}\n");
}

fn writeDotString(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |byte| {
        switch (byte) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            else => try writer.writeByte(byte),
        }
    }
}

fn fail(comptime fmt: []const u8, args: anytype) !void {
    var buf: [1024]u8 = undefined;
    var writer = std.fs.File.stderr().writer(&buf);
    const err = &writer.interface;
    try err.writeAll("error: ");
    try err.print(fmt, args);
    try err.writeByte('\n');
    try err.flush();
    return error.CommandFailed;
}

test "extracts common entities" {
    const allocator = std.testing.allocator;
    const entities = try extractEntities(
        allocator,
        "2026-04-30T10:20:30Z user=alice src=10.0.0.12 url=https://Suspicious.Example/a sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa alice@example.com",
    );
    defer freeEntities(allocator, entities);

    try std.testing.expect(entities.len >= 6);
    try std.testing.expect(containsEntity(entities, "user", "alice"));
    try std.testing.expect(containsEntity(entities, "ip", "10.0.0.12"));
    try std.testing.expect(containsEntity(entities, "domain", "suspicious.example"));
    try std.testing.expect(containsEntity(entities, "email", "alice@example.com"));
}

test "validates ipv4 and domains" {
    try std.testing.expect(validIpv4("192.168.0.1"));
    try std.testing.expect(!validIpv4("999.168.0.1"));
    try std.testing.expect(validDomain("example.com"));
    try std.testing.expect(!validDomain("not-a-domain"));
}

fn containsEntity(entities: []const Entity, kind: []const u8, value: []const u8) bool {
    for (entities) |entity| {
        if (std.mem.eql(u8, entity.kind, kind) and std.mem.eql(u8, entity.value, value)) return true;
    }
    return false;
}
