# IntelGraph Zig

IntelGraph Zig is a local-first intelligence graph CLI written in Zig. It ingests line-oriented logs, extracts common entities, stores events locally, and helps analysts search events, inspect relationships, build timelines, find paths, and export graph or event data.

The tool is designed for defensive analysis, incident response, audit review, and local data exploration. It does not exploit systems, intercept traffic, or send data anywhere. The first version intentionally has no network calls and no external runtime dependencies.

The default local store is `.intelgraph/events.tsv` in the current directory.

## Features

- Ingest line-oriented logs from local files or standard input
- Extract IP addresses, domains, URLs, email addresses, hashes, and user identifiers
- Store events in a simple local TSV database
- Search raw events and extracted entities
- Show entity-centered context and neighboring entities
- Build filtered timelines
- Find shortest paths between related entities
- Rank frequently observed entities and relationships
- Export events as JSONL and graphs as Graphviz DOT or JSON

## Build

```sh
zig build
```

The binary is installed at `zig-out/bin/intel`.

## Quick Start

```sh
zig build run -- init
zig build run -- ingest examples/access.log
zig build run -- stats
zig build run -- search suspicious --limit 5
zig build run -- entity ip:10.0.0.12 --limit 5
zig build run -- rank entities --kind domain --limit 10
zig build run -- rank edges --limit 10
zig build run -- path alice@example.com suspicious.example
zig build run -- timeline --entity suspicious.example --limit 10
zig build run -- export graph --out graph.dot
zig build run -- export graph --format json --out graph.json
zig build run -- export events --out events.jsonl
```

Render the DOT output with Graphviz:

```sh
dot -Tpng graph.dot -o graph.png
```

## Commands

```text
intel [--db PATH] init
intel [--db PATH] ingest <file|-> [--source NAME]
intel [--db PATH] search <text> [--limit N]
intel [--db PATH] entity <value|kind:value> [--limit N]
intel [--db PATH] timeline [--entity <value|kind:value>] [--limit N]
intel [--db PATH] path <from> <to>
intel [--db PATH] rank entities [--kind KIND] [--limit N]
intel [--db PATH] rank edges [--limit N]
intel [--db PATH] export graph [--format dot|json] [--out FILE]
intel [--db PATH] export events [--format jsonl] [--out FILE]
intel [--db PATH] stats
```

Use `-` as the ingest path to read from standard input:

```sh
tail -f app.log | intel ingest - --source app.log
```

## Extracted Entities

- `ip`: IPv4 addresses
- `domain`: DNS-style domains and URL hosts
- `url`: HTTP and HTTPS URLs
- `email`: email addresses
- `hash`: MD5, SHA1, and SHA256-shaped hex strings
- `user`: values from keys such as `user=`, `username=`, `uid=`, `account=`, and `principal=`

## Design Notes

This is an MVP. Ingestion is streaming and caps individual input lines at 1 MiB. The local store is still read into memory for graph-oriented commands, and it is a TSV file for portability and easy inspection. A SQLite/FTS5 backend, Zeek log parser, Sigma/YARA result importers, and richer TUI views are natural next steps.
