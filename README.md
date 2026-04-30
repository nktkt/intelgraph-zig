# IntelGraph Zig

IntelGraph Zig is a local-first intelligence graph CLI written in Zig. It ingests line-oriented logs, extracts common entities, stores events locally, and helps analysts search events, inspect relationships, build timelines, find paths, and export Graphviz maps.

The tool is designed for defensive analysis, incident response, audit review, and local data exploration. It does not exploit systems, intercept traffic, or send data anywhere. The first version intentionally has no network calls and no external runtime dependencies.

The default local store is `.intelgraph/events.tsv` in the current directory.

## Features

- Ingest line-oriented logs from local files
- Extract IP addresses, domains, URLs, email addresses, hashes, and user identifiers
- Store events in a simple local TSV database
- Search raw events and extracted entities
- Show entity-centered context and neighboring entities
- Build filtered timelines
- Find shortest paths between related entities
- Export an undirected Graphviz DOT graph

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
zig build run -- entity ip:10.0.0.12
zig build run -- path alice@example.com suspicious.example
zig build run -- timeline --entity suspicious.example
zig build run -- export graph --out graph.dot
```

Render the DOT output with Graphviz:

```sh
dot -Tpng graph.dot -o graph.png
```

## Commands

```text
intel [--db PATH] init
intel [--db PATH] ingest <file> [--source NAME]
intel [--db PATH] search <text>
intel [--db PATH] entity <value|kind:value>
intel [--db PATH] timeline [--entity <value|kind:value>]
intel [--db PATH] path <from> <to>
intel [--db PATH] export graph [--format dot] [--out file.dot]
intel [--db PATH] stats
```

## Extracted Entities

- `ip`: IPv4 addresses
- `domain`: DNS-style domains and URL hosts
- `url`: HTTP and HTTPS URLs
- `email`: email addresses
- `hash`: MD5, SHA1, and SHA256-shaped hex strings
- `user`: values from keys such as `user=`, `username=`, `uid=`, `account=`, and `principal=`

## Design Notes

This is an MVP. The current ingestion path reads files up to 128 MiB into memory, and the local store is a TSV file for portability and easy inspection. A streaming reader, SQLite/FTS5 backend, Zeek log parser, Sigma/YARA result importers, and richer TUI views are natural next steps.
