# Using This With PAI

This project is easiest to integrate into PAI in one of two ways.

The recommended order is:

1. start PinchTab
2. start this project in either HTTP or MCP mode
3. expose only the Google gallery, inspect, and download tools to PAI first
4. let PAI use batch 2 or batch 3 when it wants more variety instead of re-running batch 1

## Option 1: HTTP API

Use this when PAI can call local HTTP endpoints.

Start the service:

```bash
ais api --host 127.0.0.1 --port 8000
```

Or on Windows, use the included launcher:

```powershell
.\scripts\start-pai-http.ps1
```

Suggested tool contract inside PAI:

1. `google_image_gallery`
   - input: `query`, optional `batch_size`, optional `batch_number`, optional `offset`
   - output: `gallery_id`, `gallery_image_path`, `candidates`
2. `google_image_inspect`
   - input: `candidate_id`
   - output: resolved `image_url`, `source_page_url`, dimensions, provenance
3. `google_image_download`
   - input: `candidate_id`, `collection`, optional `tags`
   - output: cached file path plus persisted asset metadata

Recommended PAI loop:

1. ask for a gallery
2. inspect the contact sheet path and candidate manifest
3. pick one or more `candidate_id` values
4. inspect each candidate
5. download the keepers

For more variety, ask for `batch_number=2` or `batch_number=3` rather than repeating the same first page of results.

Example HTTP calls:

```bash
curl -X POST http://127.0.0.1:8000/google/gallery ^
  -H "Content-Type: application/json" ^
  -d "{\"query\":\"red fox wildlife photography\",\"batch_size\":12}"
```

```bash
curl -X POST http://127.0.0.1:8000/google/inspect ^
  -H "Content-Type: application/json" ^
  -d "{\"candidate_id\":\"<candidate-id>\"}"
```

```bash
curl -X POST http://127.0.0.1:8000/google/download ^
  -H "Content-Type: application/json" ^
  -d "{\"candidate_id\":\"<candidate-id>\",\"collection\":\"pai-picks\"}"
```

If PAI accepts custom HTTP tool definitions, start from:

- [pai-http-tools.example.json](/F:/AI/AgenticImageSourcing/docs/pai-http-tools.example.json)

## Option 2: MCP

Use this when PAI can launch an MCP server and consume MCP tools directly.

Start the MCP server:

```bash
ais mcp
```

Or on Windows, use the included launcher:

```powershell
.\scripts\start-pai-mcp.ps1
```

Expose these tools inside PAI:

- `google_image_gallery`
- `google_image_inspect`
- `google_image_download`
- `candidate_inspect`
- `image_fetch`
- `asset_save`

Recommended minimal MCP set for PAI:

- `google_image_gallery`
- `google_image_inspect`
- `google_image_download`

That smaller tool set is usually enough, and it keeps the agent from wandering into lower-level fetch helpers before it has picked a candidate.

## Practical Advice

- Keep PinchTab running locally before PAI starts image work.
- Use `headed` mode by default for reliability with Google Images. Headless mode is still available, but Google may block it and force a fallback.
- Use the gallery step first; it is much easier for an agent to reason about a contact sheet than raw URLs.
- Treat selectors as brittle over time. If Google changes markup, the fix should stay isolated to the Google browser adapter.

## Suggested PAI Tool Policy

Give PAI guidance like this:

1. Always call `google_image_gallery` first.
2. Prefer `batch_number=1` first, then `batch_number=2` or `batch_number=3` if the first batch is weak or repetitive.
3. Use `google_image_inspect` before `google_image_download`.
4. Save chosen images into a collection that matches the task, for example `week-01-b05` or `character-references`.
5. Treat `gallery_image_path` as the main visual artifact and the candidate list as supporting metadata.
