# Using This With PAI

This project is easiest to integrate into PAI in one of two ways:

## Option 1: HTTP API

Use this when PAI can call local HTTP endpoints.

Start the service:

```bash
ais api --host 127.0.0.1 --port 8000
```

Suggested tool contract inside PAI:

1. `google_image_gallery`
   - input: `query`, optional `batch_size`, optional `offset`
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

## Option 2: MCP

Use this when PAI can launch an MCP server and consume MCP tools directly.

Start the MCP server:

```bash
ais mcp
```

Expose these tools inside PAI:

- `google_image_gallery`
- `google_image_inspect`
- `google_image_download`
- `candidate_inspect`
- `image_fetch`
- `asset_save`

## Practical Advice

- Keep PinchTab running locally before PAI starts image work.
- Use `headed` mode when you want to watch Google Images behavior live.
- Use the gallery step first; it is much easier for an agent to reason about a contact sheet than raw URLs.
- Treat selectors as brittle over time. If Google changes markup, the fix should stay isolated to the Google browser adapter.
