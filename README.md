# Agentic Image Sourcing

Agentic Image Sourcing is a Python service that helps AI agents discover, inspect, fetch, and persist relevant images from Google Images and direct public websites.

It is designed around a browser-automation workflow for Google Images using PinchTab, with a clean agent-facing surface over HTTP and MCP.

## What It Provides

- HTTP API for image search, page extraction, fetching, inspection, and asset persistence
- MCP server wrapper over the same service methods
- PinchTab-driven Google Images browser workflow with contact-sheet output
- Google Custom Search image discovery adapter for keyed/API use cases
- Direct page image extraction from metadata, JSON-LD, `img`, lazy-load, and `srcset`
- Public-web crawling with per-domain rate limiting and robots-aware access checks
- Local cache for fetched images plus pluggable object-store persistence
- Provenance, dedupe, fetch metadata, content hashing, and perceptual hashing

## Quick Start

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[test,mcp]
```

2. Copy `.env.example` to `.env`.
3. Start a local PinchTab server and allow public internet browsing for the browser instance you want agents to use.
4. Optionally set `AIS_GOOGLE_API_KEY` and `AIS_GOOGLE_CSE_ID` if you also want API-backed discovery.
5. Run the HTTP API:

```bash
ais api --host 127.0.0.1 --port 8000
```

6. Or run the MCP server:

```bash
ais mcp
```

## PinchTab Requirements

- PinchTab must be running locally and reachable at `AIS_PINCHTAB_BASE_URL`.
- The Google browser workflow requires `security.allowEvaluate = true` in PinchTab.
- Headed mode is the easiest way to debug Google Images behavior live.

## HTTP Endpoints

- `POST /search`
- `POST /extract`
- `POST /google/gallery`
- `POST /google/inspect`
- `POST /google/download`
- `POST /fetch`
- `GET /candidates/{candidate_id}`
- `POST /assets`

## Google Images Workflow

Use the Google browser workflow when an agent needs to visually assess a batch of results:

1. Call `POST /google/gallery` or `google_image_gallery`.
2. The service drives PinchTab to Google Images, scores a larger pool of candidates, assembles a 12-at-a-time contact sheet, and returns a manifest of numbered candidates.
3. The agent evaluates the contact sheet visually and selects one or more `candidate_id` values.
4. Call `POST /google/inspect` or `google_image_inspect` to resolve the larger preview/source page metadata.
5. Call `POST /google/download` or `google_image_download` to cache and persist selected results.

For follow-up batches, use `batch_number=2` or `batch_number=3` on the gallery request. The service will return the next scored page of results.

The current implementation isolates Google DOM scraping inside the PinchTab-backed adapter so selector updates stay contained when Google changes its layout.

## PAI Integration

If you are wiring this into a personal assistant or another agent runtime, start here:

- [docs/PAI.md](docs/PAI.md)

The shortest path is:

1. `POST /google/gallery`
2. choose a `candidate_id`
3. `POST /google/inspect`
4. `POST /google/download`

## Example Search Request

```json
{
  "query": "industrial warehouse reference photography",
  "limit": 10,
  "preferred_domains": ["commons.wikimedia.org"]
}
```

## Storage Notes

- The default development configuration uses SQLite and a local filesystem object store.
- The object-store abstraction supports S3-compatible backends.
- The repository layer is designed so a Postgres runtime adapter can be swapped in without changing the service contract.

## Testing

```bash
pytest
```

## Notes

- `var/` is runtime-only and is intentionally ignored by git.
- `.env` is intentionally ignored by git.
- The Google Images workflow is browser-driven and may need selector updates when Google changes its markup.
