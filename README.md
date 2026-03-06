# n8n-nodes-figranium

Official n8n community node that lets you run Figranium tasks directly through your workflow using the local or remote Figranium API.

## Highlights

- Single `Execute Task` operation that posts to `POST /tasks/:id/api` with optional variables.
- Credential-based authentication, so each workflow can point at a different Figranium server or API key.
- Task dropdown is populated dynamically by calling `/api/tasks/list`, keeping the node synchronized with what the server exposes.

## Requirements

- n8n (cloud or self-hosted) with community nodes enabled.
- Figranium server reachable from wherever n8n is running (default `http://localhost:11345`).
- Valid API key created via Figranium Settings.

## Documentation

- For a full walkthrough of the n8n integration, see the official Figranium docs: https://figranium.com/docs/n8n-integration.

## Installation

### Classic (recommended)

1. In n8n, go to **Settings -> Community Nodes**.
2. Enter `n8n-nodes-figranium`.
3. Install the package and restart n8n if prompted.

### Manual (from source)

```bash
npm install
npm run build
```

This compiles the TypeScript sources under `src/` to `dist/`, copies the bundled assets, and makes the package usable as a community node.

## Configuration

### Credentials

The node uses the `Figranium API` credential type, which captures:

- **Base URL** - defaults to `http://localhost:11345`. Trim trailing slashes.
- **API Key** - stored securely and sent in the `x-api-key` header on every request.

When you connect the credential, the node can reach the `/api/tasks/list` endpoint to populate the **Task** dropdown.

### Node fields

- **Operation** - currently only `Execute Task`.
- **Task** - choose a task ID from the loaded list; the list is sorted by name and falls back to the raw ID when no name exists.
- **Variables** - optional fixed collection of key/value pairs that are sent in the request body under `variables`. Leave blank to trigger the task with its default inputs.

## Usage

1. Add the `Figranium` node to your workflow and attach it to the steps that should trigger a task.
2. Select your `Figranium API` credential.
3. Pick the task to run from the dropdown; it calls `/api/tasks/list` automatically via the credential.
4. Optionally define `Variables` to override or inject runtime data, either by hardcoding strings or by using expressions that reference previous nodes.

Each execution performs:

```
POST {baseUrl}/tasks/:taskId/api
Headers:
  x-api-key: {api key}
Body:
  {
    "variables": {
      "<name>": "<value>",
      ...
    }
  }
```

The node returns the JSON response from Figranium as the output data for downstream nodes.

## Troubleshooting

- **Tasks list is empty** - confirm the credential's Base URL/API key, ensure n8n can reach the Figranium server, and that `/api/tasks/list` returns a payload.
- **Task run fails with HTTP error** - review the Figranium logs for task-specific errors and confirm that the task ID exists.
- **Variables are ignored** - make sure each entry in the `Variables` collection has a non-empty `Name`; values can be empty strings but names are required.

## Development

- `npm run build` compiles TypeScript sources and copies icons into `dist/`.
- After building, publish the package to npm (if desired) or load the folder as a local community node archive.

## Contributing

1. Fork this repo, implement fixes or nodes in `src/`, and push a topic branch.
2. Run `npm run build` to regenerate `dist/` before submitting a pull request.
3. Describe how to reproduce or test your changes in the PR description.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
