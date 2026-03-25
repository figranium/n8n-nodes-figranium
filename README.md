# n8n-nodes-figranium

Official n8n community node for [Figranium](https://figranium.dev) — trigger tasks, inspect executions, and manage schedules directly from your n8n workflows.

## Resources and operations

### Task

| Operation | Description |
|---|---|
| **Execute** | Run a saved task and return its result. Accepts optional runtime variables. |
| **List** | Return all task IDs, names, and descriptions from the server. |

### Execution

| Operation | Description |
|---|---|
| **List** | Return a summary of all past execution records. |

### Schedule

| Operation | Description |
|---|---|
| **List** | Return all tasks that have a schedule configured. |
| **Get Status** | Get the schedule config and next run time for a specific task. |
| **Set Schedule** | Create or update a schedule on a task (frequency-based or cron). |
| **Delete Schedule** | Disable and remove the schedule from a task. |
| **Describe Schedule** | Validate and preview a schedule config without saving it. |
| **Get Scheduler Status** | Return the overall status of the task scheduler. |

## Requirements

- n8n (cloud or self-hosted) with community nodes enabled.
- Figranium server reachable from wherever n8n is running (default `http://localhost:11345`).
- Valid API key created via Figranium Settings.

## Documentation

Full walkthrough of the n8n integration: https://figranium.dev/docs/n8n-integration

## Installation

### Classic (recommended)

1. In n8n, go to **Settings → Community Nodes**.
2. Enter `n8n-nodes-figranium`.
3. Install and restart n8n if prompted.

### Manual (from source)

```bash
npm install
npm run build
```

## Configuration

### Credentials

The node uses the `Figranium API` credential type:

- **Base URL** — your Figranium server address, e.g. `http://localhost:11345`. Trim trailing slashes.
- **API Key** — stored securely and sent as `x-api-key` on every request.

### Task › Execute

- **Task** — choose from the dropdown, which is populated via `/api/tasks/list`. Each option shows the task name and description (if set).
- **Variables** — optional key/value pairs injected at runtime under `variables` in the request body. Names are required; values can be empty strings.

### Schedule › Set Schedule

- **Schedule Mode** — `Frequency` (interval/daily/weekly/monthly) or `Cron Expression`.
- Frequency fields (hour, minute, days of week, day of month) appear based on the selected frequency.
- Cron accepts a standard 5-field expression, e.g. `0 9 * * 1`.

## Usage example — Execute a task with variables

```
POST {baseUrl}/api/tasks/{taskId}/api
x-api-key: {apiKey}

{
  "variables": {
    "url": "https://example.com",
    "limit": "10"
  }
}
```

The node returns the JSON response from Figranium as output data for downstream nodes.

## Troubleshooting

- **Task dropdown is empty** — confirm the credential Base URL and API key, ensure n8n can reach the Figranium server, and check that `/api/tasks/list` returns data.
- **Execute fails with HTTP error** — check Figranium logs for task-specific errors and confirm the task ID still exists.
- **Variables are ignored** — each entry in the Variables collection must have a non-empty Name.
- **Schedule operations fail** — confirm the Task ID is correct and that the Figranium scheduler is running (`Get Scheduler Status`).

## Development

```bash
npm run build   # compile TypeScript + copy icons to dist/
```

Load the package as a local community node or publish to npm.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
