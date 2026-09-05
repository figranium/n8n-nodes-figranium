import type {
  IDataObject,
  IExecuteFunctions,
  IHttpRequestMethods,
  ILoadOptionsFunctions,
  INode,
  INodeExecutionData,
  INodePropertyOptions,
  INodeType,
  INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';

const JSON_FIELD_NAMES = ['stealth', 'actions', 'variables'];

/**
 * n8n `json`-type collection fields arrive as strings; the Figranium API
 * expects real objects/arrays for stealth, actions, and variables.
 */
function parseJsonFields(node: INode, fields: IDataObject): IDataObject {
  const result: IDataObject = { ...fields };
  for (const key of JSON_FIELD_NAMES) {
    const raw = result[key];
    if (typeof raw === 'string' && raw.trim() !== '') {
      try {
        result[key] = JSON.parse(raw);
      } catch {
        throw new NodeOperationError(node, `Invalid JSON in "${key}" field.`);
      }
    } else if (raw === '' || raw === undefined) {
      delete result[key];
    }
  }
  return result;
}

export class Figranium implements INodeType {
  methods = {
    loadOptions: {
      async getTasks(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = await this.getCredentials('figraniumApi');
        const baseUrl = String(credentials.baseUrl || '').replace(/\/+$/, '');

        if (!baseUrl) {
          return [];
        }

        try {
          const response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/list`,
              json: true,
            },
          );

          const payload = response as IDataObject;
          const tasks = Array.isArray(payload) ? payload : (payload.tasks as IDataObject[] | undefined);
          if (!Array.isArray(tasks)) {
            return [];
          }

          return tasks
            .map((task) => ({
              name: String(task?.name || task?.id || ''),
              value: String(task?.id || ''),
              description: task?.description ? String(task.description) : undefined,
            }))
            .filter((option) => option.value)
            .sort((a, b) => a.name.localeCompare(b.name));
        } catch {
          throw new NodeOperationError(this.getNode(), 'Error fetching tasks from Figranium.', {
            itemIndex: 0,
          });
        }
      },

    },
  };

  description: INodeTypeDescription = {
    displayName: 'Figranium',
    name: 'figranium',
    icon: { light: 'file:figranium_icon_light.svg', dark: 'file:figranium_icon_dark.svg' },
    group: ['transform'],
    version: 1,
    subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
    description: 'Interact with Figranium — trigger tasks, inspect executions, and manage schedules.',
    defaults: {
      name: 'Figranium',
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [NodeConnectionTypes.Main],
    usableAsTool: true,
    credentials: [
      {
        name: 'figraniumApi',
        required: true,
      },
    ],
    properties: [
      // ─── Resource selector ───────────────────────────────────────────────
      {
        displayName: 'Resource',
        name: 'resource',
        type: 'options',
        noDataExpression: true,
        options: [
          {
            name: 'Execute',
            value: 'execute',
            description: 'Run a saved task and return its result',
          },
          {
            name: 'Task Actions',
            value: 'task',
            description: 'Manage automation tasks (create, list, update, delete)',
          },
          {
            name: 'Browser',
            value: 'browser',
            description: 'Launch a managed browser session',
          },
          {
            name: 'Execution',
            value: 'execution',
            description: 'Inspect past execution records',
          },
          {
            name: 'Inspector',
            value: 'inspector',
            description: 'Highlight and inspect elements on an active browser session',
          },
          {
            name: 'Schedule',
            value: 'schedule',
            description: 'View and manage task schedules',
          },
        ],
        default: 'execute',
      },

      // ─── EXECUTE operations ───────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['execute'],
          },
        },
        options: [
          {
            name: 'Execute Task',
            value: 'execute',
            description: 'Run a saved task and return its result',
            action: 'Execute a task',
          },
        ],
        default: 'execute',
      },

      // ─── TASK ACTIONS operations ─────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['task'],
          },
        },
        options: [
          {
            name: 'Create',
            value: 'create',
            description: 'Create a new automation task',
            action: 'Create a task',
          },
          {
            name: 'Delete',
            value: 'delete',
            description: 'Permanently delete a task',
            action: 'Delete a task',
          },
          {
            name: 'List',
            value: 'list',
            description: 'Return all task IDs, names, and descriptions',
            action: 'List tasks',
          },
          {
            name: 'Update',
            value: 'update',
            description: 'Update fields on an existing task',
            action: 'Update a task',
          },
        ],
        default: 'list',
      },

      // ─── EXECUTION operations ─────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['execution'],
          },
        },
        options: [
          {
            name: 'List',
            value: 'list',
            description: 'Return a summary of all past executions',
            action: 'List executions',
          },
        ],
        default: 'list',
      },

      // ─── SCHEDULE operations ──────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['schedule'],
          },
        },
        options: [
          {
            name: 'Delete Schedule',
            value: 'delete',
            description: 'Disable and remove the schedule from a task',
            action: 'Delete a schedule',
          },
          {
            name: 'Describe Schedule',
            value: 'describe',
            description: 'Validate and preview a schedule config without saving it',
            action: 'Describe a schedule',
          },
          {
            name: 'Get Scheduler Status',
            value: 'getAllStatus',
            description: 'Return the overall status of the task scheduler',
            action: 'Get overall scheduler status',
          },
          {
            name: 'Get Status',
            value: 'getStatus',
            description: 'Get the schedule status and next run time for a specific task',
            action: 'Get schedule status',
          },
          {
            name: 'List',
            value: 'list',
            description: 'Return all tasks that have schedules configured',
            action: 'List schedules',
          },
          {
            name: 'Set Schedule',
            value: 'set',
            description: 'Create or update a schedule on a task',
            action: 'Set a schedule',
          },
        ],
        default: 'list',
      },

      // ─── BROWSER operations ─────────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['browser'],
          },
        },
        options: [
          {
            name: 'Open',
            value: 'open',
            description: 'Launch or reattach a managed browser session',
            action: 'Open a browser session',
          },
        ],
        default: 'open',
      },

      // ─── INSPECTOR operations ───────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['inspector'],
          },
        },
        options: [
          {
            name: 'Highlight',
            value: 'highlight',
            description: 'Highlight and inspect elements on an active browser session',
            action: 'Highlight elements',
          },
        ],
        default: 'highlight',
      },


      // ─── Shared: Task ID (execute / update / delete) ───────────────────────
      {
        displayName: 'Task Name or ID',
        name: 'taskId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getTasks',
        },
        default: '',
        required: true,
        description:
          'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
        displayOptions: {
          show: {
            resource: ['execute', 'task'],
            operation: ['execute', 'update', 'delete'],
          },
        },
      },

      // ─── Task ID string (for schedule ops that need a task ID) ────────────
      {
        displayName: 'Task ID',
        name: 'taskIdString',
        type: 'string',
        default: '',
        required: true,
        description: 'The ID of the task',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['getStatus', 'set', 'delete', 'describe'],
          },
        },
      },

      // ─── Task: Execute — Variables ────────────────────────────────────────
      {
        displayName: 'Variables',
        name: 'variables',
        type: 'fixedCollection',
        default: {},
        description: 'Key-value pairs passed into the task at runtime',
        typeOptions: {
          multipleValues: true,
        },
        displayOptions: {
          show: {
            resource: ['execute', 'task'],
            operation: ['execute'],
          },
        },
        options: [
          {
            name: 'values',
            displayName: 'Variable',
            values: [
              {
                displayName: 'Name',
                name: 'name',
                type: 'string',
                default: '',
                required: true,
              },
              {
                displayName: 'Value',
                name: 'value',
                type: 'string',
                default: '',
              },
            ],
          },
        ],
      },

      // ─── Schedule: Set — schedule config fields ───────────────────────────
      {
        displayName: 'Enabled',
        name: 'scheduleEnabled',
        type: 'boolean',
        default: true,
        description: 'Whether the schedule should be active',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
          },
        },
      },
      {
        displayName: 'Schedule Mode',
        name: 'scheduleMode',
        type: 'options',
        options: [
          { name: 'Frequency (Interval)', value: 'frequency' },
          { name: 'Cron Expression', value: 'cron' },
        ],
        default: 'frequency',
        description: 'How to express the schedule timing',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
          },
        },
      },
      {
        displayName: 'Frequency',
        name: 'frequency',
        type: 'options',
        options: [
          { name: 'Every N Minutes', value: 'interval' },
          { name: 'Daily', value: 'daily' },
          { name: 'Weekly', value: 'weekly' },
          { name: 'Monthly', value: 'monthly' },
        ],
        default: 'daily',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
          },
        },
      },
      {
        displayName: 'Interval (Minutes)',
        name: 'intervalMinutes',
        type: 'number',
        default: 60,
        description: 'How often to run (in minutes)',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
            frequency: ['interval'],
          },
        },
      },
      {
        displayName: 'Hour',
        name: 'scheduleHour',
        type: 'number',
        default: 9,
        description: 'Hour of day to run (0–23)',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
            frequency: ['daily', 'weekly', 'monthly'],
          },
        },
      },
      {
        displayName: 'Minute',
        name: 'scheduleMinute',
        type: 'number',
        default: 0,
        description: 'Minute of hour to run (0–59)',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
            frequency: ['daily', 'weekly', 'monthly'],
          },
        },
      },
      {
        displayName: 'Days of Week',
        name: 'daysOfWeek',
        type: 'multiOptions',
        options: [
          { name: 'Sunday', value: 0 },
          { name: 'Monday', value: 1 },
          { name: 'Tuesday', value: 2 },
          { name: 'Wednesday', value: 3 },
          { name: 'Thursday', value: 4 },
          { name: 'Friday', value: 5 },
          { name: 'Saturday', value: 6 },
        ],
        default: [1],
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
            frequency: ['weekly'],
          },
        },
      },
      {
        displayName: 'Day of Month',
        name: 'dayOfMonth',
        type: 'number',
        default: 1,
        description: 'Day of month to run (1–31)',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set'],
            scheduleMode: ['frequency'],
            frequency: ['monthly'],
          },
        },
      },
      {
        displayName: 'Cron Expression',
        name: 'cronExpression',
        type: 'string',
        default: '0 9 * * 1',
        placeholder: '0 9 * * 1',
        description: 'A standard 5-field cron expression (minute hour day month weekday)',
        displayOptions: {
          show: {
            resource: ['schedule'],
            operation: ['set', 'describe'],
          },
          hide: {
            scheduleMode: ['frequency'],
          },
        },
      },

      // ─── Task: Create — required fields ────────────────────────────────────
      {
        displayName: 'Name',
        name: 'name',
        type: 'string',
        default: '',
        required: true,
        description: 'Descriptive name of the automation task',
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'URL',
        name: 'url',
        type: 'string',
        default: '',
        required: true,
        description: 'Initial URL to navigate to when the task starts',
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'Mode',
        name: 'mode',
        type: 'options',
        options: [
          { name: 'Scrape', value: 'scrape', description: 'Fast, non-interactive, headless' },
          { name: 'Agent', value: 'agent', description: 'Automated browser interaction, multi-step' },
          { name: 'Headful', value: 'headful', description: 'Visible, interactive debug session' },
        ],
        default: 'scrape',
        required: true,
        description: 'Execution mode for the task',
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['create'],
          },
        },
      },

      // ─── Task: Create / Update — optional fields ───────────────────────────
      {
        displayName: 'Additional Fields',
        name: 'taskAdditionalFields',
        type: 'collection',
        placeholder: 'Add Field',
        default: {},
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['create'],
          },
        },
        options: [
          { displayName: 'Actions (JSON)', name: 'actions', type: 'json', default: '[]', description: 'Array of sequential action step objects' },
          { displayName: 'Cabinet ID', name: 'cabinetId', type: 'string', default: '', description: 'Cabinet used for intercepted downloads and Upload actions that omit their own Cabinet ID; leave empty to use the default Cabinet' },
          { displayName: 'Description', name: 'description', type: 'string', default: '' },
          { displayName: 'Disable Recording', name: 'disableRecording', type: 'boolean', default: false },
          {
            displayName: 'Extraction Format',
            name: 'extractionFormat',
            type: 'options',
            options: [
              { name: 'JSON', value: 'json' },
              { name: 'CSV', value: 'csv' },
            ],
            default: 'json',
          },
          { displayName: 'Extraction Script', name: 'extractionScript', type: 'string', typeOptions: { rows: 3 }, default: '', description: 'Optional post-execution script to extract data' },
          { displayName: 'Human Typing', name: 'humanTyping', type: 'boolean', default: false },
          { displayName: 'Include HTML', name: 'includeHtml', type: 'boolean', default: false },
          { displayName: 'Include Shadow DOM', name: 'includeShadowDom', type: 'boolean', default: true },
          { displayName: 'Rotate Proxies', name: 'rotateProxies', type: 'boolean', default: false },
          { displayName: 'Rotate User Agents', name: 'rotateUserAgents', type: 'boolean', default: false },
          { displayName: 'Rotate Viewport', name: 'rotateViewport', type: 'boolean', default: false },
          { displayName: 'Selector', name: 'selector', type: 'string', default: '', description: 'CSS selector to wait for before starting actions' },
          { displayName: 'Stateless Execution', name: 'statelessExecution', type: 'boolean', default: false },
          { displayName: 'Stealth (JSON)', name: 'stealth', type: 'json', default: '{}', description: 'Stealth/anti-bot config object, e.g. { "allowTypos": true, "cursorGlide": true }' },
          { displayName: 'Variables (JSON)', name: 'variables', type: 'json', default: '{}', description: 'Object of task variable definitions: { name: { type, value } }' },
          { displayName: 'Wait (Seconds)', name: 'wait', type: 'number', default: 3, description: 'Delay after navigation/page loads' },
        ],
      },
      {
        displayName: 'Update Fields',
        name: 'taskUpdateFields',
        type: 'collection',
        placeholder: 'Add Field',
        default: {},
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['update'],
          },
        },
        options: [
          { displayName: 'Actions (JSON)', name: 'actions', type: 'json', default: '[]' },
          { displayName: 'Cabinet ID', name: 'cabinetId', type: 'string', default: '', description: 'Cabinet used for intercepted downloads and Upload actions that omit their own Cabinet ID; leave empty to use the default Cabinet' },
          { displayName: 'Description', name: 'description', type: 'string', default: '' },
          { displayName: 'Disable Recording', name: 'disableRecording', type: 'boolean', default: false },
          {
            displayName: 'Extraction Format',
            name: 'extractionFormat',
            type: 'options',
            options: [
              { name: 'JSON', value: 'json' },
              { name: 'CSV', value: 'csv' },
            ],
            default: 'json',
          },
          { displayName: 'Extraction Script', name: 'extractionScript', type: 'string', typeOptions: { rows: 3 }, default: '' },
          { displayName: 'Human Typing', name: 'humanTyping', type: 'boolean', default: false },
          { displayName: 'Include HTML', name: 'includeHtml', type: 'boolean', default: false },
          { displayName: 'Include Shadow DOM', name: 'includeShadowDom', type: 'boolean', default: true },
          {
            displayName: 'Mode',
            name: 'mode',
            type: 'options',
            options: [
              { name: 'Scrape', value: 'scrape' },
              { name: 'Agent', value: 'agent' },
              { name: 'Headful', value: 'headful' },
            ],
            default: 'scrape',
          },
          { displayName: 'Name', name: 'name', type: 'string', default: '' },
          { displayName: 'Rotate Proxies', name: 'rotateProxies', type: 'boolean', default: false },
          { displayName: 'Rotate User Agents', name: 'rotateUserAgents', type: 'boolean', default: false },
          { displayName: 'Rotate Viewport', name: 'rotateViewport', type: 'boolean', default: false },
          { displayName: 'Selector', name: 'selector', type: 'string', default: '' },
          { displayName: 'Stateless Execution', name: 'statelessExecution', type: 'boolean', default: false },
          { displayName: 'Stealth (JSON)', name: 'stealth', type: 'json', default: '{}' },
          { displayName: 'URL', name: 'url', type: 'string', default: '' },
          { displayName: 'Variables (JSON)', name: 'variables', type: 'json', default: '{}' },
          { displayName: 'Wait (Seconds)', name: 'wait', type: 'number', default: 3 },
        ],
      },

      // ─── Browser: Open ──────────────────────────────────────────────────────
      {
        displayName: 'URL',
        name: 'browserUrl',
        type: 'string',
        default: '',
        description: 'Initial URL to navigate to when the browser opens',
        displayOptions: {
          show: {
            resource: ['browser'],
            operation: ['open'],
          },
        },
      },
      {
        displayName: 'Additional Fields',
        name: 'browserAdditionalFields',
        type: 'collection',
        placeholder: 'Add Field',
        default: {},
        displayOptions: {
          show: {
            resource: ['browser'],
            operation: ['open'],
          },
        },
        options: [
          {
            displayName: 'Mode',
            name: 'mode',
            type: 'options',
            options: [
              { name: 'Headful', value: 'headful' },
              { name: 'Scrape', value: 'scrape' },
              { name: 'Agent', value: 'agent' },
            ],
            default: 'headful',
            description: 'Informational only — only headful is currently supported via the VNC stack',
          },
          { displayName: 'Dev Tools', name: 'devTools', type: 'boolean', default: false, description: 'Whether to open DevTools automatically' },
        ],
      },

      // ─── Inspector: Highlight ───────────────────────────────────────────────
      {
        displayName: 'Session ID',
        name: 'sessionId',
        type: 'string',
        default: '',
        description: 'Active browser session ID. Leave empty to use the current session or launch one via URL.',
        displayOptions: {
          show: {
            resource: ['inspector'],
            operation: ['highlight'],
          },
        },
      },
      {
        displayName: 'URL',
        name: 'inspectorUrl',
        type: 'string',
        default: '',
        description: 'Optional URL to navigate to before highlighting',
        displayOptions: {
          show: {
            resource: ['inspector'],
            operation: ['highlight'],
          },
        },
      },
      {
        displayName: 'Target Hint',
        name: 'targetHint',
        type: 'string',
        default: '',
        description: 'Optional text or hint (e.g. "login button") to find and highlight target elements',
        displayOptions: {
          show: {
            resource: ['inspector'],
            operation: ['highlight'],
          },
        },
      },

    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];

    const credentials = await this.getCredentials('figraniumApi');
    const baseUrl = String(credentials.baseUrl || '').replace(/\/+$/, '');

    if (!baseUrl) {
      throw new NodeOperationError(this.getNode(), 'Base URL is required in credentials.');
    }

    for (let i = 0; i < items.length; i++) {
      const resource = this.getNodeParameter('resource', i) as string;
      const operation = this.getNodeParameter('operation', i) as string;

      let response: IDataObject | IDataObject[];

      // ── EXECUTE ───────────────────────────────────────────────────────────
      if (resource === 'execute') {
        if (operation === 'execute') {
          const taskId = this.getNodeParameter('taskId', i) as string;
          const variablesRaw = this.getNodeParameter('variables', i) as {
            values?: Array<{ name?: string; value?: string }>;
          };

          const variables: IDataObject = {};
          for (const entry of variablesRaw?.values ?? []) {
            const key = (entry.name || '').trim();
            if (key) variables[key] = entry.value ?? '';
          }

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/${encodeURIComponent(taskId)}/api`,
              body: { variables },
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported execute operation: ${operation}`, { itemIndex: i });
        }

      // ── TASK ──────────────────────────────────────────────────────────────
      } else if (resource === 'task') {
        if (operation === 'execute') {
          const taskId = this.getNodeParameter('taskId', i) as string;
          const variablesRaw = this.getNodeParameter('variables', i) as {
            values?: Array<{ name?: string; value?: string }>;
          };

          const variables: IDataObject = {};
          for (const entry of variablesRaw?.values ?? []) {
            const key = (entry.name || '').trim();
            if (key) variables[key] = entry.value ?? '';
          }

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/${encodeURIComponent(taskId)}/api`,
              body: { variables },
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'list') {
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/list`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'create') {
          const name = this.getNodeParameter('name', i) as string;
          const url = this.getNodeParameter('url', i) as string;
          const mode = this.getNodeParameter('mode', i) as string;
          const additionalFields = this.getNodeParameter('taskAdditionalFields', i, {}) as IDataObject;

          const body: IDataObject = { name, url, mode, ...parseJsonFields(this.getNode(), additionalFields) };

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'update') {
          const taskId = this.getNodeParameter('taskId', i) as string;
          const updateFields = this.getNodeParameter('taskUpdateFields', i, {}) as IDataObject;

          const body: IDataObject = parseJsonFields(this.getNode(), updateFields);

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'PATCH' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/${encodeURIComponent(taskId)}`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'delete') {
          const taskId = this.getNodeParameter('taskId', i) as string;

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'DELETE' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/${encodeURIComponent(taskId)}`,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported task operation: ${operation}`, { itemIndex: i });
        }

      // ── EXECUTION ─────────────────────────────────────────────────────────
      } else if (resource === 'execution') {
        if (operation === 'list') {
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/executions/list`,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported execution operation: ${operation}`, { itemIndex: i });
        }

      // ── SCHEDULE ──────────────────────────────────────────────────────────
      } else if (resource === 'schedule') {
        if (operation === 'list') {
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'getAllStatus') {
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules/status/all`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'getStatus') {
          const taskId = this.getNodeParameter('taskIdString', i) as string;
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules/${encodeURIComponent(taskId)}/status`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'delete') {
          const taskId = this.getNodeParameter('taskIdString', i) as string;
          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'DELETE' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules/${encodeURIComponent(taskId)}`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'set') {
          const taskId = this.getNodeParameter('taskIdString', i) as string;
          const enabled = this.getNodeParameter('scheduleEnabled', i) as boolean;
          const mode = this.getNodeParameter('scheduleMode', i) as string;

          const body: IDataObject = { enabled };

          if (mode === 'cron') {
            body.cron = this.getNodeParameter('cronExpression', i) as string;
          } else {
            const freq = this.getNodeParameter('frequency', i) as string;
            body.frequency = freq;
            if (freq === 'interval') {
              body.intervalMinutes = this.getNodeParameter('intervalMinutes', i) as number;
            } else if (freq === 'weekly') {
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
              body.daysOfWeek = this.getNodeParameter('daysOfWeek', i) as number[];
            } else if (freq === 'monthly') {
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
              body.dayOfMonth = this.getNodeParameter('dayOfMonth', i) as number;
            } else {
              // daily
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
            }
          }

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules/${encodeURIComponent(taskId)}`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'describe') {
          const taskId = this.getNodeParameter('taskIdString', i) as string;
          const mode = this.getNodeParameter('scheduleMode', i) as string;
          const body: IDataObject = {};

          if (mode === 'cron') {
            body.cron = this.getNodeParameter('cronExpression', i) as string;
          } else {
            const freq = this.getNodeParameter('frequency', i) as string;
            body.frequency = freq;
            if (freq === 'interval') {
              body.intervalMinutes = this.getNodeParameter('intervalMinutes', i) as number;
            } else if (freq === 'weekly') {
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
              body.daysOfWeek = this.getNodeParameter('daysOfWeek', i) as number[];
            } else if (freq === 'monthly') {
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
              body.dayOfMonth = this.getNodeParameter('dayOfMonth', i) as number;
            } else {
              body.hour = this.getNodeParameter('scheduleHour', i) as number;
              body.minute = this.getNodeParameter('scheduleMinute', i) as number;
            }
          }

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules/${encodeURIComponent(taskId)}/describe`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported schedule operation: ${operation}`, { itemIndex: i });
        }

      // ── BROWSER ───────────────────────────────────────────────────────────
      } else if (resource === 'browser') {
        if (operation === 'open') {
          const browserUrl = this.getNodeParameter('browserUrl', i) as string;
          const additionalFields = this.getNodeParameter('browserAdditionalFields', i, {}) as IDataObject;

          const body: IDataObject = { ...additionalFields };
          if (browserUrl) body.url = browserUrl;

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/browser/open`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported browser operation: ${operation}`, { itemIndex: i });
        }

      // ── INSPECTOR ─────────────────────────────────────────────────────────
      } else if (resource === 'inspector') {
        if (operation === 'highlight') {
          const sessionId = this.getNodeParameter('sessionId', i) as string;
          const inspectorUrl = this.getNodeParameter('inspectorUrl', i) as string;
          const targetHint = this.getNodeParameter('targetHint', i) as string;

          const body: IDataObject = {};
          if (sessionId) body.sessionId = sessionId;
          if (inspectorUrl) body.url = inspectorUrl;
          if (targetHint) body.targetHint = targetHint;

          response = await this.helpers.httpRequestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/inspector/highlight`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported inspector operation: ${operation}`, { itemIndex: i });
        }

      } else {
        throw new NodeOperationError(this.getNode(), `Unsupported resource: ${resource}`, { itemIndex: i });
      }

      // Normalise response: arrays become multiple items, objects become one
      if (Array.isArray(response)) {
        for (const item of response) {
          returnData.push({ json: item as IDataObject, pairedItem: { item: i } });
        }
      } else {
        returnData.push({ json: response as IDataObject, pairedItem: { item: i } });
      }
    }

    return [returnData];
  }
}
