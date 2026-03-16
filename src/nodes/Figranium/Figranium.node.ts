import type {
  IDataObject,
  IExecuteFunctions,
  IHttpRequestMethods,
  ILoadOptionsFunctions,
  INodeExecutionData,
  INodePropertyOptions,
  INodeType,
  INodeTypeDescription,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

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
          const response = await this.helpers.requestWithAuthentication.call(
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
            }))
            .filter((option) => option.value)
            .sort((a, b) => a.name.localeCompare(b.name));
        } catch {
          throw new NodeOperationError(this.getNode(), 'Error fetching tasks from Figranium.', {
            itemIndex: 0,
          });
        }
      },

      async getCredentialsList(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = await this.getCredentials('figraniumApi');
        const baseUrl = String(credentials.baseUrl || '').replace(/\/+$/, '');

        if (!baseUrl) {
          return [];
        }

        try {
          const response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials`,
              json: true,
            },
          );

          const list = Array.isArray(response) ? response : [];
          return list
            .map((cred: IDataObject) => ({
              name: `${String(cred.name || cred.id)} (${String(cred.provider || '')})`,
              value: String(cred.id || ''),
            }))
            .filter((option) => option.value);
        } catch {
          throw new NodeOperationError(this.getNode(), 'Error fetching credentials from Figranium.', {
            itemIndex: 0,
          });
        }
      },
    },
  };

  description: INodeTypeDescription = {
    displayName: 'Figranium',
    name: 'figranium',
    icon: 'file:figranium_icon.svg',
    group: ['transform'],
    version: 1,
    description: 'Interact with Figranium — trigger tasks, inspect executions, manage schedules, and control credentials.',
    defaults: {
      name: 'Figranium',
    },
    inputs: ['main'],
    outputs: ['main'],
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
            name: 'Task',
            value: 'task',
            description: 'Manage and execute automation tasks',
          },
          {
            name: 'Execution',
            value: 'execution',
            description: 'Inspect past execution records',
          },
          {
            name: 'Schedule',
            value: 'schedule',
            description: 'View and manage task schedules',
          },
          {
            name: 'Credential',
            value: 'credential',
            description: 'Manage output provider credentials (e.g. Baserow)',
          },
        ],
        default: 'task',
      },

      // ─── TASK operations ─────────────────────────────────────────────────
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
            name: 'Execute',
            value: 'execute',
            description: 'Run a saved task and return its result',
            action: 'Execute a task',
          },
          {
            name: 'List',
            value: 'list',
            description: 'Return all task IDs and names',
            action: 'List tasks',
          },
        ],
        default: 'execute',
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
            name: 'List',
            value: 'list',
            description: 'Return all tasks that have schedules configured',
            action: 'List schedules',
          },
          {
            name: 'Get Status',
            value: 'getStatus',
            description: 'Get the schedule status and next run time for a specific task',
            action: 'Get schedule status',
          },
          {
            name: 'Set Schedule',
            value: 'set',
            description: 'Create or update a schedule on a task',
            action: 'Set a schedule',
          },
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
        ],
        default: 'list',
      },

      // ─── CREDENTIAL operations ────────────────────────────────────────────
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ['credential'],
          },
        },
        options: [
          {
            name: 'List',
            value: 'list',
            description: 'Return all saved output-provider credentials',
            action: 'List credentials',
          },
          {
            name: 'Create',
            value: 'create',
            description: 'Save a new output-provider credential',
            action: 'Create a credential',
          },
          {
            name: 'Update',
            value: 'update',
            description: 'Update the name or config of an existing credential',
            action: 'Update a credential',
          },
          {
            name: 'Delete',
            value: 'delete',
            description: 'Permanently remove a credential',
            action: 'Delete a credential',
          },
          {
            name: 'Get Baserow Databases',
            value: 'getBaserowDatabases',
            description: 'List all Baserow databases accessible by a credential',
            action: 'Get Baserow databases',
          },
          {
            name: 'Get Baserow Tables',
            value: 'getBaserowTables',
            description: 'List all tables within a Baserow database',
            action: 'Get Baserow tables',
          },
        ],
        default: 'list',
      },

      // ─── Shared: Task ID (execute) ────────────────────────────────────────
      {
        displayName: 'Task',
        name: 'taskId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getTasks',
        },
        default: '',
        required: true,
        description: 'The task to work with',
        displayOptions: {
          show: {
            resource: ['task'],
            operation: ['execute'],
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
        required: false,
        description: 'Key-value pairs passed into the task at runtime',
        typeOptions: {
          multipleValues: true,
        },
        displayOptions: {
          show: {
            resource: ['task'],
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

      // ─── Credential: Create / Update ──────────────────────────────────────
      {
        displayName: 'Credential',
        name: 'credentialId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getCredentialsList',
        },
        default: '',
        required: true,
        description: 'The credential to work with',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['update', 'delete', 'getBaserowDatabases', 'getBaserowTables'],
          },
        },
      },
      {
        displayName: 'Name',
        name: 'credentialName',
        type: 'string',
        default: '',
        required: true,
        description: 'A human-readable name for the credential',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'Provider',
        name: 'credentialProvider',
        type: 'options',
        options: [
          { name: 'Baserow', value: 'baserow' },
        ],
        default: 'baserow',
        required: true,
        description: 'The output provider type',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'Baserow URL',
        name: 'credentialBaseUrl',
        type: 'string',
        default: '',
        placeholder: 'https://baserow.example.com',
        required: true,
        description: 'The base URL of your Baserow instance',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'Baserow Token',
        name: 'credentialToken',
        type: 'string',
        typeOptions: { password: true },
        default: '',
        required: true,
        description: 'Database token from your Baserow account',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['create'],
          },
        },
      },
      // Update fields (optional — only values provided are changed)
      {
        displayName: 'Update Fields',
        name: 'updateFields',
        type: 'collection',
        placeholder: 'Add Field',
        default: {},
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['update'],
          },
        },
        options: [
          {
            displayName: 'Name',
            name: 'name',
            type: 'string',
            default: '',
            description: 'New display name for the credential',
          },
          {
            displayName: 'Baserow URL',
            name: 'baseUrl',
            type: 'string',
            default: '',
            description: 'New Baserow base URL',
          },
          {
            displayName: 'Baserow Token',
            name: 'token',
            type: 'string',
            typeOptions: { password: true },
            default: '',
            description: 'New Baserow database token',
          },
        ],
      },
      // Baserow database ID (for table listing)
      {
        displayName: 'Database ID',
        name: 'databaseId',
        type: 'string',
        default: '',
        required: true,
        description: 'The Baserow database ID to list tables for',
        displayOptions: {
          show: {
            resource: ['credential'],
            operation: ['getBaserowTables'],
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

      // ── TASK ──────────────────────────────────────────────────────────────
      if (resource === 'task') {
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

          response = await this.helpers.requestWithAuthentication.call(
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
          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/tasks/list`,
              json: true,
            },
          ) as IDataObject;
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported task operation: ${operation}`, { itemIndex: i });
        }

      // ── EXECUTION ─────────────────────────────────────────────────────────
      } else if (resource === 'execution') {
        if (operation === 'list') {
          response = await this.helpers.requestWithAuthentication.call(
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
          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/schedules`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'getAllStatus') {
          response = await this.helpers.requestWithAuthentication.call(
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
          response = await this.helpers.requestWithAuthentication.call(
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
          response = await this.helpers.requestWithAuthentication.call(
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

          response = await this.helpers.requestWithAuthentication.call(
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

          response = await this.helpers.requestWithAuthentication.call(
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

      // ── CREDENTIAL ────────────────────────────────────────────────────────
      } else if (resource === 'credential') {
        if (operation === 'list') {
          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials`,
              json: true,
            },
          ) as IDataObject[];
        } else if (operation === 'create') {
          const name = this.getNodeParameter('credentialName', i) as string;
          const provider = this.getNodeParameter('credentialProvider', i) as string;
          const credBaseUrl = this.getNodeParameter('credentialBaseUrl', i) as string;
          const token = this.getNodeParameter('credentialToken', i) as string;

          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'POST' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials`,
              body: {
                name,
                provider,
                config: { baseUrl: credBaseUrl, token },
              },
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'update') {
          const credentialId = this.getNodeParameter('credentialId', i) as string;
          const updateFields = this.getNodeParameter('updateFields', i) as IDataObject;

          const body: IDataObject = {};
          if (updateFields.name) body.name = updateFields.name;
          if (updateFields.baseUrl || updateFields.token) {
            body.config = {};
            if (updateFields.baseUrl) (body.config as IDataObject).baseUrl = updateFields.baseUrl;
            if (updateFields.token) (body.config as IDataObject).token = updateFields.token;
          }

          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'PUT' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials/${encodeURIComponent(credentialId)}`,
              body,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'delete') {
          const credentialId = this.getNodeParameter('credentialId', i) as string;

          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'DELETE' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials/${encodeURIComponent(credentialId)}`,
              json: true,
            },
          ) as IDataObject;
        } else if (operation === 'getBaserowDatabases') {
          const credentialId = this.getNodeParameter('credentialId', i) as string;

          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials/${encodeURIComponent(credentialId)}/proxy/baserow/databases`,
              json: true,
            },
          ) as IDataObject[];
        } else if (operation === 'getBaserowTables') {
          const credentialId = this.getNodeParameter('credentialId', i) as string;
          const databaseId = this.getNodeParameter('databaseId', i) as string;

          response = await this.helpers.requestWithAuthentication.call(
            this,
            'figraniumApi',
            {
              method: 'GET' as IHttpRequestMethods,
              url: `${baseUrl}/api/credentials/${encodeURIComponent(credentialId)}/proxy/baserow/databases/${encodeURIComponent(databaseId)}/tables`,
              json: true,
            },
          ) as IDataObject[];
        } else {
          throw new NodeOperationError(this.getNode(), `Unsupported credential operation: ${operation}`, { itemIndex: i });
        }

      } else {
        throw new NodeOperationError(this.getNode(), `Unsupported resource: ${resource}`, { itemIndex: i });
      }

      // Normalise response: arrays become multiple items, objects become one
      if (Array.isArray(response)) {
        for (const item of response) {
          returnData.push({ json: item as IDataObject });
        }
      } else {
        returnData.push({ json: response as IDataObject });
      }
    }

    return [returnData];
  }
}
