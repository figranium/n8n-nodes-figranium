'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const ts = require('typescript');

function loadTypeScript(relativePath) {
  const source = fs.readFileSync(path.join(__dirname, '..', relativePath), 'utf8');
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const exports = {};
  vm.runInNewContext(compiled, {
    exports,
    require(id) {
      if (id === 'n8n-workflow') {
        return {
          NodeConnectionTypes: { Main: 'main' },
          NodeOperationError: class NodeOperationError extends Error {},
        };
      }
      throw new Error('Unexpected runtime dependency: ' + id);
    },
  }, { filename: relativePath });
  return exports;
}

const { Figranium } = loadTypeScript('nodes/Figranium/Figranium.node.ts');
const { FigraniumApi } = loadTypeScript('credentials/FigraniumApi.credentials.ts');

test('every authenticated operation refuses HTTP redirects', async () => {
  const node = new Figranium();
  const cases = [
    { resource: 'execute', operation: 'execute', taskId: 'task-1', variables: { values: [] } },
    { resource: 'task', operation: 'list' },
    { resource: 'task', operation: 'create', name: 'Example', url: 'https://example.com', mode: 'scrape', taskAdditionalFields: {} },
    { resource: 'task', operation: 'update', taskId: 'task-1', taskUpdateFields: {} },
    { resource: 'task', operation: 'delete', taskId: 'task-1' },
    { resource: 'execution', operation: 'list' },
    { resource: 'schedule', operation: 'list' },
    { resource: 'schedule', operation: 'getAllStatus' },
    { resource: 'schedule', operation: 'getStatus', taskIdString: 'task-1' },
    { resource: 'schedule', operation: 'delete', taskIdString: 'task-1' },
    { resource: 'schedule', operation: 'set', taskIdString: 'task-1', scheduleMode: 'frequency', frequency: 'daily', scheduleEnabled: true, scheduleHour: 9, scheduleMinute: 0 },
    { resource: 'schedule', operation: 'describe', taskIdString: 'task-1', scheduleMode: 'frequency', frequency: 'daily', scheduleHour: 9, scheduleMinute: 0 },
  ];
  for (const params of cases) {
    const requests = [];
    const context = {
      getInputData: () => [{ json: {} }],
      getCredentials: async () => ({ baseUrl: 'https://figranium.example' }),
      getNodeParameter: (name) => params[name],
      getNode: () => ({ name: 'Figranium' }),
      helpers: {
        httpRequestWithAuthentication: async function (credential, options) {
          assert.equal(credential, 'figraniumApi');
          requests.push(options);
          return { ok: true };
        },
      },
    };
    await node.execute.call(context);
    assert.equal(requests.length, 1, params.resource + '/' + params.operation);
    assert.equal(requests[0].disableFollowRedirect, true, params.resource + '/' + params.operation);
    assert.match(requests[0].url, /^https:\/\/figranium\.example\/api\//);
  }
});

test('Task dropdown also refuses authenticated redirects', async () => {
  const node = new Figranium();
  let request;
  await node.methods.loadOptions.getTasks.call({
    getCredentials: async () => ({ baseUrl: 'https://figranium.example' }),
    helpers: {
      httpRequestWithAuthentication: async function (_credential, options) {
        request = options;
        return { tasks: [] };
      },
    },
  });
  assert.equal(request.disableFollowRedirect, true);
});

test('credential verification refuses redirects too', () => {
  const credential = new FigraniumApi();
  assert.equal(credential.test.request.disableFollowRedirect, true);
});
