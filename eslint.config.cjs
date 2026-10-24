// ESLint 10 uses flat config; preserve the existing n8n node and credential rules.
const parser = require('@typescript-eslint/parser');
const n8n = require('eslint-plugin-n8n-nodes-base');

const typescript = {
  languageOptions: {
    parser,
    parserOptions: { ecmaVersion: 'latest', sourceType: 'module' },
  },
  plugins: { 'n8n-nodes-base': n8n },
};

module.exports = [
  {
    ...typescript,
    files: ['nodes/**/*.ts', 'index.ts'],
    rules: {
      ...n8n.configs.nodes.rules,
      'n8n-nodes-base/node-class-description-inputs-wrong-regular-node': 'off',
      'n8n-nodes-base/node-class-description-outputs-wrong': 'off',
    },
  },
  {
    ...typescript,
    files: ['credentials/**/*.ts'],
    rules: {
      ...n8n.configs.credentials.rules,
      'n8n-nodes-base/cred-class-field-documentation-url-miscased': 'off',
    },
  },
];
