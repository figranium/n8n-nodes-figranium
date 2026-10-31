"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FigraniumApi = void 0;
class FigraniumApi {
    constructor() {
        this.name = 'figraniumApi';
        this.displayName = 'Figranium API';
        this.icon = { light: 'file:figranium_icon_light.svg', dark: 'file:figranium_icon_dark.svg' };
        this.documentationUrl = 'https://figranium.dev/docs/n8n-integration';
        this.properties = [
            {
                displayName: 'Base URL',
                name: 'baseUrl',
                type: 'string',
                default: 'https://cloud.figranium.dev',
                required: true,
                description: 'Figranium Cloud URL or your self-hosted server URL',
            },
            {
                displayName: 'API Key',
                name: 'apiKey',
                type: 'string',
                typeOptions: { password: true },
                default: '',
                required: true,
                description: 'API key from Figranium Settings',
            },
        ];
        this.authenticate = {
            type: 'generic',
            properties: {
                headers: {
                    'x-api-key': '={{$credentials.apiKey}}',
                },
            },
        };
        this.test = {
            request: {
                baseURL: '={{$credentials.baseUrl}}',
                url: '/api/tasks/list',
                method: 'GET',
                disableFollowRedirect: true,
            },
        };
    }
}
exports.FigraniumApi = FigraniumApi;
