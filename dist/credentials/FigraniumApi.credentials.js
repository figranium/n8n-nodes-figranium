"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FigraniumApi = void 0;
class FigraniumApi {
    constructor() {
        this.name = 'figraniumApi';
        this.displayName = 'Figranium API';
        this.documentationUrl = 'https://figranium.com/docs/api-authentication-and-secure-access';
        this.properties = [
            {
                displayName: 'Base URL',
                name: 'baseUrl',
                type: 'string',
                default: 'http://localhost:11345',
                required: true,
                description: 'Figranium server base URL',
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
            },
        };
    }
}
exports.FigraniumApi = FigraniumApi;
