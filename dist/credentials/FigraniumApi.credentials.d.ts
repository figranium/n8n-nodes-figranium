import type { ICredentialType, INodeProperties } from 'n8n-workflow';
export declare class FigraniumApi implements ICredentialType {
    name: string;
    displayName: string;
    documentationUrl: string;
    properties: INodeProperties[];
    authenticate: {
        readonly type: "generic";
        readonly properties: {
            readonly headers: {
                readonly 'x-api-key': "={{$credentials.apiKey}}";
            };
        };
    };
}
