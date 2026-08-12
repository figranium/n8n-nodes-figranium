import type { ICredentialTestRequest, ICredentialType, INodeProperties } from 'n8n-workflow';
export declare class FigraniumApi implements ICredentialType {
    name: string;
    displayName: string;
    icon: {
        readonly light: "file:figranium_icon_light.svg";
        readonly dark: "file:figranium_icon_dark.svg";
    };
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
    test: ICredentialTestRequest;
}
