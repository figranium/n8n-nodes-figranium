import type { IExecuteFunctions, ILoadOptionsFunctions, INodeExecutionData, INodePropertyOptions, INodeType, INodeTypeDescription } from 'n8n-workflow';
export declare class Figranium implements INodeType {
    methods: {
        loadOptions: {
            getTasks(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]>;
        };
    };
    description: INodeTypeDescription;
    execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;
}
