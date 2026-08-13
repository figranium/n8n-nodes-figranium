import type { IExecuteFunctions, ILoadOptionsFunctions, INodeExecutionData, INodePropertyOptions, INodeType, INodeTypeDescription } from 'n8n-workflow';
export interface FigraniumStats {
    runCount: number;
    firstRunTime: number;
    askedMilestone?: boolean;
}
export declare function getStatsPath(): string;
export declare function loadStats(): FigraniumStats;
export declare function saveStats(stats: FigraniumStats): void;
export declare function checkAndDisplayMilestone(): void;
export declare class Figranium implements INodeType {
    methods: {
        loadOptions: {
            getTasks(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]>;
        };
    };
    description: INodeTypeDescription;
    execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;
}
