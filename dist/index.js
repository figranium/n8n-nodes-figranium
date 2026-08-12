"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FigraniumApi = exports.Figranium = void 0;
const Figranium_node_1 = require("./nodes/Figranium/Figranium.node");
Object.defineProperty(exports, "Figranium", { enumerable: true, get: function () { return Figranium_node_1.Figranium; } });
const FigraniumApi_credentials_1 = require("./credentials/FigraniumApi.credentials");
Object.defineProperty(exports, "FigraniumApi", { enumerable: true, get: function () { return FigraniumApi_credentials_1.FigraniumApi; } });
