jest.mock("../../genericFunctions/p1LoadParameters/P1LoadParameters");
jest.mock("../../genericFunctions/p1ResolveEsAddress/P1ResolveEsAddress");
jest.mock("../../genericFunctions/p1InitKafka/P1InitKafka");
jest.mock("../../infra/elasticSearch/esBootstrap.js");
jest.mock("../../core/replicaStateStore.js");
jest.mock("../../runtime/replica/replicaLeaderLoop");
jest.mock("../../runtime/processing/processingWorkerPoolRedis");
jest.mock("../../runtime/processing/retryWorker");

const { run } = require("../../path/to/p1StreamPmData");

const p1LoadParameters = require("../../genericFunctions/p1LoadParameters/P1LoadParameters");
const p1ResolveESAddress = require("../../genericFunctions/p1ResolveEsAddress/P1ResolveEsAddress");
const p1InitKafka = require("../../genericFunctions/p1InitKafka/P1InitKafka");
const { ensureIndicesAndMappings } = require("../../infra/elasticSearch/esBootstrap.js");
const { loadLastReplicaTime } = require("../../core/replicaStateStore.js");

describe("p1StreamPmData", () => {

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("throws when parameters not loaded", async () => {
    p1LoadParameters.run.mockResolvedValue({});

    await expect(run()).rejects.toThrow("Parameters could not be loaded");
  });

  test("throws when ES parameters missing", async () => {
    p1LoadParameters.run.mockResolvedValue({
      parameters: {},
      configFile: {}
    });

    await expect(run()).rejects.toThrow(
      "ES address could not be resolved: missing parameters"
    );
  });

  test("throws when ES resolution returns empty", async () => {
    p1LoadParameters.run.mockResolvedValue({
      parameters: {
        parameter: [{ "parameter-name": "p1ResolveEsAddress" }]
      },
      configFile: {}
    });

    p1ResolveESAddress.run.mockResolvedValue({ esAddress: null });

    await expect(run()).rejects.toThrow(
      "ES address could not be resolved"
    );
  });

  test("throws when Kafka initialization fails", async () => {
    p1LoadParameters.run.mockResolvedValue({
      parameters: {
        parameter: [
          { "parameter-name": "p1ResolveEsAddress" },
          { "parameter-name": "p1InitKafka" }
        ]
      },
      configFile: {}
    });

    p1ResolveESAddress.run.mockResolvedValue({ esAddress: {} });

    p1InitKafka.run.mockResolvedValue({});

    await expect(run()).rejects.toThrow(
      "Kafka session could not be established"
    );
  });

  test("runs successfully", async () => {
    p1LoadParameters.run.mockResolvedValue({
      parameters: {
        parameter: [
          { "parameter-name": "p1ResolveEsAddress" },
          { "parameter-name": "p1InitKafka" },
          { "parameter-name": "p1UpdateMwdiReplica" },
          { "parameter-name": "p1ProcessDevice" }
        ]
      },
      configFile: {}
    });

    p1ResolveESAddress.run.mockResolvedValue({ esAddress: {} });

    p1InitKafka.run.mockResolvedValue({
      kafkaConnectionList: []
    });

    ensureIndicesAndMappings.mockResolvedValue();
    loadLastReplicaTime.mockResolvedValue("2020-01-01T00:00:00Z");

    const result = await run();

    expect(result).toHaveProperty("instanceId");
    expect(result).toHaveProperty("appState");
    expect(result).toHaveProperty("kafkaConnectionList");
  });

});
