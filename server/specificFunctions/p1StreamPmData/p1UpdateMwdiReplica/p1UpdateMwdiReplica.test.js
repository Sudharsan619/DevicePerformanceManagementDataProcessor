// Mock external dependencies
jest.mock("../../../infra/onf/onfAdapter", () => ({
  getEsClient: jest.fn()
}));

jest.mock("../../../utils/functionTree", () => ({
  getParamFromFunction: jest.fn()
}));

jest.mock("../../../utils/retry", () => ({
  withRetry: jest.fn()
}));

jest.mock("../../../infra/redis/redisStreamQueue", () => ({
  ensureGroup: jest.fn(),
  enqueueMountNames: jest.fn()
}));

// Imports
const onfAdapter = require("../../../infra/onf/onfAdapter");
const { getParamFromFunction } = require("../../../utils/functionTree");
const { withRetry } = require("../../../utils/retry");
const redisQueue = require("../../../infra/redis/redisStreamQueue");

const { run } = require("./P1UpdateMwdiReplica");

describe("P1UpdateMwdiReplica.run", () => {

  beforeEach(() => {
    jest.clearAllMocks();
  });

  // Should throw error when required inputs are missing
  test("throws error when mandatory parameters are missing", async () => {
    await expect(run({})).rejects.toThrow(
      "parameters, mwdiEsClient, mwdiReplicaEsClient and loggingEsClient are mandatory"
    );
  });

  // Happy path: reindex + extract mount names + enqueue
  test("reindexes data and enqueues mount names", async () => {
    const sourceClient = {
      reindex: jest.fn().mockResolvedValue({
        body: { created: 1, updated: 0, total: 1 }
      })
    };

    const replicaClient = {
      search: jest.fn().mockResolvedValue({
        body: {
          hits: {
            hits: [
              { _source: { mountName: "mount-1" } },
              { _source: { uuid: "uuid-2" } }
            ]
          }
        }
      })
    };

    const loggingClient = {
      index: jest.fn().mockResolvedValue({})
    };

    onfAdapter.getEsClient
      .mockResolvedValueOnce(sourceClient)
      .mockResolvedValueOnce(replicaClient)
      .mockResolvedValueOnce(loggingClient);

    getParamFromFunction.mockImplementation(
      (_, __, ___, defaultValue) => defaultValue
    );

    withRetry.mockImplementation(async (fn) => fn());

    redisQueue.ensureGroup.mockResolvedValue();
    redisQueue.enqueueMountNames.mockResolvedValue();

    const request = {
      parameters: {},
      mwdiEsClient: { uuid: "src", "index-alias": "src-index" },
      mwdiReplicaEsClient: { uuid: "rep", "index-alias": "replica-index" },
      loggingEsClient: { uuid: "log", "index-alias": "log-index" },
      runtimeConfig: {},
      logger: { error: jest.fn(), info: jest.fn() }
    };

    const result = await run(request);

    expect(result.updatedMountNames).toEqual(["mount-1", "uuid-2"]);
    expect(result.timestamp).toBeDefined();

    expect(redisQueue.ensureGroup).toHaveBeenCalled();
    expect(redisQueue.enqueueMountNames).toHaveBeenCalledWith(
      ["mount-1", "uuid-2"],
      expect.any(Object),
      request.logger
    );
  });

  // Failure path: reindex fails but flow continues
  test("handles reindex failure gracefully", async () => {
    const sourceClient = {
      reindex: jest.fn().mockRejectedValue(new Error("reindex failed"))
    };

    const replicaClient = {
      search: jest.fn().mockResolvedValue({
        body: { hits: { hits: [] } }
      })
    };

    const loggingClient = {
      index: jest.fn().mockResolvedValue({})
    };

    onfAdapter.getEsClient
      .mockResolvedValueOnce(sourceClient)
      .mockResolvedValueOnce(replicaClient)
      .mockResolvedValueOnce(loggingClient);

    getParamFromFunction.mockImplementation(
      (_, __, ___, defaultValue) => defaultValue
    );

    withRetry.mockImplementation(async (fn) => fn());

    redisQueue.ensureGroup.mockResolvedValue();
    redisQueue.enqueueMountNames.mockResolvedValue();

    const request = {
      parameters: {},
      mwdiEsClient: { uuid: "src", "index-alias": "src-index" },
      mwdiReplicaEsClient: { uuid: "rep", "index-alias": "replica-index" },
      loggingEsClient: { uuid: "log", "index-alias": "log-index" },
      runtimeConfig: {},
      logger: { error: jest.fn(), info: jest.fn() }
    };

    const result = await run(request);

    expect(result.updatedMountNames).toEqual([]);
    expect(loggingClient.index).toHaveBeenCalled();
  });

});
