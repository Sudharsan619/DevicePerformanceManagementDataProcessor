const { run } = require("./P1UpdateMwdiReplica");

jest.mock("../../../infra/onf/onfAdapter", () => ({
  getEsClient: jest.fn()
}));

jest.mock("../../../utils/functionTree", () => ({
  getParamFromFunction: jest.fn((p, f, k, d) => d)
}));

jest.mock("../../../utils/retry", () => ({
  withRetry: jest.fn(fn => fn())
}));

jest.mock("../../../infra/redis/redisStreamQueue", () => ({
  ensureGroup: jest.fn(),
  enqueueMountNames: jest.fn()
}));

const onfAdapter = require("../../../infra/onf/onfAdapter");
const redisQueue = require("../../../infra/redis/redisStreamQueue");

describe("p1UpdateMwdiReplica", () => {

  let logger;
  let base;

  beforeEach(() => {
    jest.resetAllMocks();
    logger = { info: jest.fn(), error: jest.fn() };

    base = {
      parameters: {},
      mwdiEsClient: { uuid: "mwdi", "index-alias": "mwdi" },
      mwdiReplicaEsClient: { uuid: "replica", "index-alias": "mwdireplica" },
      loggingEsClient: { uuid: "log", "index-alias": "logging" },
      lastReplicaTime: "2026-01-01T00:00:00.000Z",
      runtimeConfig: { redis: { enqueueBatchSize: 5, enqueuePauseMs: 5 }},
      logger
    };
  });

  test("happy path", async () => {
    const mockReindex = { body: { created: 2, updated: 0, total: 2 }};
    const mockSearch = {
      body: { hits: { hits: [
        { _source: { "mount-name": "A" }},
        { _source: { "mount-name": "B" }}
      ]}}
    };

    onfAdapter.getEsClient
      .mockResolvedValueOnce({ reindex: jest.fn().mockResolvedValue(mockReindex) })
      .mockResolvedValueOnce({ search: jest.fn().mockResolvedValue(mockSearch) })
      .mockResolvedValueOnce({ index: jest.fn().mockResolvedValue({}) });

    redisQueue.ensureGroup.mockResolvedValue();
    redisQueue.enqueueMountNames.mockResolvedValue();

    const res = await run(base);

    expect(res["updated-mount-names"]).toEqual(["A", "B"]);
    expect(typeof res.timestamp).toBe("string");
    expect(redisQueue.enqueueMountNames).toHaveBeenCalled();
  });

  test("missing inputs", async () => {
    const res = await run({
      parameters: null,
      mwdiEsClient: null,
      mwdiReplicaEsClient: null,
      loggingEsClient: null,
      logger
    });
    expect(res).toBe("unknown error occurred");
  });

  test("mwdi es connect error", async () => {
    onfAdapter.getEsClient.mockRejectedValue(new Error("x"));
    const res = await run(base);
    expect(res).toBe("connection to MWDI ES failed");
  });

  test("replica es connect error", async () => {
    onfAdapter.getEsClient
      .mockResolvedValueOnce({})
      .mockRejectedValueOnce(new Error("x"));
    const res = await run(base);
    expect(res).toBe("connection to MWDI Replica ES failed");
  });

  test("logging es connect error", async () => {
    onfAdapter.getEsClient
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockRejectedValueOnce(new Error("x"));
    const res = await run(base);
    expect(res).toBe("connection to Logging ES failed");
  });

  test("reindex error", async () => {
    onfAdapter.getEsClient
      .mockResolvedValueOnce({ reindex: () => { throw new Error("x") }})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({});
    const res = await run(base);
    expect(res).toBe("data replication failed");
  });

  test("search error", async () => {
    onfAdapter.getEsClient
      .mockResolvedValueOnce({ reindex: jest.fn().mockResolvedValue({ body: {} }) })
      .mockResolvedValueOnce({ search: () => { throw new Error("x") }})
      .mockResolvedValueOnce({});
    const res = await run(base);
    expect(res).toBe("data replication failed");
  });

  test("redis enqueue error", async () => {
    const mockReindex = { body: { total: 1 }};
    const mockSearch = { body: { hits: { hits: [{ _source: { "mount-name": "X" }}] } }};

    onfAdapter.getEsClient
      .mockResolvedValueOnce({ reindex: jest.fn().mockResolvedValue(mockReindex) })
      .mockResolvedValueOnce({ search: jest.fn().mockResolvedValue(mockSearch) })
      .mockResolvedValueOnce({ index: jest.fn().mockResolvedValue({}) });

    redisQueue.ensureGroup.mockResolvedValue();
    redisQueue.enqueueMountNames.mockRejectedValue(new Error("x"));

    const res = await run(base);
    expect(res).toBe("unknown error occurred");
  });

  test("logging write error", async () => {
    const mockReindex = { body: { total: 1 }};
    const mockSearch = { body: { hits: { hits: [] }}};

    onfAdapter.getEsClient
      .mockResolvedValueOnce({ reindex: jest.fn().mockResolvedValue(mockReindex) })
      .mockResolvedValueOnce({ search: jest.fn().mockResolvedValue(mockSearch) })
      .mockResolvedValueOnce({ index: () => { throw new Error("x") }});

    redisQueue.ensureGroup.mockResolvedValue();
    redisQueue.enqueueMountNames.mockResolvedValue();

    const res = await run(base);
    expect(res).toBe("connection to Logging ES failed");
  });
});