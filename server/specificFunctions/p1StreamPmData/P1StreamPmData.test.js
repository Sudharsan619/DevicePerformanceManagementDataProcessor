jest.mock("os", () => ({
  hostname: jest.fn(() => "test-host")
}));

jest.mock("crypto", () => ({
  randomUUID: jest.fn(() => "uuid-123")
}));

jest.mock("../../utils/functionTree", () => ({
  findFunctionNode: jest.fn(() => ({})),
  getParamFromFunction: jest.fn(() => 1)
}));

jest.mock("../../utils/config", () => ({
  loadRuntimeConfig: jest.fn(() => ({
    redis: {},
    service: {}
  }))
}));

jest.mock("../../core/appState", () => ({
  AppState: jest.fn().mockImplementation(() => ({}))
}));

jest.mock("../../core/gracefulShutdown", () => ({
  registerGracefulShutdown: jest.fn()
}));

jest.mock("../../infra/elasticSearch/esBootstrap.js", () => ({
  ensureIndicesAndMappings: jest.fn()
}));

jest.mock("../../core/replicaStateStore.js", () => ({
  loadLastReplicaTime: jest.fn(() => Promise.resolve("time"))
}));

jest.mock("../../genericFunctions/p1LoadParameters/P1LoadParameters", () => ({
  run: jest.fn(() =>
    Promise.resolve({ parameters: {}, configFile: {} })
  )
}));

jest.mock("../../genericFunctions/p1ResolveEsAddress/P1ResolveEsAddress", () => ({
  run: jest.fn(() =>
    Promise.resolve({ esAddress: { "index-alias": "test-index" } })
  )
}));

jest.mock("../../genericFunctions/p1InitKafka/P1InitKafka", () => ({
  run: jest.fn(() =>
    Promise.resolve({ kafkaConnectionList: ["kafka1"] })
  )
}));

// ✅ FIX: MUST return Promise (for .catch)
jest.mock("../../runtime/replica/replicaLeaderLoop", () => ({
  startReplicaLeaderLoop: jest.fn(() => Promise.resolve())
}));

jest.mock("../../runtime/processing/processingWorkerPoolRedis", () => ({
  startProcessingWorkerPoolRedis: jest.fn(() => Promise.resolve())
}));

jest.mock("../../runtime/processing/retryWorker", () => ({
  startRetryWorkerPool: jest.fn(() => Promise.resolve())
}));

// ✅ FIX: mock kafka worker (not missing file)
jest.mock("../../runtime/kafka/kafkaOutboundWorker", () => ({
  startKafkaOutboundWorkerPool: jest.fn(() => Promise.resolve())
}));

jest.mock("../../service/LoggingService.js", () => ({
  getLogger: () => ({
    error: jest.fn(),
    info: jest.fn()
  })
}));

const { run } = require("./P1StreamPmData");

describe("run()", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("should initialize system and return expected output", async () => {
    const result = await run();

    expect(result).toHaveProperty("instanceId");
    expect(result).toHaveProperty("appState");
    expect(result).toHaveProperty("kafkaConnectionList");

    expect(result.kafkaConnectionList).toEqual(["kafka1"]);
  });
});