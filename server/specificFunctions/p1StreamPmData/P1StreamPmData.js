const os = require("os");
const crypto = require("crypto");

const { findFunctionNode, getParamFromFunction } = require("../../utils/functionTree");
const { acquireLock, releaseLock } = require("../../infra/redis/redisLock");
const { sleep } = require("../../utils/retry");
const { loadRuntimeConfig } = require("../../utils/config");
const { AppState } = require("../../core/appState");
const { registerGracefulShutdown } = require("../../core/gracefulShutdown");

const { ensureIndicesAndMappings } = require("../../infra/elasticSearch/esBootstrap.js");
const { loadLastReplicaTime } = require("../../core/replicaStateStore.js");

const p1LoadParameters = require("../../genericFunctions/p1LoadParameters/P1LoadParameters");
const p1ResolveESAddress = require("../../genericFunctions/p1ResolveEsAddress/P1ResolveEsAddress");
const p1InitKafka = require("../../genericFunctions/p1InitKafka/P1InitKafka");

const p1MaintainDs = require("./p1MaintainDs/P1MaintainDs");
const { startReplicaLeaderLoop } = require("../../runtime/replica/replicaLeaderLoop");
const { startProcessingWorkerPoolRedis } = require("../../runtime/processing/processingWorkerPoolRedis");
const { startRetryWorkerPool } = require("../../runtime/processing/retryWorker");

const logger = require('../../service/LoggingService.js').getLogger();
const appState = new AppState();


// Cleanup job loop using leader lock
async function startCleanupLeaderLoop(context) {
  const lockKey = "dpmdp:lock:cleanup";
  const ttlMs = context.cleanupLockTtlMs || 300000;

  const cleanupPeriodHours = Number(
    getParamFromFunction(context.cleanupParameters, "p1MaintainDs", "dataStoreCleanupPeriod", 12)
  );

  while (true) {
    const token = await acquireLock(lockKey, ttlMs, context.logger);

    if (!token) {
      await sleep(10000);
      continue;
    }

    try {
      context.logger.info("Running cleanup job");

      if (!context.cleanupParameters || !context.dataStoreEsClient) {
        throw new Error("DataStore maintenance could not be processed");
      }

      await p1MaintainDs.run({
        parameters: context.cleanupParameters,
        dataStoreEsClient: context.dataStoreEsClient,
        loggingEsClient: context.loggingEsClient,
        logger: context.logger
      });

    } finally {
      await releaseLock(lockKey, token, context.logger).catch(() => {});
    }

    await sleep(cleanupPeriodHours * 3600 * 1000);
  }
}


// Main service entrypoint
async function run() {
  debugger;

  logger.info({ service: "p1StreamPmData" }, "Service starting");

  const runtimeConfig = loadRuntimeConfig() || {};
  const redisConfig = runtimeConfig.redis || {};
  const serviceConfig = runtimeConfig.service || {};

  const instanceId = `${os.hostname()}-${process.pid}-${crypto.randomUUID()}`;

  registerGracefulShutdown(appState, logger, {
    shutdownGraceMs: serviceConfig.shutdownGraceMs || 30000
  });

  logger.info("Loading parameters");

  // Load parameters from config
  const loaded = await p1LoadParameters.run({
    "function-name": "p1StreamPmData",
  });

  if (!loaded || !loaded.parameters || !loaded.configFile) {
    throw new Error("Parameters could not be loaded");
  }

  logger.info("Resolving Elasticsearch clients");

  // Extract function-specific parameters
  const p1ResolveEsAddressParameters = findFunctionNode(loaded.parameters, "p1ResolveEsAddress");
  const p1InitKafkaParameters = findFunctionNode(loaded.parameters, "p1InitKafka");
  const p1UpdateMwdiReplicaParameters = findFunctionNode(loaded.parameters, "p1UpdateMwdiReplica");
  const p1ProcessDeviceParameters = findFunctionNode(loaded.parameters, "p1ProcessDevice");
  const p1MaintainDsParameters = findFunctionNode(loaded.parameters, "p1MaintainDs");

  if (!p1ResolveEsAddressParameters) {
    throw new Error("ES address could not be resolved: missing parameters");
  }

  // Resolve ES clients
  const mwdiEsClient = (
    await p1ResolveESAddress.run({
      parameters: p1ResolveEsAddressParameters,
      "config-file": loaded.configFile,
      "es-name": "mwdiEsClient"
    })
  ).esAddress;

  const mwdiReplicaEsClient = (
    await p1ResolveESAddress.run({
      parameters: p1ResolveEsAddressParameters,
      "config-file": loaded.configFile,
      "es-name": "mwdiReplicaEsClient"
    })
  ).esAddress;

  const loggingEsClient = (
    await p1ResolveESAddress.run({
      parameters: p1ResolveEsAddressParameters,
      "config-file": loaded.configFile,
      "es-name": "loggingEsClient"
    })
  ).esAddress;

  const dataStoreEsClient = (
    await p1ResolveESAddress.run({
      parameters: p1ResolveEsAddressParameters,
      "config-file": loaded.configFile,
      "es-name": "dataStoreEsClient"
    })
  ).esAddress;

  if (!mwdiEsClient || !mwdiReplicaEsClient || !loggingEsClient || !dataStoreEsClient) {
    throw new Error("ES address could not be resolved");
  }

  await ensureIndicesAndMappings(
    { mwdiReplicaEsClient, loggingEsClient, dataStoreEsClient },
    logger
  );

  logger.info("Elasticsearch indices ensured");

  // Restore replication state
  const restoredLastReplicaTime = await loadLastReplicaTime(loggingEsClient, logger);
  appState.lastReplicaTime = restoredLastReplicaTime;

  logger.info(
    { lastReplicaTime: restoredLastReplicaTime },
    "Replica state restored"
  );

  // Initialize Kafka
  logger.info("Initializing Kafka");

  const kafkaInit = await p1InitKafka.run({
    parameters: p1InitKafkaParameters,
    "config-file": loaded.configFile,
    logger
  });

  if (!kafkaInit || !kafkaInit.kafkaConnectionList) {
    throw new Error("Kafka session could not be established");
  }

  logger.info(
    { kafkaConnections: kafkaInit.kafkaConnectionList },
    "Kafka initialized"
  );

  // Start background processes
  logger.info("Starting replica leader loop");

  startReplicaLeaderLoop({
    logger,
    appState,
    updateParameters: p1UpdateMwdiReplicaParameters,
    mwdiEsClient,
    mwdiReplicaEsClient,
    loggingEsClient,
    maxQueueLengthBeforeReplicaPause: Number(redisConfig.maxQueueLengthBeforeReplicaPause) || 20000,
    replicaPauseMsWhenBacklogged: Number(redisConfig.replicaPauseMsWhenBacklogged) || 30000,
    replicaLockTtlMs: redisConfig.replicaLockTtlMs || 60000
  }).catch((error) => logger.error({ error }, "Replica loop crashed"));

  logger.info(
    { workerCount: Number(serviceConfig.concurrency || 4) },
    "Starting processing worker pool"
  );

  startProcessingWorkerPoolRedis({
    logger,
    instanceId,
    appState,
    workerCount: Number(serviceConfig.concurrency || 4),
    processDeviceParameters: p1ProcessDeviceParameters,
    configFile: loaded.configFile,
    mwdiReplicaEsClient,
    dataStoreEsClient
  }).catch((error) => logger.error({ error }, "Worker pool crashed"));

  startRetryWorkerPool({
    logger,
    instanceId,
    appState,
    workerCount: 1,
    retryDelayMs: Number(redisConfig.retryIntervalMs || 10000)
  }).catch((error) => logger.error({ error }, "Retry worker crashed"));

  logger.info({ instanceId }, "Service initialized successfully");

  return {
    instanceId,
    appState,
    kafkaConnectionList: kafkaInit.kafkaConnectionList
  };
}


module.exports = { run };


// CLI entry point
if (require.main === module) {
  run()
    .then((res) => console.log("Run completed:", res))
    .catch((err) => console.error("Run failed:", err));
}
