const onfAdapter = require("../../../infra/onf/onfAdapter");
const { getParamFromFunction } = require("../../../utils/functionTree");
const { withRetry } = require("../../../utils/retry");
const redisQueue = require("../../../infra/redis/redisStreamQueue");

async function run(request) {

  const {
    parameters,
    mwdiEsClient,
    mwdiReplicaEsClient,
    loggingEsClient,
    lastReplicaTime,
    runtimeConfig,
    logger
  } = request;

  logger.info({ label: "p1UpdateMwdiReplica.start", lastReplicaTime }, "Replica job started");

  if (!parameters || !mwdiEsClient || !mwdiReplicaEsClient || !loggingEsClient) {
    logger.error({ label: "p1UpdateMwdiReplica.validation" }, "Missing required inputs");
    return 'unknown error occurred';
  }

  let sourceClient;
  let replicaClient;
  let loggingClient;

  try {
    sourceClient = await onfAdapter.getEsClient(false, mwdiEsClient.uuid, mwdiEsClient, logger);
  } catch (e) {
    logger.error({ label: "mwdi-client-error", error: e.message || e });
    return 'connection to MWDI ES failed';
  }

  try {
    replicaClient = await onfAdapter.getEsClient(false, mwdiReplicaEsClient.uuid, mwdiReplicaEsClient, logger);
  } catch (e) {
    logger.error({ label: "replica-client-error", error: e.message || e });
    return 'connection to MWDI Replica ES failed';
  }

  try {
    loggingClient = await onfAdapter.getEsClient(false, loggingEsClient.uuid, loggingEsClient, logger);
  } catch (e) {
    logger.error({ label: "logging-client-error", error: e.message || e });
    return 'connection to Logging ES failed';
  }

  const jobName = getParamFromFunction(parameters, "p1UpdateMwdiReplica", "jobName", "mwdi-replica-update-job");

  const lastUpdatedField = getParamFromFunction(
    parameters,
    "p1UpdateMwdiReplica",
    "lastUpdatedField",
    "last-complete-control-construct-update-time"
  );

  const overlapMs = Number(getParamFromFunction(parameters, "p1UpdateMwdiReplica", "overlapMs", 60000));
  const reqPerSec = Number(getParamFromFunction(parameters, "p1UpdateMwdiReplica", "reqPerSec", 2));
  const scrollSize = Number(getParamFromFunction(parameters, "p1UpdateMwdiReplica", "scrollSize", 200));

  const scrollTtl =
    String(getParamFromFunction(parameters, "p1UpdateMwdiReplica", "scrollTtl", 2)) + "m";

  const now = Date.now();
  const lastTimestamp = lastReplicaTime
    ? new Date(lastReplicaTime).getTime()
    : now - overlapMs;

  const periodStartTime = new Date(lastTimestamp - overlapMs).toJSON();
  const periodEndTime = new Date(now).toJSON();

  let statusMessage = "SUCCESS";
  let reindexResp;

  try {
    logger.info(
      {
        label: "p1UpdateMwdiReplica.reindex.start",
        sourceIndex: mwdiEsClient["index-alias"],
        destIndex: mwdiReplicaEsClient["index-alias"],
        periodStartTime,
        periodEndTime
      },
      "Starting reindex operation"
    );

    reindexResp = await withRetry(
      async () =>
        sourceClient.reindex({
          refresh: true,
          wait_for_completion: true,
          requests_per_second: reqPerSec,
          scroll: scrollTtl,
          body: {
            source: {
              index: mwdiEsClient["index-alias"],
              size: scrollSize,
              query: {
                bool: {
                  must: [
                    { exists: { field: "core-model-1-4:control-construct" }},
                    {
                      range: {
                        [lastUpdatedField]: {
                          gt: periodStartTime,
                          lte: periodEndTime
                        }
                      }
                    }
                  ]
                }
              }
            },
            dest: {
              index: mwdiReplicaEsClient["index-alias"],
              op_type: "index"
            },
            conflicts: "proceed"
          }
        }),
      {
        label: "p1UpdateMwdiReplica.reindex",
        retryIntervalMs: 10000,
        logger
      }
    );

    logger.info(
      {
        label: "p1UpdateMwdiReplica.reindex.success",
        created: reindexResp?.body?.created,
        updated: reindexResp?.body?.updated,
        total: reindexResp?.body?.total
      },
      "Reindex completed successfully"
    );

  } catch (error) {
    statusMessage = 'data replication failed';

    logger.error(
      { label: "p1UpdateMwdiReplica.reindex.error", error: error.message || error },
      "data replication failed"
    );

    try {
      await loggingClient.index({
        index: loggingEsClient["index-alias"],
        document: {
          jobName,
          periodStartTime,
          periodEndTime,
          status: "FAILED",
          error: statusMessage,
          lastReplicaTime: new Date().toJSON(),
          timestamp: new Date().toJSON()
        }
      });
    } catch (e) {
      logger.error({ label: "logging-failure", error: e.message || e });
      return 'connection to Logging ES failed';
    }

    return 'data replication failed';
  }

  logger.info(
    { label: "p1UpdateMwdiReplica.search.start" },
    "Searching replica index"
  );

  let replicaResponse;

  try {
    replicaResponse = await withRetry(
      async () =>
        replicaClient.search({
          index: mwdiReplicaEsClient["index-alias"],
          scroll: scrollTtl,
          size: scrollSize,
          body: {
            query: {
              bool: {
                must: [
                  { exists: { field: "core-model-1-4:control-construct" }},
                  {
                    range: {
                      [lastUpdatedField]: {
                        gt: periodStartTime,
                        lte: periodEndTime
                      }
                    }
                  }
                ]
              }
            }
          }
        }),
      {
        label: "p1UpdateMwdiReplica.search",
        retryIntervalMs: 10000,
        logger
      }
    );
  } catch (error) {
    logger.error({ label: "search-failure", error: error.message || error });
    return 'data replication failed';
  }

  
  const hits = (((replicaResponse || {}).body?.hits || {}).hits || []);

  logger.info(
    { label: "p1UpdateMwdiReplica.search.result", count: hits.length },
    "Replica search completed"
  );

  const updatedMountNames = hits
    .map((hit) => {
      const src = hit._source || {};
      return src.mountName || src["mount-name"] || src.uuid || hit._id;
    })
    .filter(Boolean);

  logger.info(
    {
      label: "p1UpdateMwdiReplica.mountNames",
      count: updatedMountNames.length
    },
    "Extracted mount names"
  );

  try {
    await redisQueue.ensureGroup(logger);

    logger.info(
      { label: "p1UpdateMwdiReplica.redis.enqueue.start", count: updatedMountNames.length },
      "Enqueuing mount names"
    );

    await redisQueue.enqueueMountNames(
      updatedMountNames,
      {
        batchSize: runtimeConfig?.redis?.enqueueBatchSize || 500,
        pauseMs: runtimeConfig?.redis?.enqueuePauseMs || 50
      },
      logger
    );

    logger.info(
      { label: "p1UpdateMwdiReplica.redis.enqueue.success" },
      "Redis enqueue completed"
    );

  } catch (error) {
    logger.error({ label: "redis-error", error: error.message || error });
    return 'unknown error occurred';
  }

  try {
    await withRetry(
      async () =>
        loggingClient.index({
          index: loggingEsClient["index-alias"],
          body: {
            jobName,
            periodStartTime,
            periodEndTime,
            replicated: reindexResp?.body?.created ?? 0,
            updated: reindexResp?.body?.updated ?? 0,
            total: reindexResp?.body?.total ?? 0,
            status: statusMessage,
            updatedMountNames,
            lastReplicaTime: new Date().toJSON(),
            timestamp: new Date().toJSON()
          },
          refresh: false
        }),
      {
        label: "p1UpdateMwdiReplica.logging",
        retryIntervalMs: 10000,
        logger
      }
    );
  } catch (error) {
    logger.error({ label: "logging-failure", error: error.message || error });
    return 'connection to Logging ES failed';
  }

  logger.info(
    {
      label: "p1UpdateMwdiReplica.completed",
      totalProcessed: reindexResp?.body?.total ?? 0,
      status: statusMessage
    },
    "Replica job completed"
  );

  return {
    "updated-mount-names": updatedMountNames,
    "timestamp": periodEndTime
  };
}

module.exports = { run };