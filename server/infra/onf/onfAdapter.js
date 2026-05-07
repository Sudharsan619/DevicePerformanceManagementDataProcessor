const { loadConfigFile } = require("../../utils/config");
const { Client } = require("@elastic/elasticsearch");
const { withRetry } = require("../../utils/retry");

// Helper to safely require optional ONF modules
function tryRequire(name) {
  try {
    return require(name);
  } catch {
    return undefined;
  }
}

// Local fallback mode (ONF disabled)
const OnfAttributes = undefined;

const esClients = new Map();

// Load configuration
function getConfig(configFile) {
  return configFile || loadConfigFile();
}

// Extract control construct
function getControlConstruct(configFile) {
  const cfg = getConfig(configFile);
  return cfg["core-model-1-4:control-construct"] || cfg;
}

// Fallback ONF attributes
function getFallbackOnfAttributes() {
  return {
    GLOBAL_CLASS: { UUID: "uuid" },

    LOGICAL_TERMINATION_POINT: {
      SERVER_LTP: "server-ltp",
      LAYER_PROTOCOL: "layer-protocol"
    },

    LAYER_PROTOCOL: {
      ES_CLIENT_INTERFACE_PAC:
        "elasticsearch-client-interface-1-0:elasticsearch-client-interface-pac",
      KAFKA_CLIENT_INTERFACE_PAC:
        "kafka-client-interface-1-0:kafka-client-interface-pac"
    },

    ES_CLIENT: {
      CONFIGURATION: "elasticsearch-client-interface-configuration",
      INDEX_ALIAS: "index-alias",
      AUTH: "auth",
      API_KEY: "api-key"
    },

    KAFKA_CLIENT: {
      CONFIGURATION: "kafka-client-interface-configuration",
      CLIENT_ID: "client-id",
      GROUP_ID: "group-id",
      TOPIC_NAME: "topic-name"
    },

    TCP_CLIENT: {
      CONFIGURATION: "tcp-client-interface-configuration",
      REMOTE_ADDRESS: "remote-address",
      REMOTE_PORT: "remote-port",
      REMOTE_PROTOCOL: "remote-protocol",
      IP_ADDRESS: "ip-address",
      IPV_4_ADDRESS: "ipv-4-address",
      DOMAIN_NAME: "domain-name"
    }
  };
}

function getOnfAttributes() {
  return OnfAttributes || getFallbackOnfAttributes();
}

// Read config
async function readControlConstruct(configFile) {
  return getConfig(configFile);
}

// LTP list
async function getLogicalTerminationPointListAsync(layerProtocolName, configFile) {
  const ltpList = getControlConstruct(configFile)["logical-termination-point"] || [];

  if (!layerProtocolName) return ltpList;

  return ltpList.filter((ltp) => {
    const lp = (ltp["layer-protocol"] || [])[0] || {};
    return lp["layer-protocol-name"] === layerProtocolName;
  });
}

// LTP by UUID
async function getLogicalTerminationPointAsync(uuid, configFile) {
  const ltpList = getControlConstruct(configFile)["logical-termination-point"] || [];
  return ltpList.find((ltp) => ltp.uuid === uuid) || null;
}

// Convert remote address
function remoteAddressToHost(remoteAddress) {
  const attrs = getOnfAttributes();

  if (remoteAddress?.[attrs.TCP_CLIENT.IP_ADDRESS]) {
    return remoteAddress[attrs.TCP_CLIENT.IP_ADDRESS][attrs.TCP_CLIENT.IPV_4_ADDRESS];
  }

  if (remoteAddress?.[attrs.TCP_CLIENT.DOMAIN_NAME]) {
    return remoteAddress[attrs.TCP_CLIENT.DOMAIN_NAME];
  }

  return "127.0.0.1";
}

// Mock ONF network calls
async function getRemoteAddressAsync() {
  return {
    "ip-address": {
      "ipv-4-address": "127.0.0.1"
    }
  };
}

async function getRemotePortAsync() {
  return 9200;
}

async function getRemoteProtocolAsync() {
  return "HTTP";
}

// Elasticsearch client
async function getEsClient(forceCreate, uuid, esAddress, logger) {
  const key = esAddress?.node || uuid || "default";

  if (!forceCreate && esClients.has(key)) {
    return esClients.get(key);
  }

  return withRetry(
    async () => {
      const client = new Client({
        node: esAddress?.node || "http://localhost:9200",
        requestTimeout: 60000
      });

      await client.info();
      esClients.set(key, client);

      return client;
    },
    {
      label: `getEsClient:${key}`,
      retryIntervalMs: 5000,
      logger
    }
  );
}

// ✅ Kafka is MOCKED (important)
async function connectKafkaProducer(clientId, brokers, logger) {
  logger.info({ clientId, brokers }, "Kafka producer skipped (mock mode)");
  return { mock: true };
}

async function sendKafkaMessage(topic, message, clientId, brokers, logger) {
  logger.info({ topic, message }, "Kafka message skipped (mock mode)");
  return { mock: true };
}

module.exports = {
  readControlConstruct,
  getLogicalTerminationPointListAsync,
  getLogicalTerminationPointAsync,
  getOnfAttributes,
  remoteAddressToHost,
  getEsClient,
  connectKafkaProducer,
  sendKafkaMessage,
  getRemoteAddressAsync,
  getRemotePortAsync,
  getRemoteProtocolAsync
};