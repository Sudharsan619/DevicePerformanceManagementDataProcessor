const { getParamsByPurpose } = require("../../utils/functionTree");
const { readKafkaAddress } = require("../../utils/ltpResolution");
const onfAdapter = require("../../infra/onf/onfAdapter");

/**
 * Request:
 * {
 *   parameters: <function-tree>,
 *   config-file: <control-construct>
 * }
 *
 * Response:
 * {
 *   kafkaConnectionList: [
 *     {
 *       parameterName,
 *       kafkaClientUuid,
 *       clientId,
 *       brokerList,
 *       topicName,
 *       type
 *     }
 *   ]
 * }
 */
async function run(request) {
  const parameters = request.parameters;
  const configFile = request["config-file"];
  const logger = request.logger;

  if (!parameters) {
    throw new Error("Kafka session could not be established: missing 'parameters'");
  }

  if (!configFile) {
    throw new Error("Kafka session could not be established: missing 'config-file'");
  }

  const kafkaParams = getParamsByPurpose(
    parameters,
    "p1InitKafka",
    "kafkaClient"
  );

  const kafkaConnectionList = [];

  for (const param of kafkaParams) {
    const kafkaClient = await readKafkaAddress(configFile, param.value);

    if (!kafkaClient) {
      throw new Error(`Kafka session could not be established: no address found for '${param.value}'`);
    }

    await onfAdapter.connectKafkaProducer(
      kafkaClient.clientId,
      kafkaClient.brokerList,
      logger
    );

    kafkaConnectionList.push({
      parameterName: param["parameter-name"],
      kafkaClientUuid: param.value,
      clientId: kafkaClient.clientId,
      groupId: kafkaClient.groupId,
      auth: kafkaClient.auth,
      brokerList: kafkaClient.brokerList,
      topicName: kafkaClient.topicName,
      type: kafkaClient.type
    });
  }

  return { kafkaConnectionList };
}

module.exports = { run };
