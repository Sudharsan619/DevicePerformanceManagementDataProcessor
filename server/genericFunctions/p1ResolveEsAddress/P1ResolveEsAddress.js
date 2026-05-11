const { readEsAddress } = require("../../utils/ltpResolution");

/**
 * P1ResolveEsAddress
 * Composes the address information of an ElasticSearchClient
 * 
 * Request:
 * {
 *   parameters: <object>,
 *   "config-file": <object>,
 *   "es-name": "mwdiEsClient"
 * }
 * 
 * Response:
 * {
 *   esAddress: {
 *     uuid,
 *     node,
 *     "index-alias",
 *     "api-key"
 *   }
 * }
 */
async function run(request) {
  const parameters = request["parameters"];
  const configFile = request["config-file"];
  const esName = request["es-name"];
 
  if (!parameters || !configFile || !esName) {
    throw new Error("parameters, configFile and esName are mandatory");
  }
 
  const parameterEntry = (parameters.parameter || []).find(
    (item) => item["parameter-name"] === esName
  );
 
  if (!parameterEntry) {
    throw new Error("ES parameter not found for " + esName);
  }
 
  const esAddress = await readEsAddress(configFile, parameterEntry.value);
 
  return { esAddress };
}
 
module.exports = { run };
 