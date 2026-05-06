const { run } = require("./P1CreateResultCc");

describe("p1CreateResultCc.run", () => {

  let logger;

  beforeEach(() => {
    logger = {
      error: jest.fn(),
      info: jest.fn()
    };

    jest.clearAllMocks();
  });

  /* =========================
   * VALIDATION TESTS
   * ========================= */

  test("throws error when parameters is missing", async () => {
    await expect(
      run({ rawCc: {}, logger })
    ).rejects.toThrow("parameters and rawCc are mandatory");

    expect(logger.error).toHaveBeenCalled();
  });

  test("throws error when rawCc is missing", async () => {
    await expect(
      run({ parameters: {}, logger })
    ).rejects.toThrow("parameters and rawCc are mandatory");

    expect(logger.error).toHaveBeenCalled();
  });

  /* =========================
   * HAPPY PATH
   * ========================= */

  test("generates interfaceMetadataList correctly", async () => {
    const rawCc = {
      "logical-termination-point": [
        {
          uuid: "ltp-1",
          "layer-protocol": [
            {
              "air-interface-2-0:air-interface-pac": {
                "air-interface-historical-performances": {
                  "historical-performance-data-list": [
                    {
                      periodEndTime: "2025-01-01T00:00:00Z",
                      granularity: "15-MINUTES"
                    },
                    {
                      periodEndTime: "2025-01-02T00:00:00Z",
                      granularity: "24-HOURS"
                    }
                  ]
                }
              }
            }
          ]
        }
      ]
    };

    const result = await run({
      parameters: {},
      rawCc,
      mountName: "device-1",
      logger
    });

    expect(result.interfaceMetadataList).toEqual([
      {
        uuid: "ltp-1",
        mostRecentPeriodEndTime: "2025-01-02T00:00:00Z",
        mostRecentPeriodEndTime24: "2025-01-02T00:00:00Z"
      }
    ]);

    expect(result.resultCc["interface-metadata-list"]).toBeDefined();
    expect(result.mountName).toBe("device-1");
  });

  /* =========================
   * MULTIPLE LTPs
   * ========================= */

  test("handles multiple LTPs", async () => {
    const rawCc = {
      "logical-termination-point": [
        { uuid: "ltp-1", "layer-protocol": [] },
        { uuid: "ltp-2", "layer-protocol": [] }
      ]
    };

    const result = await run({
      parameters: {},
      rawCc,
      logger
    });

    expect(result.interfaceMetadataList).toEqual([
      { uuid: "ltp-1", mostRecentPeriodEndTime: null, mostRecentPeriodEndTime24: null },
      { uuid: "ltp-2", mostRecentPeriodEndTime: null, mostRecentPeriodEndTime24: null }
    ]);
  });

  /* =========================
   * EMPTY INPUT
   * ========================= */

  test("handles empty logical-termination-point", async () => {
    const rawCc = {};

    const result = await run({
      parameters: {},
      rawCc,
      logger
    });

    expect(result.interfaceMetadataList).toEqual([]);
  });

  /* =========================
   * INPUT IMMUTABILITY
   * ========================= */

  test("does not mutate original rawCc", async () => {
    const rawCc = {
      "logical-termination-point": []
    };

    const originalCopy = JSON.parse(JSON.stringify(rawCc));

    await run({
      parameters: {},
      rawCc,
      logger
    });

    expect(rawCc).toEqual(originalCopy);
  });

  /* =========================
   * ERROR HANDLING
   * ========================= */

  test("logs error and rethrows", async () => {
    await expect(
      run({
        parameters: null,
        rawCc: null,
        logger
      })
    ).rejects.toThrow();

    expect(logger.error).toHaveBeenCalled();
  });

});