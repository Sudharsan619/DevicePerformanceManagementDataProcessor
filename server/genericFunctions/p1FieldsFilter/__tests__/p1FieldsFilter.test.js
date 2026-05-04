/**
 * Unit tests for P1FieldsFilter.run
 * Fully isolated, professional-grade unit tests
 */

jest.mock("../../../utils/fieldsFilter", () => ({
  applyFieldsFilter: jest.fn()
}));

const { run } = require("../P1FieldsFilter");
const { applyFieldsFilter } = require("../../../utils/fieldsFilter");

describe("P1FieldsFilter.run", () => {

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("throws error when dataStructure is missing", async () => {
    await expect(run({}))
      .rejects
      .toThrow("dataStructure is mandatory");
  });

  test("filters dataStructure using provided fieldsFilterString", async () => {
    const request = {
      dataStructure: { a: 1, b: 2 },
      fieldsFilterString: "a"
    };

    applyFieldsFilter.mockReturnValue({ a: 1 });

    const response = await run(request);

    expect(applyFieldsFilter).toHaveBeenCalledTimes(1);
    expect(applyFieldsFilter).toHaveBeenCalledWith(
      { a: 1, b: 2 },
      "a"
    );

    expect(response).toEqual({
      filteredDataStructure: { a: 1 }
    });
  });

  test("uses empty string when fieldsFilterString is not provided", async () => {
    const request = {
      dataStructure: { a: 1 }
    };

    applyFieldsFilter.mockReturnValue({ a: 1 });

    const response = await run(request);

    expect(applyFieldsFilter).toHaveBeenCalledWith(
      { a: 1 },
      ""
    );

    expect(response).toEqual({
      filteredDataStructure: { a: 1 }
    });
  });

  test("handles empty fieldsFilterString explicitly", async () => {
    const request = {
      dataStructure: { a: 1 },
      fieldsFilterString: ""
    };

    applyFieldsFilter.mockReturnValue({ a: 1 });

    const response = await run(request);

    expect(applyFieldsFilter).toHaveBeenCalledWith(
      { a: 1 },
      ""
    );

    expect(response.filteredDataStructure).toEqual({ a: 1 });
  });

  test("handles empty dataStructure safely", async () => {
    const request = {
      dataStructure: {},
      fieldsFilterString: "a"
    };

    applyFieldsFilter.mockReturnValue({});

    const response = await run(request);

    expect(applyFieldsFilter).toHaveBeenCalledWith(
      {},
      "a"
    );

    expect(response).toEqual({
      filteredDataStructure: {}
    });
  });

});
