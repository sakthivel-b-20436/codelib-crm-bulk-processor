class RecordsProcessor {

  /**
 * Processes an array of Zoho CRM records.
 *
 * @param {Array<{
 *   module: string,
 *   data: Record<string, unknown>
 * }>} zcrmRecords - Array of Zoho CRM records
 *
 * @returns {Array<{
 *   module: string,
 *   data: Record<string, unknown>
 * }>} Processed Zoho CRM records.
 */

  processRecords(zcrmRecords) {
    return zcrmRecords;
  }
}
module.exports = RecordsProcessor;
