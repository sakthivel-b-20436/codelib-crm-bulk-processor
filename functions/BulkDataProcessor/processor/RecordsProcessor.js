/**
   * Processes an array of Zoho CRM records.
   * @param {Array<Object>} zcrmRecords - An array of records retrieved from Zoho CRM.
   * Each object in the array is expected to have a `data` property.
   * Example structure: `[{ data: { First_Name: '...', Last_Name: '...', Phone: '...' } }, ...]
   * @returns {Array<Object>} - The array of Zoho CRM records after processing and modification.
*/

class RecordsProcessor {
  processRecords(zcrmRecords) {
    return zcrmRecords;
  }
}
module.exports = RecordsProcessor;
