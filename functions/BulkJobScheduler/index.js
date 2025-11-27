/**
 * 
 * @param {import("./types/job").JobRequest} jobRequest 
 * @param {import("./types/job").Context} context 
 */

const catalyst = require("zcatalyst-sdk-node");
module.exports = async (jobRequest, context) => {
  try {
    const app = catalyst.initialize(context);
    const module = jobRequest.getJobParam("MODULE");
    const fieldsToBeProcessed = jobRequest.getJobParam("FIELDS_TO_BE_PROCESSED");
	if (!module) {
      throw new Error("MODULE cannot be empty");
    }
    if (!fieldsToBeProcessed) {
      throw new Error("FIELDS_TO_BE_PROCESSED cannot be empty");
    }

    const table = app.datastore().table("BulkRead");
    await table.insertRow({
      MODULE_NAME: module,
      FIELDS_TO_BE_PROCESSED: fieldsToBeProcessed.replace(/\s/g, "") 
    });
    console.log("Inserted SucessFully");
  } catch (err) {
    console.log("Exception in Job Function ", err)
    return context.closeWithFailure();
  }
  return context.closeWithSuccess();
};
