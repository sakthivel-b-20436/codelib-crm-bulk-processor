const catalyst = require("zcatalyst-sdk-node");

module.exports = async (jobRequest, context) => {
  try {
    const app = catalyst.initialize(context);
    const module = jobRequest.getJobParam("MODULE");
    const fields_to_be_processed = jobRequest.getJobParam("FIELDS_TO_BE_PROCESSED");
	if (!module) {
      throw new Error("MODULE cannot be empty");
    }
    if (!fields_to_be_processed) {
      throw new Error("FIELDS_TO_BE_PROCESSED cannot be empty");
    }

    const table = app.datastore().table("BulkRead");
    await table.insertRow({
      MODULE_NAME: module,
      FIELDS_TO_BE_PROCESSED: fields_to_be_processed.replace(/\s/g, "") 
    });
    console.log("Inserted SucessFully");
  } catch (err) {
    console.log("Exception in Job Function ", err)
    return context.closeWithFailure();
  }
  return context.closeWithSuccess();
};
