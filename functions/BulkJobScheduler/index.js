const catalyst = require("zcatalyst-sdk-node");

module.exports = async (jobRequest, context) => {
  try {
    const app = catalyst.initialize(context);
    const MODULE = jobRequest.getJobParam("MODULE");
    const FIELDS_TO_BE_PROCESSED = jobRequest.getJobParam("FIELDS_TO_BE_PROCESSED");
	if (!MODULE || MODULE.toString().trim() === "") {
      throw new Error("MODULE cannot be empty");
    }
    if (!FIELDS_TO_BE_PROCESSED || FIELDS_TO_BE_PROCESSED.toString().trim() === "") {
      throw new Error("FIELDS_TO_BE_PROCESSED cannot be empty");
    }

    const table = app.datastore().table("BulkRead");
    await table.insertRow({
      MODULE_NAME: MODULE,
      FIELDS_TO_BE_PROCESSED: FIELDS_TO_BE_PROCESSED.replace(/\s/g, "") 
    });
    console.log("Inserted SucessFully");
  } catch (err) {
    console.log("Exception in Job Function ", err)
    return context.closeWithFailure();
  }
  return context.closeWithSuccess();
};
