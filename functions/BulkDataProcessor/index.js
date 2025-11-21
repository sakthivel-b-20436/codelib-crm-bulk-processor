const catalyst = require("zcatalyst-sdk-node");
const DownloadQueueProcessor = require("./processor/impl/DownloadQueueProcessor");
const ReadQueueProcessor = require("./processor/impl/ReadQueueProcessor");
const UploadQueueProcessor = require("./processor/impl/UploadQueueProcessor");
const Tables = require("./util/tables"); 

module.exports = async (event, context) => {
  try {
    const catalystApp = catalyst.initialize(context);


    // const rawData = event.getRawData();
    // const rowData = rawData.events[0].data;
    // const tableName = rowData.table_details.table_name;
    // const eventData = [rowData];

     const rawData = event.getData();
	const rData = event.getRawData(); // Remove this line if not needed
	console.log("Raw Data:", rData);
    const eventData = Array.isArray(rawData) ? rawData : [rawData];
    console.log("Event Data:", eventData);


    const tableId = event.getSourceEntityId();
	console.log("TABLE ID:", tableId);
    const datastore = catalystApp.datastore();
    const tableDetails = await datastore.getTableDetails(tableId);
	console.log("Table Details:", tableDetails);

    const tableName = tableDetails._tableDetails.table_name;

    console.log("Triggered Table:", tableName);


    if (tableName === Tables.BULK_READ.TABLE) {
      await new DownloadQueueProcessor().process(eventData, catalystApp);
    } else if (tableName === Tables.READ_QUEUE.TABLE) {
      await new ReadQueueProcessor().process(eventData, catalystApp);
    } else {
      await new UploadQueueProcessor().process(eventData, catalystApp);
    }
  } catch (err) {
    console.error("Exception in Job Function:", err);
    context.closeWithFailure();
  }
    context.closeWithSuccess();
};
