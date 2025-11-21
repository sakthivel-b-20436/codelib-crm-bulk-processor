const fs = require("fs");
const path = require("path");
const axios = require("axios");
const AdmZip = require("adm-zip"); 


const Tables = require("../../util/tables");
const CommonUtil = require("../../util/commonUtil"); 
const { pipeline } = require("stream/promises");
const { error } = require("console");

class DownloadQueueProcessor {


  async process(tableData, catalystApp) {
    try{

      for (const row of tableData) {
      const rowData = row[Tables.BULK_READ.TABLE] || row;
      const requestedPageNo = rowData[Tables.BULK_READ.REQUESTED_PAGE_NO];
      const moduleName = rowData[Tables.BULK_READ.MODULE_NAME];
      const fetchedPageNo = rowData[Tables.BULK_READ.FETCHED_PAGE_NO];
      const downloadURL = rowData[Tables.BULK_READ.DOWNLOAD_URL] || null;
      const crmJobId = rowData[Tables.BULK_READ.CRMJOBID] || null;
      const fieldsToBeProcessed = rowData[Tables.BULK_READ.FIELDS_TO_BE_PROCESSED]?.split(",");

      const accessToken = await CommonUtil.getCRMAccessToken(catalystApp);

      if (crmJobId && downloadURL && downloadURL.trim() !== "") {
         const zipPath = path.join("/tmp", `${crmJobId}.zip`);
         const csvPath = path.join("/tmp", `${crmJobId}.csv`);

          const response = await axios.get(downloadURL, {
            headers: { Authorization: accessToken },
            responseType: "stream"                 
          });
       
          const writer = fs.createWriteStream(zipPath);
          await pipeline(response.data, writer);
          console.log(`[process] Successfully streamed ZIP file to: ${zipPath}`);

          const zip = new AdmZip(zipPath);
          const zipEntries = zip.getEntries();
          
          const csvEntry = zipEntries.find(entry => entry.entryName.endsWith(".csv"));
          
          if (csvEntry) {
              // Extract the CSV file content to the temporary csvPath
              const csvData = zip.getEntry(csvEntry.entryName).getData();
              fs.writeFileSync(csvPath, csvData);
              console.log(`[process] Successfully extracted CSV file to: ${csvPath}`);
          } else {
              throw new Error("CSV file not found in ZIP archive.");
          }

        // await fs.createReadStream(zipPath).pipe(unzipper.Parse()).on("entry", (entry) => {
        //       if (entry.path.endsWith(".csv")) {
        //         entry.pipe(fs.createWriteStream(csvPath));
        //       } else {
        //         entry.autodrain();
        //       }
        //     })
        //     .promise();

        const stratus = catalystApp.stratus();
        const bucket = stratus.bucket(CommonUtil.CSVFILES);
        const fileStream = fs.createReadStream(csvPath);

        // const fileBuffer = fs.readFileSync(csvPath);
        const uniquePath = `Files/${crmJobId}.csv`;
        await bucket.putObject(uniquePath, fileStream);

        const datastore = catalystApp.datastore();
        const readQueueTable = datastore.table(Tables.READ_QUEUE.TABLE);
        await readQueueTable.insertRow({
          [Tables.READ_QUEUE.FILEID]: uniquePath,
          [Tables.READ_QUEUE.CRM_JOB_ID]: crmJobId,
          [Tables.READ_QUEUE.MODULE]: moduleName
        });
      }

      if (fetchedPageNo < requestedPageNo) {
        const callback = {
          url: CommonUtil.getCallBackURL(catalystApp),
          method: "post"
        };
        const module = { api_name: moduleName };
        const query = {
          module,
          page: requestedPageNo,
          fields: fieldsToBeProcessed
        };
        const input = {
          callback,
          query,
          file_type: "csv"
        };

        const bulkResponse = await axios.post(CommonUtil.CRM_BULK_READ_URL, input, {
          headers: {
            Authorization: accessToken,
            "Content-Type": "application/json"
          }
        });

        const jobId = bulkResponse.data.data[0].details.id;

        let zcql = catalystApp.zcql();
        await zcql.executeZCQLQuery("UPDATE BulkRead SET CRMJOBID='" + jobId + "',DOWNLOAD_URL='',FETCHED_PAGE_NO='"
                  + requestedPageNo + "'  where ROWID='" + rowData.ROWID + "'");
      }
    } 
  } catch(err)
   {
    console.log("Internal server error occurred. Please try again in some time.");
    throw new error(e);
   }
} 
}
module.exports = DownloadQueueProcessor;
