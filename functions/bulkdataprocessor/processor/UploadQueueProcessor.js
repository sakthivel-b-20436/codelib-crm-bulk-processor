const fs = require('fs');
const path = require('path');
const axios = require('axios');
const FormData = require('form-data');
const archiver = require('archiver');
const { parse } = require('csv-parse/.'); 
const { pipeline } = require('stream/promises');

const Tables = require('../util/tables');
const CommonUtil = require('../util/commonUtil'); 

class UploadQueueProcessor {
  async process(tableData, catalystApp) {
      for (let i = 0; i < tableData.length; i++) {
      let rowData = tableData[i];
      if (rowData && rowData[Tables.WRITE_QUEUE.TABLE]) {
        rowData = rowData[Tables.WRITE_QUEUE.TABLE];
      }
      const moduleName = rowData[Tables.WRITE_QUEUE.MODULE];
      const meta = await CommonUtil.getFields(moduleName, catalystApp);
      const isUploaded = rowData.IS_UPLOADED === true || String(rowData.IS_UPLOADED).toLowerCase() === 'true';
      if (isUploaded) {
        continue;
      }
      const stratus = catalystApp.stratus();
      const bucket = stratus.bucket(CommonUtil.CSVFILES);
      const fileId = rowData[Tables.WRITE_QUEUE.FILE_ID];
      const objectStream = await bucket.getObject(fileId);
      const tmpCsvPath = path.join('/tmp', 'data.csv');
      const out = fs.createWriteStream(tmpCsvPath);
      await pipeline(objectStream, out);

      const firstLine = await this._readFirstLineFromCsv(tmpCsvPath);

      const zipPath = path.join('/tmp', 'out.zip');
      await this._zipFile(tmpCsvPath, zipPath, 'data.csv');
     
      const accessToken = await CommonUtil.getCRMAccessToken(catalystApp);

      let zgid = '';
        const orgResp = await axios.get(CommonUtil.CRM_ORG_GET_URL, {
          headers: { Authorization: accessToken }
        });
        if (!orgResp || !orgResp) {
        throw new Error("CRM Org returned empty response");
        }
        zgid = orgResp.data.org[0].zgid;

      const form = new FormData();
      form.append('file', fs.createReadStream(zipPath), { filename: path.basename(zipPath), contentType: 'application/zip' });
      let uploadResp;
     
        uploadResp = await axios.post(CommonUtil.CRM_UPLOAD_URL, form, {
          headers: {
            ...form.getHeaders(),
            Authorization: accessToken,
            feature: 'bulk-write',
            'X-CRM-ORG': zgid
          },
        });

      if (!uploadResp || !uploadResp.data) {
        throw new Error('CRM upload returned empty response');
      }
      const bulkUploadDetails = uploadResp.data;    
      const crmFileId = bulkUploadDetails.details.file_id || null;

      const bulkWriteInput = {
        operation: 'upsert',
        ignore_empty: true,
        resource: []
      };

      const resourceObj = {
        type: 'data',
        module: { api_name: moduleName },
        file_id: crmFileId,
        find_by: 'id'
      };

      const fieldMappings = [];
      for (let j = 0; j < firstLine.length; j++) {
        const csvHeader = firstLine[j];
        let apiName = csvHeader;
        if (apiName && apiName.toLowerCase() === 'id') {
          apiName = apiName.toLowerCase();  
        }
        const dataType = meta && meta.fieldDataTypeMap ? meta.fieldDataTypeMap[csvHeader] : null;
        const readOnlyFields = meta && meta.readOnlyFields ? meta.readOnlyFields : [];
        const disabledTypes = ['profileimage', 'formula', 'autonumber', 'fileupload', 'imageupload'];
        const isReadOnlyField = readOnlyFields.includes(csvHeader);
        const isDisabledType = dataType ? disabledTypes.includes(String(dataType).toLowerCase()) : false;
       
        if (apiName === 'id' || (dataType && !isReadOnlyField && !isDisabledType)) {
          const fieldMapJSON = {
            api_name: apiName,
            index: j
          };

          if (dataType && String(dataType).toLowerCase().includes('lookup')) {
            fieldMapJSON.find_by = 'id';
          }
          fieldMappings.push(fieldMapJSON);
        }
      }

      if (fieldMappings.length > 1) {
        resourceObj.field_mappings = fieldMappings;
      }
      bulkWriteInput.resource.push(resourceObj);
      let bulkWriteResp;
     
         bulkWriteResp = await axios.post(
            CommonUtil.CRM_BULK_WRITE_URL,
            bulkWriteInput,  
            {
              headers: {
                Authorization: accessToken,
                'Content-Type': 'application/json'
              }
            }
          );

      if (!bulkWriteResp || !bulkWriteResp.data) {
        throw new Error('CRM bulk write returned empty response');
      }

      const bulkWriteBody = bulkWriteResp.data;
      const jobId = bulkWriteBody.details.id || null;

      const zcql = catalystApp.zcql();
      const rowRowId = rowData['ROWID'] || rowData.ROWID || rowData.rowid;
      const updateQuery = `UPDATE ${Tables.WRITE_QUEUE.TABLE} SET ${Tables.WRITE_QUEUE.CRM_JOB_ID}='${jobId}', IS_UPLOADED=true WHERE ROWID='${rowRowId}'`;
      await zcql.executeZCQLQuery(updateQuery);
    }
} 

  async _readFirstLineFromCsv(filePath) {
    let inputFS;
    try{
    inputFS = fs.createReadStream(filePath);
    const parser = inputFS.pipe(parse({
        bom: true, 
        skip_empty_lines: true,
        columns: false, 
    }));
        for await (const record of parser) {
            return record; 
        }
        return [];
    } catch (e) {
        console.error('Error parsing first line of CSV:', e.message);
        throw e;
    } finally {
        inputFS.destroy(); 
    }
  }

  async _zipFile(inputPath, outputZipPath, entryName = 'data.csv') {
    return new Promise((resolve, reject) => {
      const output = fs.createWriteStream(outputZipPath);
      const archive = archiver('zip', { zlib: { level: 9 } }); 
      output.on('close', () => resolve());
      archive.on('error', err => reject(err));
      archive.pipe(output);
      archive.file(inputPath, { name: entryName });
      archive.finalize();
    });
  }
}
module.exports = UploadQueueProcessor;
