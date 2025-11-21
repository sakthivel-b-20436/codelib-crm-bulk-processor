const fs = require('fs');
const path = require('path');
const { pipeline } = require("stream/promises");
// const fastCsv = require('fast-csv');
// const { parse } = require('csv-parse/sync');
const { parse } = require('csv-parse'); 

const os = require('os');

const Tables = require('../../util/tables');
const CommonUtil = require('../../util/commonUtil');
const RecordsProcessor = require('../impl/RecordsProcessor');

const BATCH_SIZE = 5;
const INPUT_FILE = path.join(os.tmpdir(), 'input.csv');
const OUTPUT_FILE = path.join(os.tmpdir(), 'output.csv');

class ReadQueueProcessor {

  async closeWriter(outputFS) {
      if (outputFS && !outputFS.destroyed) {
        await new Promise((res) => outputFS.end(res));
      }
 }

  async createWriteQueueJob(filePath, bucket,zcql, module, objectPath) {
    const fileStream = fs.createReadStream(filePath); 
    const uniquePath = objectPath;
    await bucket.putObject(uniquePath, fileStream);
    const query = `INSERT INTO ${Tables.WRITE_QUEUE.TABLE} (${Tables.WRITE_QUEUE.FILE_ID}, ${Tables.WRITE_QUEUE.MODULE}) VALUES ('${uniquePath}', '${module}')`;
    await zcql.executeZCQLQuery(query);
    return uniquePath;
  }

  async persistReadQueueDetails(filePath, bucket,zcql, rowId, lineProcessed, module, objectPath) {
    const uploadedPath = await this.createWriteQueueJob(filePath, bucket,zcql, module, objectPath);
    const updateQuery = `UPDATE ${Tables.READ_QUEUE.TABLE} SET ${Tables.READ_QUEUE.LINE_PROCESSED}='${String(lineProcessed).padStart(0, '0')}' WHERE ${Tables.READ_QUEUE.ROWID}='${rowId}'`;
    await zcql.executeZCQLQuery(updateQuery);
    return uploadedPath;
  }

  async processBatch(processor, outputFS, headers, zcrmRecords) {
    const updatedRecords = await processor.processRecords(zcrmRecords);
    for (const zcrmRecord of updatedRecords) {
      console.log("ZCRM RECORDS::: ", zcrmRecord);
      const values = headers.map(h => {
        const v = (zcrmRecord.data && (zcrmRecord.data[h] !== undefined && zcrmRecord.data[h] !== null)) ? zcrmRecord.data[h] : ''; //
        return String(v);
      });
     outputFS.write(values.join(',') + '\n');
    }
  }

  async process(tableData, catalystApp) {
    try{
    const zcql = catalystApp.zcql();
    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket(CommonUtil.CSVFILES);
    let rowData = tableData[0];
    
    if (rowData.ReadQueue) rowData = rowData.ReadQueue;
    else if (rowData.READ_QUEUE) rowData = rowData.READ_QUEUE;
    else if (rowData.data && rowData.data[0]) rowData = rowData.data[0];

    const rowId = rowData.ROWID; 
    const query = `Select * from ReadQueue where IS_PROCESS_COMPLETED=false and ROWID='${rowId}'`;
    const rows = await zcql.executeZCQLQuery(query);

    const processor = new RecordsProcessor();

    for (const rowObj of rows) {
      const filepath = rowObj.ReadQueue.FILEID;
      const module = rowObj.ReadQueue.MODULE;

      let processedLine = Number(rowObj.ReadQueue.LINE_PROCESSED) || 0;

      const csvInputPath = INPUT_FILE;
      const csvOutputPath = OUTPUT_FILE;

      const objectPath = filepath;

      const objectStream = await bucket.getObject(objectPath);
      await pipeline(objectStream, fs.createWriteStream(csvInputPath));
       const inputFS = fs.createReadStream(csvInputPath);
          const parser = inputFS.pipe(parse({
              bom: true,
              skip_empty_lines: true,
              columns: false, 
       }));

      let totalRecords = 0;
      let bulkWriteChunkSize = 0;
      const zcrmRecords = [];
      let headers = null;
      let outputFS = null;
      let chunkIndex = 0; 
      
      const initializeChunkWriter = () => {
        if (!outputFS) {
          if (fs.existsSync(csvOutputPath)) {
            fs.unlinkSync(csvOutputPath);
          }
          outputFS = fs.createWriteStream(csvOutputPath, { flags: 'w' });
          outputFS.write(headers.join(',') + '\n');
        }
      };

       for await (const line of parser) {
        totalRecords++;
        const rec = { data: {}, moduleName: module };

        if (totalRecords === 1) {
          headers = line;
          continue;
        }

        if (totalRecords <= processedLine) {
          continue;
        }

        if (bulkWriteChunkSize === 0) {
          initializeChunkWriter(); 
        }

        for (let i = 0; i < line.length && totalRecords > 1; i++) {
          rec.data[headers[i]] = line[i] || '';
        }

        zcrmRecords.push(rec);
        bulkWriteChunkSize++;

        if (zcrmRecords.length === BATCH_SIZE) {
          await this.processBatch(processor, outputFS, headers, zcrmRecords);
          processedLine += zcrmRecords.length;
          zcrmRecords.length = 0;
        }

        if (bulkWriteChunkSize >= 10) {
          await this.closeWriter(outputFS);
          outputFS = null;
          chunkIndex++;
          const chunkKey = `output/${rowId}_chunk_${chunkIndex}.csv`;
          await this.persistReadQueueDetails(csvOutputPath, bucket,zcql, rowId, processedLine, module, chunkKey);
          bulkWriteChunkSize = 0;
        }
      }

      if (zcrmRecords.length > 0) {
        if (bulkWriteChunkSize === 0) {
            initializeChunkWriter();
        }
        await this.processBatch(processor, outputFS, headers, zcrmRecords);
        processedLine += zcrmRecords.length;
        bulkWriteChunkSize += zcrmRecords.length; 
        zcrmRecords.length = 0;
      }
      
      if (bulkWriteChunkSize > 0) {
        await this.closeWriter(outputFS);
        outputFS = null;
        chunkIndex++;
        const finalChunkKey = `output/${rowId}_chunk_${chunkIndex}.csv`;
        await this.persistReadQueueDetails(csvOutputPath, bucket,zcql, rowId, processedLine, module, finalChunkKey);
        bulkWriteChunkSize = 0;
      }
      await zcql.executeZCQLQuery(`update ReadQueue set IS_PROCESS_COMPLETED=true where ROWID='${rowId}'`);
      await this.closeWriter(outputFS);
      outputFS = null;
    } 
  } catch(err){
     console.log("Internal server error occurred. Please try again in some time.");
     throw new error(err);
}
} 
}
module.exports = ReadQueueProcessor;