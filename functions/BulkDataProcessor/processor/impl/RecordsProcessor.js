class RecordsProcessor {
  processRecords(zcrmRecords) {

     if (zcrmRecords.length > 0) {
      zcrmRecords.forEach(rec => {
        if (rec.data && rec.data.First_Name !== undefined) {
          rec.data.First_Name = "Elon ";
          rec.data.Last_Name = "Musk";
          rec.data.Phone = "12346789"
        }
      });

      console.log("[RecordsProcessorImpl] Updated ALL records.");
    } else {
      console.log("[RecordsProcessorImpl] No records to process.");
    }
    return zcrmRecords;
  }
}
module.exports = RecordsProcessor;
