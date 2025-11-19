'use strict'
const express = require('express')
const catalyst = require('zcatalyst-sdk-node')

const ErrorHandler = require('./utils/ErrorHandler')
const AuthService = require('./utils/AuthService')
const ZohoCRMUtil = require('./utils/ZohoCRMUtils')
const AppError = require('./utils/AppError')

const app = express()
app.use(express.json())

app.post('/job', async (req, res) => {
  try {
    const catalystApp = catalyst.initialize(req);
    if (!AuthService.getInstance().isValidRequest(req.query['catalyst-codelib-secret-key'])) {
      throw new AppError(
        401,
        "You don't have permission to perform this operation. Kindly contact your administrator for more details."
      );
    }

    const payload = req.body;
    ZohoCRMUtil.validatePayload(payload);

    if (payload.state === 'COMPLETED') {
      const page = payload.result?.page;
      const requestedPageNo = payload.result?.more_records ? page + 1 : page;
      const downloadURL = ZohoCRMUtil.buildDownloadURL(payload.result.download_url);
    
      if (payload.operation === 'read') {
        await catalystApp
          .zcql()
          .executeZCQLQuery(
            `UPDATE BulkRead SET DOWNLOAD_URL='${downloadURL}',REQUESTED_PAGE_NO='${requestedPageNo}' WHERE CRMJOBID='${payload.job_id}'`
          );
      }
    }
    res.status(200).send({
      status: 'success',
      message: 'Data processed successfully'
    });
  } catch (error) {
    const { statusCode, ...others } = ErrorHandler.getInstance().processError(error);
    res.status(statusCode).send(others);

  }
})

module.exports = app
