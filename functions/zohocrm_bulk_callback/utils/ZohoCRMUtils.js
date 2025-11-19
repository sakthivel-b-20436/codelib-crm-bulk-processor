const validator = require('validator')

const AppError = require('./AppError')

class ZohoCRMUtil {
  static #NUMERIC_REGEX = /^\d+$/

  static buildDownloadURL = (path = '') => {
    return 'https://zohoapis.com/' + path.slice(1)
  }

  static validatePayload = (data = {}) => {
    if (!data.job_id) {
      throw new AppError(400, 'job_id cannot be empty.')
    }

    if (!this.#NUMERIC_REGEX.test(data.job_id)) {
      throw new AppError(
        400,
        'Invalid value for job_id. job_id should be a positive number.'
      )
    }
    if (!data.result) {
      throw new AppError(400, 'result cannot be empty.')
    }
    if (!data.result.download_url) {
      throw new AppError(400, 'download_url cannot be empty.')
    }
    if (!validator.isURL(this.buildDownloadURL(data.result.download_url))) {
      throw new AppError(400, 'Invalid value for download_url.')
    }

    if (!data.result.page) {
      throw new AppError(400, 'page cannot be empty.')
    }

    if (!this.#NUMERIC_REGEX.test(data.result.page)) {
      throw new AppError(
        400,
        'Invalid value for page. page should be a positive number.'
      )
    }
  }
}
module.exports = ZohoCRMUtil
