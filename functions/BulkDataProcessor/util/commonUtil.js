const axios = require("axios");
const FieldMeta = require("../models/FieldMeta");

class CommonUtil {

  static CRM_UPLOAD_URL = "https://content.zohoapis.com/crm/v8/upload";
  static CRM_BULK_READ_URL = "https://zohoapis.com/crm/bulk/v8/read";
  static CRM_ORG_GET_URL = "https://zohoapis.com/crm/v8/org";
  static CRM_BULK_WRITE_URL = "https://zohoapis.com/crm/bulk/v8/write";
  static CRM_FIELD_API = "https://zohoapis.com/crm/v8/settings/fields";
  static BULK_READ = "BulkRead";
  static CSVFILES = process.env.BUCKET_NAME;


  static async getCRMAccessToken(catalystApp) {
    try {     
      const authJson = {
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
        refresh_token: process.env.REFRESH_TOKEN,
        auth_url: "https://accounts.zoho.com/oauth/v2/token",
        refresh_url: "https://accounts.zoho.com/oauth/v2/token"
      };
      const connectorJson = {
        CRMConnector: authJson
      };
      const connectionInstance = catalystApp.connection(connectorJson);
      const crmConnector = connectionInstance.getConnector("CRMConnector");
      const token = await crmConnector.getAccessToken();
      const accessToken = `Zoho-oauthtoken ${token}`;
      return accessToken; 
    } catch (err) {
      console.error("Error fetching access token:", err);
      throw err;
    }
  }
 
  static getCallBackURL(catalystApp) {
    const projectdomain = catalystApp.config.projectDomain;
    const secretKey = process.env.CODELIB_SECRET_KEY;
    const callbackURL = `${projectdomain}/server/zohocrm_bulk_callback/job?catalyst-codelib-secret-key=${secretKey}`;
    return callbackURL;
  }

  static async getFields(module, catalystApp) {
    const meta = new FieldMeta();
    try {
      const accessToken = await this.getCRMAccessToken(catalystApp);
      const url = `${this.CRM_FIELD_API}?module=${module}&type=all`;
      const response = await axios.get(url, {
        headers: { Authorization: accessToken },
      });
      const data = response.data;
      const fields = data.fields || data.data?.fields || [];
     
      for (const field of fields) {
        const apiName = field.api_name;
        const dataType = field.data_type;

        const isUpdatable =
          field.operation_type && typeof field.operation_type.api_update === "boolean"
            ? field.operation_type.api_update
            : false;

        const isExcluded =
          apiName.includes("Tag") ||
          ["profileimage", "formula", "autonumber", "fileupload", "imageupload"].includes(
            dataType.toLowerCase()
          );

         if (isExcluded) {
            meta.addField(apiName, dataType, true);
          } else {
            meta.addField(apiName, dataType, !isUpdatable); 
          }
      }
      return meta;
    } catch (error) {
      console.error("Error occurred in Get fields:", error.message);
      throw error;
    }
  }
}
module.exports = CommonUtil;