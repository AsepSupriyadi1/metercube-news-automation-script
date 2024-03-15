require('dotenv').config()
const {google} = require('googleapis');
const fs = require('fs');
const fsa = require('fs/promises');
const path = require('path');
const SERVICE_ACCOUNT_KEY_DIR = "key/news-automation-417116-2fa28228256b.json";

main().then(() => {
  console.log('Script completed')
})

async function main() {
  // Generating google sheet client
  const googleSheetClient = await _getGoogleSheetClient();

  // Reading Google Sheet from a specific range
  const data = await _readGoogleSheet(googleSheetClient, process.env.SHEET_ID, process.env.TAB_NAME, process.env.FULL_DATA_RANGE);

  // Convert sheets to object
  const convertedData = await convertToObject(data);

  // Filter (only row which has 'FALSE' value will be retrieved)
  const filterizedData = filterObjectsByLaunchStatus(convertedData);


  if(filterizedData.length == 0){
    console.log("There is no data which can be published ! \n")
    return false;
  }

  // Generate queries
  const resultQueries = generateInsertQueries(filterizedData);

  // Put queries to the file
  await generateFile(resultQueries);

  // Update all news status to TRUE
  await updateNewsStatus(googleSheetClient, process.env.SHEET_ID, process.env.TAB_NAME, filterizedData);
}

async function _getGoogleSheetClient() {
  console.log("Connecting Google sheet client.....")
  const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_KEY_DIR,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const authClient = await auth.getClient();

  console.log("Google sheet client connected sucessfully.....\n")
  return google.sheets({
    version: 'v4',
    auth: authClient,
  });
}

async function _readGoogleSheet(googleSheetClient, sheetId, tabName, range) {
  const res = await googleSheetClient.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: `${tabName}!${range}`,
  });

  return res.data.values;
}

async function convertToObject(dataArray){
  console.log("Converting all data to objects.....")
  const columnNames = dataArray[0];
  const resultArray = [];

  for (let i = 1; i < dataArray.length; i++) {
    const obj = {};
    for (let j = 0; j < columnNames.length; j++) {
      const key = columnNames[j] === 'Key points' ? 'key_points' : columnNames[j].toLowerCase();
      obj[key] = dataArray[i][j];
    }
    resultArray.push(obj);
  }
  console.log("Data converted sucessfully.....\n")

  return resultArray;
}


function filterObjectsByLaunchStatus(dataArray) {
  return dataArray.filter(obj => obj.metercube_launch_status === 'FALSE');
}

function generateInsertQueries(dataArray) {
  console.log("Generating Queries....")
  const tableName = 'metercube_news';
  const columns = Object.keys(dataArray[0]).filter(key => key !== 'metercube_launch_status').join(', ');

  let procedures = '';
  for (let i = 0; i < dataArray.length; i++) {
      const obj = dataArray[i];
      const { title, date, tags, type, key_points, links } = obj;
      const formattedTitle = title.replace(/'/g, '');

      const dateObject = new Date(date); 
      const dateISO = dateObject.toISOString();

      const tagsInput =tags.replace(/'/g, '');
      let tagArr = tagsInput.split(',').map(item => item.trim());
      let formattedTags = "ARRAY['" + tagArr.join("', '") + "']";

      const formattedKeyPoints = key_points.replace(/'/g, '');
    
      console.log(formattedTags)

      const valueFormat = `
        SELECT * FROM fn_insert_news(
          '${formattedTitle}',
          null,
          ARRAY['Indonesia'],
          '${formattedKeyPoints}',
          'null',
          ${formattedTags},
          null,
          '${links}',
          'Draft',
          '${dateISO}'
        );
      `
      procedures += ` ${valueFormat}\n`;
  }
  
  const query = `
  -- Start Transaction
  BEGIN; \n
  ${procedures} \n
  COMMIT;
  -- END Transaction
  `

  console.log("Queries generated sucessfully.....\n")
  

  return query;
}



async function updateNewsStatus(googleSheetClient, spreadsheetId,  tabName, filterizedData){
  console.log("Updating all rows.....")
  let affectedRows = 0;

  // Change status to FALSE
  filterizedData.forEach(row => {
    row['metercube_launch_status'] = "TRUE";
    affectedRows++;
  });

  // Convert into multidimensional array
  const result = filterizedData.map(obj => Object.values(obj));

  for(let i = 0; i < result.length; i++){
    if(result[i][0] == filterizedData[i].no){
      console.log("Updating row no-" + filterizedData[i].no + "...");
      // UPDATE ROWS BASED ON NUMBER
      await googleSheetClient.spreadsheets.values.update({
        spreadsheetId,
        range: `${tabName}!A${parseInt(filterizedData[i].no) + 1}`,
        valueInputOption: 'RAW',
        resource: {
          values: [result[i]]
        }
      })
    }
  }

  console.log("All rows status updated successfully.....")
  console.log(`${affectedRows} rows affected \n`)
}


async function generateFile(fileContent){
  console.log("Generating the files.....")

  const currentDate = new Date();
  const day = currentDate.getDate();
  const monthNames = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER'];
  const month = monthNames[currentDate.getMonth()];
  const year = currentDate.getFullYear();
  const hours = String(currentDate.getHours()).padStart(2, '0');
  const minutes = String(currentDate.getMinutes()).padStart(2, '0');

  const fileName = `QUERY_${day}-${month}-${year}_${hours}-${minutes}.sql`;

  const folderPath = './script';
  const filePath = path.join(folderPath, fileName);

  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath);
  }

  // Stored query into the file
  try {
    await fsa.writeFile(filePath, fileContent, 'utf-8');
  } catch(err){
    console.error('Failed creating the file', err);
  }

  console.log(`File successfully created: ${filePath} \n`)
}

