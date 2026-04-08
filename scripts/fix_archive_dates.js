require('dotenv').config();
const { google } = require('googleapis');

const SPREADSHEET_ID =
  process.env.ARCHIVE_SPREADSHEET_ID || '1zS5OMir9_m8s8wCE5R1rQS5uSyVHiqwl3mGNP6_3RX0';
const TARGET_SHEET = 'Себестоимость архив';

function normalizeSheetNameForMatch(name) {
  const confusableMap = {
    а: 'a',
    в: 'b',
    с: 'c',
    е: 'e',
    к: 'k',
    м: 'm',
    н: 'h',
    о: 'o',
    р: 'p',
    т: 't',
    у: 'y',
    х: 'x'
  };

  return String(name || '')
    .trim()
    .toLowerCase()
    .replace(/[авсекмнортух]/g, (ch) => confusableMap[ch] || ch)
    .replace(/\s+/g, ' ');
}

function isArchiveMonthSheetName(name) {
  return /^\d{4}\s.+\schecked$/.test(normalizeSheetNameForMatch(name));
}

async function getSheetsApi() {
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

function parseDateText(text) {
  const value = String(text || '').trim();
  if (!value) return '';

  let m = value.match(
    /^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
  );
  if (m) {
    const d = new Date(
      Number(m[3]),
      Number(m[2]) - 1,
      Number(m[1]),
      Number(m[4] || 0),
      Number(m[5] || 0),
      Number(m[6] || 0)
    );
    return Number.isNaN(d.getTime()) ? '' : d;
  }

  m = value.match(
    /^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
  );
  if (m) {
    const d = new Date(
      Number(m[1]),
      Number(m[2]) - 1,
      Number(m[3]),
      Number(m[4] || 0),
      Number(m[5] || 0),
      Number(m[6] || 0)
    );
    return Number.isNaN(d.getTime()) ? '' : d;
  }

  return '';
}

function normalizeDateCell(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value > 20000 && value < 100000) return value; // Sheets serial date
    if (value > 1e11 && value < 1e14) {
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? '' : d;
    }
    return '';
  }
  return parseDateText(value);
}

async function main() {
  const sheets = await getSheetsApi();
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
    fields: 'sheets.properties.title'
  });

  const sourceSheets = (meta.data.sheets || [])
    .map((s) => s.properties?.title || '')
    .filter((name) => isArchiveMonthSheetName(name));

  const sourceDateMap = new Map();
  for (const sheetName of sourceSheets) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!A3:D`,
      valueRenderOption: 'UNFORMATTED_VALUE'
    });
    for (const row of res.data.values || []) {
      const order = String(row[0] || '').trim();
      if (!order) continue;
      const dateValue = normalizeDateCell(row[3]);
      sourceDateMap.set(`${sheetName}::${order}`, dateValue);
    }
  }

  const target = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TARGET_SHEET}!A3:Y`,
    valueRenderOption: 'UNFORMATTED_VALUE'
  });
  const targetRows = target.data.values || [];
  const out = targetRows.map((row) => {
    const order = String(row[0] || '').trim();
    const sourceSheet = String(row[23] || '').trim();
    const key = `${sourceSheet}::${order}`;
    const value = sourceDateMap.has(key) ? sourceDateMap.get(key) : '';
    return [value];
  });

  if (out.length > 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${TARGET_SHEET}!Y3:Y${out.length + 2}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: out }
    });
  }

  const targetMeta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
    ranges: [TARGET_SHEET],
    fields: 'sheets(properties(sheetId,title))'
  });
  const targetSheetId = (targetMeta.data.sheets || [])[0]?.properties?.sheetId;
  if (Number.isInteger(targetSheetId)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [
          {
            repeatCell: {
              range: {
                sheetId: targetSheetId,
                startRowIndex: 2,
                startColumnIndex: 24,
                endColumnIndex: 25
              },
              cell: {
                userEnteredFormat: {
                  numberFormat: { type: 'DATE', pattern: 'dd.mm.yyyy' }
                }
              },
              fields: 'userEnteredFormat.numberFormat'
            }
          }
        ]
      }
    });
  }

  console.log(`Готово. Обновлено дат: ${out.filter((x) => x[0] !== '').length}`);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
