
import fs from 'fs';
import path from 'path';
import * as XLSX from 'xlsx';
import readline from 'readline';
import { SearchIndex } from '../app/lib/types';

const DATA_DIR = path.join(process.cwd(), 'data');
const INDEX_FILE = path.join(DATA_DIR, 'search-index.json');

async function buildIndex() {
    console.log('Starting index build...');
    const startTime = Date.now();

    const newIndex: SearchIndex[] = [];

    try {
        if (!fs.existsSync(DATA_DIR)) {
            console.warn('Data directory missing!');
            return;
        }

        const files = fs.readdirSync(DATA_DIR);

        for (const file of files) {
            if (!file.match(/\.(csv|xlsx|xls|json)$/)) continue;
            // Skip the index file itself if it exists
            if (file === 'search-index.json') continue;

            console.log(`Processing ${file}...`);
            const filePath = path.join(DATA_DIR, file);

            if (file.endsWith('.csv')) {
                const fileStream = fs.createReadStream(filePath);
                const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

                let isHeader = true;
                let nameIdx = -1, cinIdx = -1, stateIdx = -1, statusIdx = -1;
                let index = 0;

                for await (const line of rl) {
                    // Simple CSV parser: handles quoted fields roughly
                    const values: string[] = [];
                    let inQuote = false;
                    let currentVal = '';

                    for (let i = 0; i < line.length; i++) {
                        const char = line[i];
                        if (char === '"') {
                            inQuote = !inQuote;
                        } else if (char === ',' && !inQuote) {
                            values.push(currentVal.trim());
                            currentVal = '';
                        } else {
                            currentVal += char;
                        }
                    }
                    values.push(currentVal.trim());

                    // Clean quotes from values
                    const cleanValues = values.map(v => v.replace(/^"|"$/g, '').trim());

                    if (isHeader) {
                        nameIdx = cleanValues.findIndex(h => h.match(/Company.*Name|Name/i));
                        cinIdx = cleanValues.findIndex(h => h.match(/CIN/i));
                        stateIdx = cleanValues.findIndex(h => h.match(/State|CompanyStateCode/i));
                        statusIdx = cleanValues.findIndex(h => h.match(/Status|CompanyStatus/i));
                        isHeader = false;
                        continue;
                    }

                    // Extract fields
                    const name = (nameIdx >= 0 ? cleanValues[nameIdx] : '') || 'Unknown';
                    const cin = (cinIdx >= 0 ? cleanValues[cinIdx] : '') || '';
                    const state = (stateIdx >= 0 ? cleanValues[stateIdx] : '') || '';
                    const status = (statusIdx >= 0 ? cleanValues[statusIdx] : '') || '';

                    if (name !== 'Unknown') {
                        newIndex.push({
                            n: name.toLowerCase(),
                            c: cin,
                            s: state,
                            st: status,
                            f: file,
                            i: index,
                            r: name
                        });
                    }
                    index++;
                }
            } else {
                // Fallback for Excel/JSON
                const fileBuffer = fs.readFileSync(filePath);
                const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
                const sheet = workbook.Sheets[workbook.SheetNames[0]];
                const data = XLSX.utils.sheet_to_json<any>(sheet);

                data.forEach((row, idx) => {
                    const name = row['CompanyName'] || row['Company Name'] || row['Name'] || 'Unknown';
                    const cin = row['CIN'] || '';
                    const state = row['CompanyStateCode'] || row['State'] || '';
                    const status = row['CompanyStatus'] || row['Status'] || '';

                    newIndex.push({
                        n: name.toLowerCase(),
                        c: cin,
                        s: state,
                        st: status,
                        f: file,
                        i: idx,
                        r: name
                    });
                });
            }
        }

        console.log(`Writing index to ${INDEX_FILE}...`);
        fs.writeFileSync(INDEX_FILE, JSON.stringify(newIndex));
        console.log(`Index built in ${Date.now() - startTime}ms. Total records: ${newIndex.length}`);

    } catch (e) {
        console.error('Index build failed:', e);
        process.exit(1);
    }
}

buildIndex();
