'use server';

import fs from 'fs';
import path from 'path';
import * as XLSX from 'xlsx';
import { Company } from './lib/types';

const DATA_DIR = path.join(process.cwd(), 'data');

let CACHED_DATA: Company[] | null = null;

async function loadData() {
    if (CACHED_DATA) return CACHED_DATA;

    console.log('Loading data into memory...');
    const allCompanies: Company[] = [];

    if (!fs.existsSync(DATA_DIR)) {
        console.warn('Data directory not found');
        return [];
    }

    const files = fs.readdirSync(DATA_DIR);

    for (const file of files) {
        if (!file.match(/\.(xlsx|xls|csv|json)$/)) continue;

        try {
            console.log(`Reading file: ${file}`);
            const filePath = path.join(DATA_DIR, file);
            let data: any[] = [];

            if (file.endsWith('.json')) {
                const fileContent = fs.readFileSync(filePath, 'utf-8');
                const jsonData = JSON.parse(fileContent);
                data = Array.isArray(jsonData) ? jsonData : [];
            } else {
                const fileBuffer = fs.readFileSync(filePath);
                const workbook = XLSX.read(fileBuffer, { type: 'buffer' });

                const sheetName = workbook.SheetNames[0];
                const sheet = workbook.Sheets[sheetName];
                data = XLSX.utils.sheet_to_json<any>(sheet);
            }

            const fileCompanies = data.map((row, index) => ({
                id: `${file}-${index}`,
                name: row['CompanyName'] || row['Company Name'] || row['Name'] || 'Unknown',
                state: row['CompanyStateCode'] || row['State'] || file.replace(/\.(xlsx|xls|csv|json)$/, ''),
                cin: row['CIN'],
                status: row['CompanyStatus'] || row['Status'],
                ...row
            }));

            allCompanies.push(...fileCompanies);
            console.log(`Loaded ${fileCompanies.length} records from ${file}`);
        } catch (error) {
            console.error(`Failed to load ${file}:`, error);
        }
    }

    CACHED_DATA = allCompanies;
    console.log(`Total records loaded: ${allCompanies.length}`);
    return allCompanies;
}

export async function searchCompanies(query: string): Promise<Company[]> {
    if (!query || query.length < 2) return [];

    try {
        const data = await loadData();
        const lowerQuery = query.toLowerCase();

        // Search in the cached data
        const results = data.filter(company => {
            // Optimize: Check name first as it's most likely
            if (company.name?.toLowerCase().includes(lowerQuery)) return true;

            // Then check other fields
            const values = Object.values(company).join(' ').toLowerCase();
            return values.includes(lowerQuery);
        });

        return results.slice(0, 50);
    } catch (error) {
        console.error('Critical error in searchCompanies:', error);
        throw new Error(`Search failed: ${(error as Error).message}`);
    }
}
