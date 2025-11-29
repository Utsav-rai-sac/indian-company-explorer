'use server';

import fs from 'fs';
import path from 'path';
import * as XLSX from 'xlsx';
import { Company, SearchIndex } from './lib/types';
import { cookies, headers } from 'next/headers';
import { checkRateLimit, isUserLoggedIn, verifyUser } from './lib/auth';
import { redirect } from 'next/navigation';
import readline from 'readline';

const DATA_DIR = path.join(process.cwd(), 'data');

// Optimized In-Memory Index
// We store only essential data for search and list display.
// This eliminates Disk I/O during search queries.


// Global cache for the index
let GLOBAL_INDEX: SearchIndex[] | null = null;
let IS_INDEXING = false;

const INDEX_FILE = path.join(DATA_DIR, 'search-index.json');

async function buildIndex() {
    if (GLOBAL_INDEX || IS_INDEXING) return;
    IS_INDEXING = true;

    // Try loading from file first
    if (fs.existsSync(INDEX_FILE)) {
        console.log('Loading index from file...');
        try {
            const fileContent = fs.readFileSync(INDEX_FILE, 'utf-8');
            GLOBAL_INDEX = JSON.parse(fileContent);
            IS_INDEXING = false;
            console.log(`Index loaded. Total records: ${GLOBAL_INDEX?.length}`);
            return;
        } catch (e) {
            console.error('Failed to load index file:', e);
        }
    }

    console.log('Starting index build (fallback)...');
    const startTime = Date.now();

    const newIndex: SearchIndex[] = [];

    try {
        if (!fs.existsSync(DATA_DIR)) {
            console.warn('Data directory missing!');
            GLOBAL_INDEX = [];
            return;
        }

        const files = fs.readdirSync(DATA_DIR);

        for (const file of files) {
            if (!file.match(/\.(csv|xlsx|xls|json)$/)) continue;

            const filePath = path.join(DATA_DIR, file);

            if (file.endsWith('.csv')) {
                const fileStream = fs.createReadStream(filePath);
                const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

                let isHeader = true;
                let nameIdx = -1, cinIdx = -1, stateIdx = -1, statusIdx = -1;
                let index = 0;

                for await (const line of rl) {
                    // Simple CSV parser: handles quoted fields roughly
                    // We split by comma but respect quotes
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
    } catch (e) {
        console.error('Index build failed:', e);
    }

    GLOBAL_INDEX = newIndex;
    IS_INDEXING = false;
    console.log(`Index built in ${Date.now() - startTime}ms. Total records: ${newIndex.length}`);
}

export async function loginAction(formData: FormData) {
    const username = formData.get('username') as string;
    const password = formData.get('password') as string;

    if (await verifyUser(username, password)) {
        const cookieStore = await cookies();
        cookieStore.set('premium_session', 'true', {
            httpOnly: true,
            secure: process.env.NODE_ENV === 'production',
            maxAge: 60 * 60 * 24 * 7 // 1 week
        });
        return { success: true };
    }

    return { success: false, error: 'Invalid credentials' };
}

export async function logoutAction() {
    const cookieStore = await cookies();
    cookieStore.delete('premium_session');
    redirect('/');
}

export async function searchCompanies(query: string): Promise<{ results: Company[], error?: string, remaining?: number, isPremium?: boolean }> {
    if (!query || query.length < 2) return { results: [] };

    const isLoggedIn = await isUserLoggedIn();
    let remaining = -1;

    if (!isLoggedIn) {
        const headersList = await headers();
        const ip = headersList.get('x-forwarded-for') || '127.0.0.1';

        const limit = await checkRateLimit(ip);
        if (!limit.allowed) {
            return {
                results: [],
                error: 'Free search limit exceeded (10/day). Please login for unlimited access.',
                remaining: 0,
                isPremium: false
            };
        }
        remaining = limit.remaining;
    }

    try {
        // Build index if missing
        if (!GLOBAL_INDEX) {
            await buildIndex();
        }

        if (!GLOBAL_INDEX) return { results: [] };

        // 1. Find matches in the In-Memory Index (Fast)
        const matchedEntries: SearchIndex[] = [];
        const lowerQuery = query.toLowerCase();

        for (const entry of GLOBAL_INDEX) {
            if (entry.n.includes(lowerQuery) || (entry.c && entry.c.toLowerCase().includes(lowerQuery))) {
                matchedEntries.push(entry);
                if (matchedEntries.length >= 50) break;
            }
        }

        // 2. Fetch FULL details from disk for the matches (Slower but complete)
        // Group by file to minimize file opens
        const matchesByFile = new Map<string, number[]>();
        for (const match of matchedEntries) {
            if (!matchesByFile.has(match.f)) {
                matchesByFile.set(match.f, []);
            }
            matchesByFile.get(match.f)!.push(match.i);
        }

        const results: Company[] = [];

        for (const [file, indices] of matchesByFile.entries()) {
            const targetIndices = new Set(indices);
            const filePath = path.join(DATA_DIR, file);

            try {
                if (file.endsWith('.csv')) {
                    const fileStream = fs.createReadStream(filePath);
                    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

                    let isHeader = true;
                    let currentIndex = 0;
                    let header: string[] = [];

                    for await (const line of rl) {
                        // Simple CSV parser
                        const values: string[] = [];
                        let inQuote = false;
                        let currentVal = '';
                        for (let i = 0; i < line.length; i++) {
                            const char = line[i];
                            if (char === '"') inQuote = !inQuote;
                            else if (char === ',' && !inQuote) {
                                values.push(currentVal.trim());
                                currentVal = '';
                            } else currentVal += char;
                        }
                        values.push(currentVal.trim());
                        const cleanValues = values.map(v => v.replace(/^"|"$/g, '').trim());

                        if (isHeader) {
                            header = cleanValues;
                            isHeader = false;
                            continue;
                        }

                        if (targetIndices.has(currentIndex)) {
                            const row: any = {};
                            header.forEach((h, i) => {
                                row[h] = cleanValues[i] || '';
                            });

                            results.push({
                                id: `${file}-${currentIndex}`,
                                name: row['CompanyName'] || row['Company Name'] || row['Name'] || 'Unknown',
                                state: row['CompanyStateCode'] || row['State'] || '',
                                cin: row['CIN'],
                                status: row['CompanyStatus'] || row['Status'],
                                ...row
                            });
                        }
                        currentIndex++;
                    }
                } else {
                    // Fallback for Excel/JSON
                    const fileBuffer = fs.readFileSync(filePath);
                    const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
                    const sheet = workbook.Sheets[workbook.SheetNames[0]];
                    const data = XLSX.utils.sheet_to_json<any>(sheet);

                    indices.forEach(idx => {
                        const row = data[idx];
                        if (row) {
                            results.push({
                                id: `${file}-${idx}`,
                                name: row['CompanyName'] || row['Company Name'] || row['Name'] || 'Unknown',
                                state: row['CompanyStateCode'] || row['State'] || '',
                                cin: row['CIN'],
                                status: row['CompanyStatus'] || row['Status'],
                                ...row
                            });
                        }
                    });
                }
            } catch (err) {
                console.error(`Error reading details from ${file}:`, err);
            }
        }

        return {
            results,
            remaining,
            isPremium: isLoggedIn
        };
    } catch (error) {
        console.error('Critical error in searchCompanies:', error);
        throw new Error(`Search failed: ${(error as Error).message}`);
    }
}
