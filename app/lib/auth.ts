import { cookies } from 'next/headers';
import path from 'path';
import * as XLSX from 'xlsx';
import fs from 'fs';

const USERS_FILE = path.join(process.cwd(), 'data', 'users.xlsx');

// Simple in-memory rate limiter: IP -> count
const RATE_LIMIT = new Map<string, { count: number; timestamp: number }>();
const LIMIT_WINDOW = 24 * 60 * 60 * 1000; // 24 hours
const MAX_FREE_SEARCHES = 10;

export async function checkRateLimit(ip: string): Promise<{ allowed: boolean; remaining: number }> {
    const now = Date.now();
    const record = RATE_LIMIT.get(ip);

    if (!record) {
        RATE_LIMIT.set(ip, { count: 1, timestamp: now });
        return { allowed: true, remaining: MAX_FREE_SEARCHES - 1 };
    }

    // Reset if window passed
    if (now - record.timestamp > LIMIT_WINDOW) {
        RATE_LIMIT.set(ip, { count: 1, timestamp: now });
        return { allowed: true, remaining: MAX_FREE_SEARCHES - 1 };
    }

    if (record.count >= MAX_FREE_SEARCHES) {
        return { allowed: false, remaining: 0 };
    }

    record.count += 1;
    return { allowed: true, remaining: MAX_FREE_SEARCHES - record.count };
}

export async function verifyUser(username: string, password: string): Promise<boolean> {
    if (!fs.existsSync(USERS_FILE)) {
        console.warn('Users file not found at:', USERS_FILE);
        return false;
    }

    try {
        console.log(`Attempting to verify user: ${username}`);
        // Use fs.readFileSync for better stability against file locks
        const fileBuffer = fs.readFileSync(USERS_FILE);
        const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const data = XLSX.utils.sheet_to_json<any>(sheet, { header: 1 });

        // Skip header row if it exists, assume Col A is index 0, Col B is index 1
        for (const row of data) {
            const storedUser = String(row[0]);
            const storedPass = String(row[1]);

            // Debug log (be careful with passwords in production, but helpful here)
            // console.log(`Checking against: ${storedUser}`);

            if (storedUser === username && storedPass === password) {
                console.log('User verified successfully');
                return true;
            }
        }
        console.log('User not found or password incorrect');
    } catch (error) {
        console.error('Error reading users file during verification:', error);
    }

    return false;
}

export async function isUserLoggedIn() {
    const cookieStore = await cookies();
    return cookieStore.has('premium_session');
}
