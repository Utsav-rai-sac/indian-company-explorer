
import fs from 'fs';
import path from 'path';
import readline from 'readline';

const DATA_DIR = path.join(process.cwd(), 'data');
const MAX_SIZE_BYTES = 40 * 1024 * 1024; // 40MB
const LINES_PER_CHUNK = 50000; // Adjust as needed

async function splitLargeFiles() {
    console.log('Checking for large files...');
    const files = fs.readdirSync(DATA_DIR);

    for (const file of files) {
        if (!file.endsWith('.csv')) continue;

        const filePath = path.join(DATA_DIR, file);
        const stats = fs.statSync(filePath);

        if (stats.size > MAX_SIZE_BYTES) {
            console.log(`Splitting ${file} (${(stats.size / 1024 / 1024).toFixed(2)} MB)...`);
            await splitFile(filePath, file);
        }
    }
}

async function splitFile(filePath: string, fileName: string) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    let header = '';
    let lineCount = 0;
    let chunkIndex = 1;
    let currentChunkLines: string[] = [];

    for await (const line of rl) {
        if (lineCount === 0) {
            header = line;
            lineCount++;
            continue;
        }

        currentChunkLines.push(line);

        if (currentChunkLines.length >= LINES_PER_CHUNK) {
            writeChunk(fileName, chunkIndex, header, currentChunkLines);
            chunkIndex++;
            currentChunkLines = [];
        }
        lineCount++;
    }

    if (currentChunkLines.length > 0) {
        writeChunk(fileName, chunkIndex, header, currentChunkLines);
    }

    console.log(`Finished splitting ${fileName} into ${chunkIndex} parts.`);

    // Rename original file to .bak so it's ignored by git but kept as backup
    const backupPath = filePath + '.bak';
    if (fs.existsSync(backupPath)) fs.unlinkSync(backupPath);
    fs.renameSync(filePath, backupPath);
    console.log(`Renamed original to ${fileName}.bak`);
}

function writeChunk(originalName: string, index: number, header: string, lines: string[]) {
    const ext = path.extname(originalName);
    const base = path.basename(originalName, ext);
    const chunkName = `${base}_part${index}${ext}`;
    const chunkPath = path.join(DATA_DIR, chunkName);

    const content = [header, ...lines].join('\n');
    fs.writeFileSync(chunkPath, content);
    console.log(`Written ${chunkName}`);
}

splitLargeFiles();
