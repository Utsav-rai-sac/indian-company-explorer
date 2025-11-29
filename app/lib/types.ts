export interface Company {
    id: string;
    name: string;
    state: string;
    cin?: string; // Corporate Identification Number
    status?: string;
    [key: string]: any; // Allow other fields from the sheet
}

export interface SearchIndex {
    n: string; // Name (lowercase for search)
    c: string; // CIN
    s: string; // State
    st: string; // Status
    f: string; // Filename
    i: number; // Line index in file
    r: string; // Raw name (display)
}
