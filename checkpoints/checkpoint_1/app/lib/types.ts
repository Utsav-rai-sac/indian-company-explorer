export interface Company {
    id: string;
    name: string;
    state: string;
    cin?: string; // Corporate Identification Number
    status?: string;
    [key: string]: any; // Allow other fields from the sheet
}
