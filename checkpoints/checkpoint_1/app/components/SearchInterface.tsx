'use client';

import { useState, useTransition } from 'react';
import { searchCompanies } from '../actions';
import { Company } from '../lib/types';
import { Search, Loader2, Building2, MapPin, FileText } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function SearchInterface() {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<Company[]>([]);
    const [isPending, startTransition] = useTransition();
    const [hasSearched, setHasSearched] = useState(false);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (!query.trim()) return;

        startTransition(async () => {
            const data = await searchCompanies(query);
            setResults(data);
            setHasSearched(true);
        });
    };

    return (
        <div className="w-full space-y-8">
            <form onSubmit={handleSearch} className="relative mx-auto max-w-2xl">
                <div className="relative">
                    <Search className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-slate-400" />
                    <input
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Enter company name..."
                        className="h-14 w-full rounded-2xl border-0 bg-white/10 pl-12 pr-4 text-lg text-white placeholder:text-slate-400 focus:ring-2 focus:ring-blue-500 backdrop-blur-sm transition-all"
                    />
                    <button
                        type="submit"
                        disabled={isPending}
                        className="absolute right-2 top-1/2 -translate-y-1/2 rounded-xl bg-blue-600 px-4 py-2 font-medium text-white transition-colors hover:bg-blue-500 disabled:opacity-50"
                    >
                        {isPending ? <Loader2 className="h-5 w-5 animate-spin" /> : 'Search'}
                    </button>
                </div>
                <p className="mt-2 text-sm text-slate-500">
                    Make sure your Excel/CSV files are in the <code>data/</code> folder.
                </p>
            </form>

            <div className="space-y-4">
                <AnimatePresence mode="popLayout">
                    {results.map((company) => (
                        <motion.div
                            key={company.id}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, scale: 0.95 }}
                            layout
                            className="overflow-hidden rounded-xl bg-white/5 p-6 text-left backdrop-blur-md transition-colors hover:bg-white/10 border border-white/10"
                        >
                            <div className="flex items-start justify-between gap-4">
                                <div className="space-y-1">
                                    <h3 className="text-xl font-semibold text-white flex items-center gap-2">
                                        <Building2 className="h-5 w-5 text-blue-400" />
                                        {company.name}
                                    </h3>
                                    <div className="flex items-center gap-4 text-sm text-slate-300">
                                        <span className="flex items-center gap-1">
                                            <MapPin className="h-4 w-4 text-emerald-400" />
                                            {company.state}
                                        </span>
                                        {company.cin && (
                                            <span className="flex items-center gap-1 font-mono text-slate-400">
                                                <FileText className="h-4 w-4" />
                                                {company.cin}
                                            </span>
                                        )}
                                    </div>
                                </div>
                                {company.status && (
                                    <span className={`rounded-full px-3 py-1 text-xs font-medium ${company.status.toLowerCase().includes('active')
                                            ? 'bg-emerald-500/20 text-emerald-300'
                                            : 'bg-slate-500/20 text-slate-300'
                                        }`}>
                                        {company.status}
                                    </span>
                                )}
                            </div>

                            {/* Expandable details could go here */}
                            <div className="mt-4 grid grid-cols-2 gap-4 text-sm text-slate-400 sm:grid-cols-3">
                                {Object.entries(company)
                                    .filter(([key]) => !['id', 'name', 'state', 'cin', 'status'].includes(key))
                                    .slice(0, 6) // Show first 6 other fields
                                    .map(([key, value]) => (
                                        <div key={key}>
                                            <span className="block text-xs uppercase tracking-wider text-slate-500">{key}</span>
                                            <span className="block truncate text-slate-300">{String(value)}</span>
                                        </div>
                                    ))}
                            </div>
                        </motion.div>
                    ))}
                </AnimatePresence>

                {hasSearched && results.length === 0 && !isPending && (
                    <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className="text-center text-slate-400"
                    >
                        No companies found matching "{query}".
                    </motion.div>
                )}
            </div>
        </div>
    );
}
