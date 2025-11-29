'use client';

import { useState, useTransition } from 'react';
import { searchCompanies, logoutAction } from '../actions';
import { Company } from '../lib/types';
import { Search, Loader2, Building2, MapPin, FileText, LogIn, LogOut, Crown } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import LoginModal from './LoginModal';

export default function SearchInterface() {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<Company[]>([]);
    const [isPending, startTransition] = useTransition();
    const [hasSearched, setHasSearched] = useState(false);
    const [searchError, setSearchError] = useState('');
    const [remaining, setRemaining] = useState<number | null>(null);
    const [isPremium, setIsPremium] = useState(false);
    const [showLogin, setShowLogin] = useState(false);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (!query.trim()) return;

        setSearchError('');
        startTransition(async () => {
            try {
                const response = await searchCompanies(query);

                if (response.error) {
                    setSearchError(response.error);
                    setResults([]);
                    if (response.remaining !== undefined) setRemaining(response.remaining);
                } else {
                    setResults(response.results);
                    setRemaining(response.remaining ?? null);
                    setIsPremium(!!response.isPremium);
                }
                setHasSearched(true);
            } catch (err) {
                setSearchError('An unexpected error occurred.');
            }
        });
    };

    return (
        <div className="w-full space-y-8">
            {showLogin && <LoginModal onClose={() => setShowLogin(false)} />}

            <div className="flex justify-end">
                {isPremium ? (
                    <div className="flex items-center gap-4">
                        <span className="flex items-center gap-2 text-amber-400 font-medium px-3 py-1 bg-amber-400/10 rounded-full border border-amber-400/20">
                            <Crown className="h-4 w-4" />
                            Premium Access
                        </span>
                        <button
                            onClick={() => logoutAction()}
                            className="flex items-center gap-2 text-sm text-slate-400 hover:text-white transition-colors"
                        >
                            <LogOut className="h-4 w-4" />
                            Logout
                        </button>
                    </div>
                ) : (
                    <button
                        onClick={() => setShowLogin(true)}
                        className="flex items-center gap-2 text-sm font-medium text-blue-400 hover:text-blue-300 transition-colors px-4 py-2 rounded-lg hover:bg-blue-500/10"
                    >
                        <LogIn className="h-4 w-4" />
                        Premium Login
                    </button>
                )}
            </div>

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

                {!isPremium && remaining !== null && (
                    <p className="mt-2 text-center text-sm text-slate-500">
                        Free searches remaining today: <span className={remaining === 0 ? "text-red-400 font-bold" : "text-white font-bold"}>{remaining}</span>
                    </p>
                )}

                {searchError && (
                    <motion.div
                        initial={{ opacity: 0, y: -10 }}
                        animate={{ opacity: 1, y: 0 }}
                        className="mt-4 rounded-xl bg-red-500/10 border border-red-500/20 p-4 text-center text-red-200"
                    >
                        {searchError}
                        {!isPremium && searchError.includes('limit') && (
                            <button
                                onClick={() => setShowLogin(true)}
                                className="ml-2 underline hover:text-white"
                            >
                                Login now
                            </button>
                        )}
                    </motion.div>
                )}
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

                            <div className="mt-4 grid grid-cols-1 gap-4 text-sm text-slate-400 sm:grid-cols-2 lg:grid-cols-3">
                                {Object.entries(company)
                                    .filter(([key, value]) => !['id', 'name', 'state', 'cin', 'status'].includes(key) && value)
                                    .map(([key, value]) => (
                                        <div key={key} className="break-words">
                                            <span className="block text-xs uppercase tracking-wider text-slate-500 mb-1">{key.replace(/([A-Z])/g, ' $1').trim()}</span>
                                            <span className="block text-slate-300">{String(value)}</span>
                                        </div>
                                    ))}
                            </div>
                        </motion.div>
                    ))}
                </AnimatePresence>

                {hasSearched && results.length === 0 && !isPending && !searchError && (
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
