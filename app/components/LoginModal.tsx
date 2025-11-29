'use client';

import { useState, useTransition } from 'react';
import { loginAction } from '../actions';
import { Loader2, Lock } from 'lucide-react';

export default function LoginModal({ onClose }: { onClose: () => void }) {
    const [isPending, startTransition] = useTransition();
    const [error, setError] = useState('');

    const handleSubmit = (formData: FormData) => {
        setError('');
        startTransition(async () => {
            const result = await loginAction(formData);
            if (result.success) {
                window.location.reload(); // Reload to update UI state
            } else {
                setError(result.error || 'Login failed');
            }
        });
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
            <div className="w-full max-w-md rounded-2xl bg-slate-900 border border-slate-700 p-8 shadow-2xl">
                <div className="mb-6 flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-full bg-blue-500/20 text-blue-400">
                        <Lock className="h-5 w-5" />
                    </div>
                    <h2 className="text-xl font-semibold text-white">Premium Login</h2>
                </div>

                <form action={handleSubmit} className="space-y-4">
                    <div>
                        <label className="mb-1 block text-sm font-medium text-slate-400">Username</label>
                        <input
                            name="username"
                            type="text"
                            required
                            className="w-full rounded-lg border border-slate-700 bg-slate-800 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                            placeholder="Enter username"
                        />
                    </div>
                    <div>
                        <label className="mb-1 block text-sm font-medium text-slate-400">Password</label>
                        <input
                            name="password"
                            type="password"
                            required
                            className="w-full rounded-lg border border-slate-700 bg-slate-800 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                            placeholder="Enter password"
                        />
                    </div>

                    {error && (
                        <div className="rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
                            {error}
                        </div>
                    )}

                    <div className="flex gap-3 pt-2">
                        <button
                            type="button"
                            onClick={onClose}
                            className="flex-1 rounded-lg border border-slate-700 bg-transparent px-4 py-2.5 font-medium text-slate-300 hover:bg-slate-800"
                        >
                            Cancel
                        </button>
                        <button
                            type="submit"
                            disabled={isPending}
                            className="flex-1 rounded-lg bg-blue-600 px-4 py-2.5 font-medium text-white hover:bg-blue-500 disabled:opacity-50 flex justify-center items-center gap-2"
                        >
                            {isPending && <Loader2 className="h-4 w-4 animate-spin" />}
                            Login
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
}
