
'use client';

import { Mail, MapPin, Phone, Send } from 'lucide-react';
import { useState } from 'react';

export default function ContactPage() {
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        const formData = new FormData(e.target as HTMLFormElement);
        const subject = formData.get('subject') as string;
        const message = formData.get('message') as string;
        const name = `${formData.get('firstName')} ${formData.get('lastName')}`;

        // Construct mailto link
        const mailtoLink = `mailto:write2vedant@gmail.com?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(`Name: ${name}\n\n${message}`)}`;

        window.location.href = mailtoLink;
        setSubmitted(true);
    };

    return (
        <main className="flex min-h-[calc(100vh-4rem)] flex-col items-center justify-center p-4">
            <div className="w-full max-w-2xl space-y-8">
                <div className="space-y-4 text-center">
                    <h1 className="text-4xl font-bold tracking-tight text-white sm:text-5xl">
                        Contact Us
                    </h1>
                    <p className="text-lg text-slate-400">
                        Fill out the form below to send us a message directly.
                    </p>
                </div>

                {/* Contact Form */}
                <div className="rounded-2xl bg-white/5 p-8 backdrop-blur-sm border border-white/10">
                    {submitted ? (
                        <div className="h-full flex flex-col items-center justify-center text-center space-y-4 py-12">
                            <div className="h-16 w-16 rounded-full bg-green-500/20 flex items-center justify-center text-green-400">
                                <Send className="h-8 w-8" />
                            </div>
                            <h3 className="text-2xl font-bold text-white">Opening Email Client...</h3>
                            <p className="text-slate-400">
                                We've prepared the email for you. Please hit send in your mail app!
                            </p>
                            <button
                                onClick={() => setSubmitted(false)}
                                className="text-blue-400 hover:text-blue-300 underline"
                            >
                                Send another message
                            </button>
                        </div>
                    ) : (
                        <form onSubmit={handleSubmit} className="space-y-6">
                            <div className="grid gap-6 sm:grid-cols-2">
                                <div className="space-y-2">
                                    <label className="text-sm font-medium text-slate-300">First Name</label>
                                    <input
                                        name="firstName"
                                        type="text"
                                        required
                                        className="w-full rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                        placeholder="John"
                                    />
                                </div>
                                <div className="space-y-2">
                                    <label className="text-sm font-medium text-slate-300">Last Name</label>
                                    <input
                                        name="lastName"
                                        type="text"
                                        required
                                        className="w-full rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                        placeholder="Doe"
                                    />
                                </div>
                            </div>

                            <div className="space-y-2">
                                <label className="text-sm font-medium text-slate-300">Email</label>
                                <input
                                    name="email"
                                    type="email"
                                    required
                                    className="w-full rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                    placeholder="john@example.com"
                                />
                            </div>

                            <div className="space-y-2">
                                <label className="text-sm font-medium text-slate-300">Subject</label>
                                <select name="subject" className="w-full rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500">
                                    <option>General Inquiry</option>
                                    <option>Support</option>
                                    <option>Sales</option>
                                    <option>Partnership</option>
                                </select>
                            </div>

                            <div className="space-y-2">
                                <label className="text-sm font-medium text-slate-300">Message</label>
                                <textarea
                                    name="message"
                                    required
                                    rows={4}
                                    className="w-full rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-white focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                    placeholder="How can we help you?"
                                />
                            </div>

                            <button
                                type="submit"
                                className="w-full rounded-lg bg-blue-600 px-4 py-3 font-semibold text-white transition-colors hover:bg-blue-500 flex items-center justify-center gap-2"
                            >
                                <Send className="h-4 w-4" />
                                Send Message
                            </button>
                        </form>
                    )}
                </div>
            </div>
        </main>
    );
}
