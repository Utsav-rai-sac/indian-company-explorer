export default function AboutPage() {
    return (
        <main className="flex min-h-[calc(100vh-4rem)] flex-col items-center justify-center p-4">
            <div className="w-full max-w-3xl space-y-8">
                <div className="space-y-4 text-center">
                    <h1 className="text-4xl font-bold tracking-tight text-white sm:text-5xl">
                        About Us
                    </h1>
                    <p className="text-lg text-slate-400">
                        Empowering businesses with comprehensive data insights.
                    </p>
                </div>

                <div className="rounded-2xl bg-white/5 p-8 backdrop-blur-sm border border-white/10 space-y-6 text-slate-300 leading-relaxed">
                    <p>
                        Welcome to <strong>Company Explorer</strong>, your premier destination for accessing detailed information about companies registered in India. Our mission is to simplify business research by providing a fast, reliable, and user-friendly interface to search across millions of company records.
                    </p>

                    <h2 className="text-xl font-semibold text-white pt-4">Our Data</h2>
                    <p>
                        We aggregate data from various official sources, ensuring you have access to the most up-to-date information including:
                    </p>
                    <ul className="list-disc pl-6 space-y-2 text-slate-400">
                        <li>Company Registration Details (CIN, Date of Incorporation)</li>
                        <li>Registered Address and State</li>
                        <li>Current Status (Active, Strike Off, etc.)</li>
                        <li>Authorized and Paid-up Capital</li>
                    </ul>

                    <h2 className="text-xl font-semibold text-white pt-4">Why Choose Us?</h2>
                    <p>
                        Unlike traditional government portals that can be slow and difficult to navigate, Company Explorer offers:
                    </p>
                    <ul className="list-disc pl-6 space-y-2 text-slate-400">
                        <li><strong>Lightning Fast Search:</strong> Instant results powered by our optimized in-memory database.</li>
                        <li><strong>User-Friendly Interface:</strong> A clean, modern design that works on any device.</li>
                        <li><strong>Premium Access:</strong> Unlimited searches and advanced filtering for power users.</li>
                    </ul>
                </div>
            </div>
        </main>
    );
}
