'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Building2, Info, Mail, Home } from 'lucide-react';
import { clsx } from 'clsx';

export default function Header() {
    const pathname = usePathname();

    const navItems = [
        { name: 'Home', href: '/', icon: Home },
        { name: 'About Us', href: '/about', icon: Info },
        { name: 'Contact Us', href: '/contact', icon: Mail },
    ];

    return (
        <header className="fixed top-0 left-0 right-0 z-50 border-b border-white/10 bg-slate-900/80 backdrop-blur-md">
            <div className="mx-auto flex h-16 max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
                <Link href="/" className="flex items-center gap-2 text-xl font-bold text-white transition-opacity hover:opacity-80">
                    <Building2 className="h-6 w-6 text-blue-400" />
                    <span>Company Explorer</span>
                </Link>

                <nav className="flex gap-1">
                    {navItems.map((item) => {
                        const isActive = pathname === item.href;
                        return (
                            <Link
                                key={item.name}
                                href={item.href}
                                className={clsx(
                                    'flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors',
                                    isActive
                                        ? 'bg-white/10 text-white'
                                        : 'text-slate-400 hover:bg-white/5 hover:text-white'
                                )}
                            >
                                <item.icon className="h-4 w-4" />
                                <span className="hidden sm:inline">{item.name}</span>
                            </Link>
                        );
                    })}
                </nav>
            </div>
        </header>
    );
}
