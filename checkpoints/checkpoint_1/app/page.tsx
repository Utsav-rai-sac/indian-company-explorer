import SearchInterface from "./components/SearchInterface";

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-4 text-white">
      <div className="w-full max-w-4xl space-y-8 text-center">
        <div className="space-y-4">
          <h1 className="text-5xl font-bold tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-emerald-400 sm:text-7xl">
            Company Explorer
          </h1>
          <p className="text-lg text-slate-400">
            Search across multiple state databases instantly.
          </p>
        </div>
        
        <SearchInterface />
      </div>
    </main>
  );
}
