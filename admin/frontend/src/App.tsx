import { Link as RouterLink, Route, Routes } from "react-router-dom";
import { TokenGate } from "./auth/TokenGate";
import { clearToken } from "./auth/token";
import { RunsList } from "./pages/RunsList";
import { RunDetail } from "./pages/RunDetail";
import { Button } from "@/components/ui/button";

export default function App() {
  return (
    <div className="min-h-screen bg-muted/30">
      <header className="border-b bg-background">
        <div className="mx-auto flex h-14 max-w-6xl items-center px-4">
          <RouterLink to="/" className="text-sm font-semibold hover:text-primary">
            Duratiq Admin
          </RouterLink>
          <div className="ml-auto">
            <Button variant="ghost" size="sm" onClick={clearToken}>
              Sign out
            </Button>
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-8">
        <TokenGate>
          <Routes>
            <Route path="/" element={<RunsList />} />
            <Route path="/runs/:runId" element={<RunDetail />} />
          </Routes>
        </TokenGate>
      </main>
    </div>
  );
}
