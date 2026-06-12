// Probes the API with the current token. On 401 it shows a token form instead of
// the app; on success it renders children. Re-runs whenever the token changes.

import { useEffect, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { ApiError, api } from "../api/client";
import { onTokenChange, setToken } from "./token";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function TokenGate({ children }: { children: ReactNode }) {
  const [, force] = useState(0);
  useEffect(() => onTokenChange(() => force((n) => n + 1)), []);

  const probe = useQuery({ queryKey: ["probe"], queryFn: api.stats, retry: false });

  if (probe.isLoading) {
    return (
      <div className="flex h-[60vh] items-center justify-center text-muted-foreground">
        Connecting…
      </div>
    );
  }

  if (probe.error instanceof ApiError && probe.error.status === 401) {
    return <TokenForm />;
  }

  if (probe.error) {
    return (
      <div className="flex h-[60vh] items-center justify-center px-6">
        <div className="max-w-md rounded-md border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          Cannot reach the API: {(probe.error as Error).message}
        </div>
      </div>
    );
  }

  return <>{children}</>;
}

function TokenForm() {
  const [value, setValue] = useState("");
  return (
    <div className="flex h-[70vh] items-center justify-center px-6">
      <div className="w-full max-w-sm space-y-4">
        <h1 className="text-lg font-semibold">Duratiq Admin</h1>
        <p className="text-sm text-muted-foreground">
          Enter the admin token to continue.
        </p>
        <Input
          type="password"
          placeholder="ADMIN_TOKEN"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && value && setToken(value)}
        />
        <Button className="w-full" disabled={!value} onClick={() => setToken(value)}>
          Sign in
        </Button>
      </div>
    </div>
  );
}
