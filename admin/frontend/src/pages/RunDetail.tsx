import { type ReactNode, useState } from "react";
import { Link as RouterLink, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { TERMINAL, api } from "../api/client";
import { JsonBlock } from "../components/JsonBlock";
import { StatusBadge } from "../components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";

export function RunDetail() {
  const { runId = "" } = useParams();
  const qc = useQueryClient();
  const run = useQuery({ queryKey: ["run", runId], queryFn: () => api.getRun(runId) });
  const steps = useQuery({
    queryKey: ["steps", runId],
    queryFn: () => api.getSteps(runId),
  });

  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["run", runId] });
    qc.invalidateQueries({ queryKey: ["steps", runId] });
    qc.invalidateQueries({ queryKey: ["stats"] });
    qc.invalidateQueries({ queryKey: ["runs"] });
  };

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(runId),
    onSuccess: () => {
      toast.success("Run cancelled");
      refresh();
    },
    onError: (e: Error) => toast.error("Cancel failed", { description: e.message }),
  });

  const retry = useMutation({
    mutationFn: () => api.retryRun(runId),
    onSuccess: () => {
      toast.success("Retry enqueued", { description: "Run re-armed to PENDING." });
      refresh();
    },
    onError: (e: Error) => toast.error("Retry failed", { description: e.message }),
  });

  const [signalName, setSignalName] = useState("");
  const [signalPayload, setSignalPayload] = useState("");
  const signal = useMutation({
    mutationFn: () => {
      let payload: unknown = null;
      if (signalPayload.trim()) {
        try {
          payload = JSON.parse(signalPayload);
        } catch {
          throw new Error("Payload must be valid JSON (or empty)");
        }
      }
      return api.signalRun(runId, signalName.trim(), payload);
    },
    onSuccess: () => {
      toast.success("Signal delivered", { description: "A tick was enqueued to advance the run." });
      setSignalName("");
      setSignalPayload("");
      refresh();
    },
    onError: (e: Error) => toast.error("Signal failed", { description: e.message }),
  });

  if (run.isLoading) return <div className="text-muted-foreground">Loading…</div>;
  if (run.error)
    return <div className="text-destructive">{(run.error as Error).message}</div>;
  if (!run.data) return null;

  const r = run.data;
  const isTerminal = TERMINAL.includes(r.status);

  return (
    <div className="space-y-6">
      <div>
        <RouterLink to="/" className="text-sm text-primary hover:underline">
          ← All runs
        </RouterLink>
        <div className="mt-2 flex items-center gap-3">
          <h1 className="text-xl font-semibold">{r.name}</h1>
          <StatusBadge status={r.status} />
          <div className="ml-auto flex gap-2">
            {!isTerminal && (
              <Button
                variant="destructive"
                size="sm"
                disabled={cancel.isPending}
                onClick={() =>
                  window.confirm(`Cancel run ${r.id}?`) && cancel.mutate()
                }
              >
                Cancel
              </Button>
            )}
            {r.status === "FAILED" && (
              <Button
                size="sm"
                disabled={retry.isPending}
                onClick={() =>
                  window.confirm(`Retry run ${r.id}? This re-runs the failed step.`) &&
                  retry.mutate()
                }
              >
                Retry
              </Button>
            )}
          </div>
        </div>
        <div className="font-mono text-sm text-muted-foreground">{r.id}</div>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <Field label="Version" value={String(r.version)} />
        <Field label="Created" value={new Date(r.created_at).toLocaleString()} />
        <Field label="Updated" value={new Date(r.updated_at).toLocaleString()} />
        <Field label="Lease owner" value={r.lease_owner ?? "—"} />
      </div>

      {r.parent_run_id && (
        <Card className="p-3">
          <div className="text-xs text-muted-foreground">Child of</div>
          <RouterLink
            to={`/runs/${r.parent_run_id}`}
            className="font-mono text-sm text-primary hover:underline"
          >
            {r.parent_run_id}
          </RouterLink>
          <span className="ml-2 text-xs text-muted-foreground">(step #{r.parent_seq})</span>
        </Card>
      )}

      {Object.keys(r.search_attributes).length > 0 && (
        <Labeled label="Search attributes">
          <JsonBlock value={r.search_attributes} />
        </Labeled>
      )}

      {!isTerminal && (
        <Card className="space-y-3 p-4">
          <div className="text-sm font-semibold">Send signal</div>
          <div className="flex flex-wrap items-start gap-3">
            <Input
              className="w-48"
              placeholder="Signal name"
              value={signalName}
              onChange={(e) => setSignalName(e.target.value)}
            />
            <Input
              className="w-72 font-mono"
              placeholder='Payload JSON (optional), e.g. {"approved": true}'
              value={signalPayload}
              onChange={(e) => setSignalPayload(e.target.value)}
            />
            <Button
              size="sm"
              disabled={!signalName.trim() || signal.isPending}
              onClick={() => signal.mutate()}
            >
              Send
            </Button>
          </div>
        </Card>
      )}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <Labeled label="Input">
          <JsonBlock value={r.input} />
        </Labeled>
        <Labeled label="Result">
          <JsonBlock value={r.result} />
        </Labeled>
        <Labeled label="Error">
          <JsonBlock value={r.error} />
        </Labeled>
      </div>

      <Separator />

      <div>
        <h2 className="mb-3 text-sm font-semibold">
          Steps {steps.data ? `(${steps.data.length})` : ""}
        </h2>
        {steps.isLoading ? (
          <div className="text-muted-foreground">Loading…</div>
        ) : (
          <div className="space-y-3">
            {steps.data?.map((s) => (
              <Card key={s.seq} className="p-4">
                <div className="flex gap-4">
                  <div className="font-mono text-sm text-muted-foreground">
                    #{s.seq}
                  </div>
                  <div className="flex-1">
                    <div className="mb-2 flex flex-wrap items-center gap-3">
                      <span className="font-semibold">{s.name}</span>
                      <span className="text-sm text-muted-foreground">{s.kind}</span>
                      <StatusBadge status={s.status} />
                      {s.attempt > 0 && (
                        <span className="text-sm text-muted-foreground">
                          attempt {s.attempt}
                        </span>
                      )}
                      {s.timeout_at && (
                        <span className="text-sm text-muted-foreground">
                          timeout {new Date(s.timeout_at).toLocaleTimeString()}
                        </span>
                      )}
                    </div>
                    {s.heartbeat != null && (
                      <Labeled label="Heartbeat" small>
                        <JsonBlock value={s.heartbeat} />
                      </Labeled>
                    )}
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                      <Labeled label="Input" small>
                        <JsonBlock value={s.input} />
                      </Labeled>
                      <Labeled label="Result" small>
                        <JsonBlock value={s.result} />
                      </Labeled>
                      <Labeled label="Error" small>
                        <JsonBlock value={s.error} />
                      </Labeled>
                    </div>
                  </div>
                </div>
              </Card>
            ))}
            {steps.data?.length === 0 && (
              <div className="text-muted-foreground">No steps recorded yet.</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <Card className="p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-sm">{value}</div>
    </Card>
  );
}

function Labeled({
  label,
  small,
  children,
}: {
  label: string;
  small?: boolean;
  children: ReactNode;
}) {
  return (
    <div>
      <div className={cn("mb-1 text-muted-foreground", small ? "text-xs" : "text-sm")}>
        {label}
      </div>
      {children}
    </div>
  );
}
