import { useState } from "react";
import { Link as RouterLink } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const STATUSES = [
  "PENDING",
  "RUNNING",
  "SUSPENDED",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
];
const PAGE = 25;
const ALL = "__all__"; // Radix Select items can't have an empty value.

export function RunsList() {
  const [status, setStatus] = useState("");
  const [name, setName] = useState("");
  const [offset, setOffset] = useState(0);

  const stats = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  const runs = useQuery({
    queryKey: ["runs", status, name, offset],
    queryFn: () => api.listRuns({ status, name, limit: PAGE, offset }),
    placeholderData: keepPreviousData,
  });

  return (
    <div>
      <div className="mb-6 grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Total runs" value={stats.data?.total} />
        {["COMPLETED", "RUNNING", "FAILED"].map((s) => (
          <StatCard key={s} label={s} value={stats.data?.by_status[s] ?? 0} />
        ))}
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="w-48">
          <Select
            value={status || ALL}
            onValueChange={(v) => {
              setStatus(v === ALL ? "" : v);
              setOffset(0);
            }}
          >
            <SelectTrigger>
              <SelectValue placeholder="All statuses" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All statuses</SelectItem>
              {STATUSES.map((s) => (
                <SelectItem key={s} value={s}>
                  {s}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <Input
          className="w-64"
          placeholder="Filter by workflow name"
          value={name}
          onChange={(e) => {
            setName(e.target.value);
            setOffset(0);
          }}
        />
        <Button variant="outline" onClick={() => runs.refetch()}>
          Refresh
        </Button>
      </div>

      {runs.isLoading ? (
        <div className="text-muted-foreground">Loading…</div>
      ) : (
        <Card className="overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Run ID</TableHead>
                <TableHead>Workflow</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.data?.items.map((r) => (
                <TableRow key={r.id}>
                  <TableCell>
                    <RouterLink
                      to={`/runs/${r.id}`}
                      className="font-mono text-primary hover:underline"
                    >
                      {r.id.slice(0, 12)}…
                    </RouterLink>
                  </TableCell>
                  <TableCell>{r.name}</TableCell>
                  <TableCell>
                    <StatusBadge status={r.status} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">
                    {new Date(r.created_at).toLocaleString()}
                  </TableCell>
                </TableRow>
              ))}
              {runs.data?.items.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4}>
                    <div className="py-4 text-muted-foreground">No runs match.</div>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </Card>
      )}

      <div className="mt-4 flex items-center justify-between">
        <span className="text-sm text-muted-foreground">
          {runs.data
            ? `${offset + 1}–${Math.min(offset + PAGE, runs.data.total)} of ${runs.data.total}`
            : ""}
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setOffset(Math.max(0, offset - PAGE))}
            disabled={offset === 0}
          >
            Prev
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setOffset(offset + PAGE)}
            disabled={!runs.data || offset + PAGE >= runs.data.total}
          >
            Next
          </Button>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value?: number }) {
  return (
    <Card className="p-4">
      <div className="text-sm text-muted-foreground">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value ?? "—"}</div>
    </Card>
  );
}
