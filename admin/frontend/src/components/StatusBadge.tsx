import { cn } from "@/lib/utils";

const STYLES: Record<string, string> = {
  PENDING: "bg-muted text-muted-foreground",
  SCHEDULED: "bg-muted text-muted-foreground",
  RUNNING: "bg-blue-100 text-blue-700",
  SUSPENDED: "bg-amber-100 text-amber-700",
  COMPLETED: "bg-emerald-100 text-emerald-700",
  FAILED: "bg-red-100 text-red-700",
  CANCELLED: "bg-orange-100 text-orange-700",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold",
        STYLES[status] ?? "bg-muted text-muted-foreground",
      )}
    >
      {status}
    </span>
  );
}
