import { Badge } from "@chakra-ui/react";

const COLORS: Record<string, string> = {
  PENDING: "gray",
  SCHEDULED: "gray",
  RUNNING: "blue",
  SUSPENDED: "yellow",
  COMPLETED: "green",
  FAILED: "red",
  CANCELLED: "orange",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <Badge colorScheme={COLORS[status] ?? "gray"} variant="subtle">
      {status}
    </Badge>
  );
}
