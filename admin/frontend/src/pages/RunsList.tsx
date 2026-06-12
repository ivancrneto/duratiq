import { useState } from "react";
import { Link as RouterLink } from "react-router-dom";
import {
  Button,
  Flex,
  HStack,
  Input,
  Link,
  SimpleGrid,
  Spinner,
  Stat,
  StatLabel,
  StatNumber,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  Select,
  Box,
} from "@chakra-ui/react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";

const STATUSES = [
  "PENDING",
  "RUNNING",
  "SUSPENDED",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
];
const PAGE = 25;

export function RunsList() {
  const [status, setStatus] = useState("");
  const [name, setName] = useState("");
  const [offset, setOffset] = useState(0);

  const stats = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  const runs = useQuery({
    queryKey: ["runs", status, name, offset],
    queryFn: () =>
      api.listRuns({ status, name, limit: PAGE, offset }),
    placeholderData: keepPreviousData,
  });

  return (
    <Box>
      <SimpleGrid columns={{ base: 2, md: 4 }} spacing={4} mb={6}>
        <StatCard label="Total runs" value={stats.data?.total} />
        {["COMPLETED", "RUNNING", "FAILED"].map((s) => (
          <StatCard key={s} label={s} value={stats.data?.by_status[s] ?? 0} />
        ))}
      </SimpleGrid>

      <HStack mb={4} spacing={3} flexWrap="wrap">
        <Select
          maxW="48"
          placeholder="All statuses"
          value={status}
          onChange={(e) => {
            setStatus(e.target.value);
            setOffset(0);
          }}
        >
          {STATUSES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </Select>
        <Input
          maxW="64"
          placeholder="Filter by workflow name"
          value={name}
          onChange={(e) => {
            setName(e.target.value);
            setOffset(0);
          }}
        />
        <Button onClick={() => runs.refetch()} isLoading={runs.isFetching}>
          Refresh
        </Button>
      </HStack>

      {runs.isLoading ? (
        <Spinner />
      ) : (
        <Box borderWidth="1px" borderRadius="md" overflowX="auto">
          <Table size="sm">
            <Thead>
              <Tr>
                <Th>Run ID</Th>
                <Th>Workflow</Th>
                <Th>Status</Th>
                <Th>Created</Th>
              </Tr>
            </Thead>
            <Tbody>
              {runs.data?.items.map((r) => (
                <Tr key={r.id}>
                  <Td>
                    <Link
                      as={RouterLink}
                      to={`/runs/${r.id}`}
                      color="purple.600"
                      fontFamily="mono"
                    >
                      {r.id.slice(0, 12)}…
                    </Link>
                  </Td>
                  <Td>{r.name}</Td>
                  <Td>
                    <StatusBadge status={r.status} />
                  </Td>
                  <Td whiteSpace="nowrap" color="gray.500">
                    {new Date(r.created_at).toLocaleString()}
                  </Td>
                </Tr>
              ))}
              {runs.data?.items.length === 0 && (
                <Tr>
                  <Td colSpan={4}>
                    <Text color="gray.500" py={4}>
                      No runs match.
                    </Text>
                  </Td>
                </Tr>
              )}
            </Tbody>
          </Table>
        </Box>
      )}

      <Flex mt={4} align="center" justify="space-between">
        <Text color="gray.500" fontSize="sm">
          {runs.data
            ? `${offset + 1}–${Math.min(offset + PAGE, runs.data.total)} of ${runs.data.total}`
            : ""}
        </Text>
        <HStack>
          <Button
            size="sm"
            onClick={() => setOffset(Math.max(0, offset - PAGE))}
            isDisabled={offset === 0}
          >
            Prev
          </Button>
          <Button
            size="sm"
            onClick={() => setOffset(offset + PAGE)}
            isDisabled={!runs.data || offset + PAGE >= runs.data.total}
          >
            Next
          </Button>
        </HStack>
      </Flex>
    </Box>
  );
}

function StatCard({ label, value }: { label: string; value?: number }) {
  return (
    <Stat borderWidth="1px" borderRadius="md" p={4}>
      <StatLabel color="gray.500">{label}</StatLabel>
      <StatNumber>{value ?? "—"}</StatNumber>
    </Stat>
  );
}
