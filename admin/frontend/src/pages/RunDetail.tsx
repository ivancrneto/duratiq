import { Link as RouterLink, useParams } from "react-router-dom";
import {
  Box,
  Button,
  Divider,
  Grid,
  GridItem,
  HStack,
  Heading,
  Link,
  SimpleGrid,
  Spacer,
  Spinner,
  Stack,
  Text,
  useToast,
} from "@chakra-ui/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { TERMINAL, api } from "../api/client";
import { JsonBlock } from "../components/JsonBlock";
import { StatusBadge } from "../components/StatusBadge";

export function RunDetail() {
  const { runId = "" } = useParams();
  const qc = useQueryClient();
  const toast = useToast();
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
      toast({ status: "success", title: "Run cancelled" });
      refresh();
    },
    onError: (e: Error) =>
      toast({ status: "error", title: "Cancel failed", description: e.message }),
  });

  const retry = useMutation({
    mutationFn: () => api.retryRun(runId),
    onSuccess: () => {
      toast({ status: "success", title: "Retry enqueued", description: "Run re-armed to PENDING." });
      refresh();
    },
    onError: (e: Error) =>
      toast({ status: "error", title: "Retry failed", description: e.message }),
  });

  if (run.isLoading) return <Spinner />;
  if (run.error)
    return <Text color="red.500">{(run.error as Error).message}</Text>;
  if (!run.data) return null;

  const r = run.data;
  const isTerminal = TERMINAL.includes(r.status);

  return (
    <Stack spacing={6}>
      <Box>
        <Link as={RouterLink} to="/" color="purple.600" fontSize="sm">
          ← All runs
        </Link>
        <HStack mt={2} align="center" spacing={3}>
          <Heading size="md">{r.name}</Heading>
          <StatusBadge status={r.status} />
          <Spacer />
          {!isTerminal && (
            <Button
              size="sm"
              colorScheme="red"
              variant="outline"
              isLoading={cancel.isPending}
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
              colorScheme="purple"
              isLoading={retry.isPending}
              onClick={() =>
                window.confirm(`Retry run ${r.id}? This re-runs the failed step.`) &&
                retry.mutate()
              }
            >
              Retry
            </Button>
          )}
        </HStack>
        <Text fontFamily="mono" color="gray.500" fontSize="sm">
          {r.id}
        </Text>
      </Box>

      <SimpleGrid columns={{ base: 2, md: 4 }} spacing={4}>
        <Field label="Version" value={String(r.version)} />
        <Field label="Created" value={new Date(r.created_at).toLocaleString()} />
        <Field label="Updated" value={new Date(r.updated_at).toLocaleString()} />
        <Field label="Lease owner" value={r.lease_owner ?? "—"} />
      </SimpleGrid>

      <SimpleGrid columns={{ base: 1, md: 3 }} spacing={4}>
        <Labeled label="Input">
          <JsonBlock value={r.input} />
        </Labeled>
        <Labeled label="Result">
          <JsonBlock value={r.result} />
        </Labeled>
        <Labeled label="Error">
          <JsonBlock value={r.error} />
        </Labeled>
      </SimpleGrid>

      <Divider />

      <Box>
        <Heading size="sm" mb={3}>
          Steps {steps.data ? `(${steps.data.length})` : ""}
        </Heading>
        {steps.isLoading ? (
          <Spinner />
        ) : (
          <Stack spacing={3}>
            {steps.data?.map((s) => (
              <Grid
                key={s.seq}
                templateColumns={{ base: "1fr", md: "auto 1fr" }}
                gap={4}
                borderWidth="1px"
                borderRadius="md"
                p={4}
              >
                <GridItem>
                  <Text fontFamily="mono" color="gray.400" fontSize="sm">
                    #{s.seq}
                  </Text>
                </GridItem>
                <GridItem>
                  <HStack spacing={3} mb={2} flexWrap="wrap">
                    <Text fontWeight="semibold">{s.name}</Text>
                    <Text color="gray.500" fontSize="sm">
                      {s.kind}
                    </Text>
                    <StatusBadge status={s.status} />
                    {s.attempt > 0 && (
                      <Text color="gray.500" fontSize="sm">
                        attempt {s.attempt}
                      </Text>
                    )}
                  </HStack>
                  <SimpleGrid columns={{ base: 1, md: 3 }} spacing={3}>
                    <Labeled label="Input" small>
                      <JsonBlock value={s.input} />
                    </Labeled>
                    <Labeled label="Result" small>
                      <JsonBlock value={s.result} />
                    </Labeled>
                    <Labeled label="Error" small>
                      <JsonBlock value={s.error} />
                    </Labeled>
                  </SimpleGrid>
                </GridItem>
              </Grid>
            ))}
            {steps.data?.length === 0 && (
              <Text color="gray.500">No steps recorded yet.</Text>
            )}
          </Stack>
        )}
      </Box>
    </Stack>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <Box borderWidth="1px" borderRadius="md" p={3}>
      <Text color="gray.500" fontSize="xs">
        {label}
      </Text>
      <Text fontSize="sm">{value}</Text>
    </Box>
  );
}

function Labeled({
  label,
  small,
  children,
}: {
  label: string;
  small?: boolean;
  children: React.ReactNode;
}) {
  return (
    <Box>
      <Text color="gray.500" fontSize={small ? "xs" : "sm"} mb={1}>
        {label}
      </Text>
      {children}
    </Box>
  );
}
