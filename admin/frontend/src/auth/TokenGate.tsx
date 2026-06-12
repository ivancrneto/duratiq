// Probes the API with the current token. On 401 it shows a token form instead of
// the app; on success it renders children. Re-runs whenever the token changes.

import { useEffect, useState } from "react";
import {
  Alert,
  AlertIcon,
  Box,
  Button,
  Center,
  Heading,
  Input,
  Stack,
  Text,
} from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import { ApiError, api } from "../api/client";
import { onTokenChange, setToken } from "./token";

export function TokenGate({ children }: { children: React.ReactNode }) {
  const [, force] = useState(0);
  useEffect(() => onTokenChange(() => force((n) => n + 1)), []);

  const probe = useQuery({
    queryKey: ["probe"],
    queryFn: api.stats,
    retry: false,
  });

  if (probe.isLoading) {
    return (
      <Center h="60vh">
        <Text color="gray.500">Connecting…</Text>
      </Center>
    );
  }

  if (probe.error instanceof ApiError && probe.error.status === 401) {
    return <TokenForm />;
  }

  if (probe.error) {
    return (
      <Center h="60vh" px={6}>
        <Alert status="error" maxW="md" borderRadius="md">
          <AlertIcon />
          Cannot reach the API: {(probe.error as Error).message}
        </Alert>
      </Center>
    );
  }

  return <>{children}</>;
}

function TokenForm() {
  const [value, setValue] = useState("");
  return (
    <Center h="70vh" px={6}>
      <Box maxW="sm" w="full">
        <Stack spacing={4}>
          <Heading size="md">Duratiq Admin</Heading>
          <Text color="gray.500" fontSize="sm">
            Enter the admin token to continue.
          </Text>
          <Input
            type="password"
            placeholder="ADMIN_TOKEN"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && value && setToken(value)}
          />
          <Button
            colorScheme="purple"
            isDisabled={!value}
            onClick={() => setToken(value)}
          >
            Sign in
          </Button>
        </Stack>
      </Box>
    </Center>
  );
}
