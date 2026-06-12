import { Box } from "@chakra-ui/react";

// Pretty-print a JSON value (input / result / error payloads).
export function JsonBlock({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return (
      <Box as="span" color="gray.400" fontStyle="italic">
        —
      </Box>
    );
  }
  return (
    <Box
      as="pre"
      bg="gray.50"
      borderWidth="1px"
      borderColor="gray.200"
      borderRadius="md"
      p={3}
      fontSize="xs"
      overflowX="auto"
      whiteSpace="pre-wrap"
      wordBreak="break-word"
    >
      {JSON.stringify(value, null, 2)}
    </Box>
  );
}
