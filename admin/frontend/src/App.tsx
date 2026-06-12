import { Link as RouterLink, Route, Routes } from "react-router-dom";
import { Box, Button, Container, Flex, Heading, Spacer } from "@chakra-ui/react";
import { TokenGate } from "./auth/TokenGate";
import { clearToken } from "./auth/token";
import { RunsList } from "./pages/RunsList";
import { RunDetail } from "./pages/RunDetail";

export default function App() {
  return (
    <Box minH="100vh" bg="gray.50">
      <Box bg="white" borderBottomWidth="1px">
        <Container maxW="6xl">
          <Flex align="center" h={14}>
            <Heading
              size="sm"
              as={RouterLink}
              to="/"
              _hover={{ color: "purple.600" }}
            >
              Duratiq Admin
            </Heading>
            <Spacer />
            <Button size="sm" variant="ghost" onClick={clearToken}>
              Sign out
            </Button>
          </Flex>
        </Container>
      </Box>
      <Container maxW="6xl" py={8}>
        <TokenGate>
          <Routes>
            <Route path="/" element={<RunsList />} />
            <Route path="/runs/:runId" element={<RunDetail />} />
          </Routes>
        </TokenGate>
      </Container>
    </Box>
  );
}
