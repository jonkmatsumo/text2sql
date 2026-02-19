import { Route, Routes } from "react-router-dom";
import Layout from "./components/common/Layout";
import ReviewCuration from "./routes/ReviewCuration";
import Recommendations from "./routes/Recommendations";
import SystemOperations from "./routes/SystemOperations";
import AgentChat from "./routes/AgentChat";
import TraceDetail from "./routes/TraceDetail";
import TraceResolver from "./routes/TraceResolver";
import TraceExplorer from "./routes/TraceExplorer";
import TraceSearch from "./routes/TraceSearch";
import TraceCompare from "./routes/TraceCompare";
import MetricsPreview from "./routes/MetricsPreview";
import QueryTargetSettings from "./routes/QueryTargetSettings";
import Diagnostics from "./routes/Diagnostics";

import JobsDashboard from "./routes/JobsDashboard";
import RunDetails from "./routes/RunDetails";
import RunHistory from "./routes/RunHistory";

import { AdminErrorBoundary } from "./components/common/AdminErrorBoundary";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<AgentChat />} />
        <Route path="/traces/:traceId" element={<TraceDetail />} />
        <Route path="/traces/interaction/:interactionId" element={<TraceResolver />} />

        {/* Protected Operator Routes */}
        <Route path="/admin" element={<AdminErrorBoundary />}>
          <Route path="review" element={<ReviewCuration />} />
          <Route path="recommendations" element={<Recommendations />} />
          <Route path="operations" element={<SystemOperations />} />
          <Route path="jobs" element={<JobsDashboard />} />
          <Route path="traces" element={<TraceExplorer />} />
          <Route path="traces/search" element={<TraceSearch />} />
          <Route path="traces/compare" element={<TraceCompare />} />
          <Route path="observability/metrics" element={<MetricsPreview />} />
          <Route path="settings/query-target" element={<QueryTargetSettings />} />
          <Route path="diagnostics" element={<Diagnostics />} />
          <Route path="runs" element={<RunHistory />} />
          <Route path="runs/:runId" element={<RunDetails />} />
        </Route>
      </Routes>
    </Layout>
  );
}
