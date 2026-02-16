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

import JobsDashboard from "./routes/JobsDashboard";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<AgentChat />} />
        <Route path="/traces/:traceId" element={<TraceDetail />} />
        <Route path="/traces/interaction/:interactionId" element={<TraceResolver />} />
        <Route path="/admin/review" element={<ReviewCuration />} />
        <Route path="/admin/recommendations" element={<Recommendations />} />
        <Route path="/admin/operations" element={<SystemOperations />} />
        <Route path="/admin/jobs" element={<JobsDashboard />} />
        <Route path="/admin/traces" element={<TraceExplorer />} />
        <Route path="/admin/traces/search" element={<TraceSearch />} />
        <Route path="/admin/traces/compare" element={<TraceCompare />} />
        <Route path="/admin/observability/metrics" element={<MetricsPreview />} />
        <Route path="/admin/settings/query-target" element={<QueryTargetSettings />} />
      </Routes>
    </Layout>
  );
}
