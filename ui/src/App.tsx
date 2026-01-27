import { Route, Routes } from "react-router-dom";
import Layout from "./components/common/Layout";
import ReviewCuration from "./routes/ReviewCuration";
import Recommendations from "./routes/Recommendations";
import SystemOperations from "./routes/SystemOperations";
import AgentChat from "./routes/AgentChat";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<AgentChat />} />
        <Route path="/admin/review" element={<ReviewCuration />} />
        <Route path="/admin/recommendations" element={<Recommendations />} />
        <Route path="/admin/operations" element={<SystemOperations />} />
      </Routes>
    </Layout>
  );
}
