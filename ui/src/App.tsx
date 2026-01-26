import { Route, Routes } from "react-router-dom";
import AgentChat from "./routes/AgentChat";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<AgentChat />} />
    </Routes>
  );
}
