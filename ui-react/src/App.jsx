import { Route, Routes } from "react-router-dom";
import AgentChat from "./routes/AgentChat.jsx";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<AgentChat />} />
    </Routes>
  );
}
