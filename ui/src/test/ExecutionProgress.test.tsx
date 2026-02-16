
import { ExecutionProgress } from "../components/common/ExecutionProgress";
import { render, screen } from "@testing-library/react";

describe("ExecutionProgress", () => {
    it("renders progress phase correctly", () => {
        render(<ExecutionProgress currentPhase="plan" />);
        expect(screen.getByText("Planning...")).toBeDefined();
    });

    it("renders unknown phase correctly", () => {
        render(<ExecutionProgress currentPhase="unknown_phase" />);
        expect(screen.getByText("unknown_phase...")).toBeDefined();
    });

    it("renders nothing if no phase", () => {
        const { container } = render(<ExecutionProgress currentPhase={null} />);
        expect(container.firstChild).toBeNull();
    });

    it("shows stepper rail with all phases", () => {
        render(<ExecutionProgress currentPhase="execute" />);
        expect(screen.getByTestId("phase-router")).toBeDefined();
        expect(screen.getByTestId("phase-plan")).toBeDefined();
        expect(screen.getByTestId("phase-execute")).toBeDefined();
        expect(screen.getByTestId("phase-synthesize")).toBeDefined();
        expect(screen.getByTestId("phase-visualize")).toBeDefined();
    });

    it("marks completed phases with checkmark", () => {
        render(
            <ExecutionProgress
                currentPhase="execute"
                completedPhases={["router", "plan"]}
            />
        );
        expect(screen.getByTestId("phase-router")).toHaveTextContent("\u2713");
        expect(screen.getByTestId("phase-plan")).toHaveTextContent("\u2713");
        expect(screen.getByTestId("phase-execute")).not.toHaveTextContent("\u2713");
    });

    it("shows current phase label with spinner", () => {
        render(
            <ExecutionProgress
                currentPhase="synthesize"
                completedPhases={["router", "plan", "execute"]}
            />
        );
        expect(screen.getByText("Synthesizing...")).toBeDefined();
    });

    it("shows correction attempt when in correct phase", () => {
        render(
            <ExecutionProgress
                currentPhase="correct"
                completedPhases={["router", "plan", "execute"]}
                correctionAttempt={2}
            />
        );
        expect(screen.getByTestId("correction-attempt")).toHaveTextContent("Attempt 2: correcting SQL");
    });

    it("does not show correction attempt when not in correct phase", () => {
        render(
            <ExecutionProgress
                currentPhase="execute"
                correctionAttempt={1}
            />
        );
        expect(screen.queryByTestId("correction-attempt")).toBeNull();
    });
});
