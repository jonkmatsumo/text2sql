import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import Recommendations from './Recommendations';
import { OpsService, AdminService } from '../api';
import { vi, describe, it, expect } from 'vitest';

// Mock dependencies
vi.mock('../api', () => ({
  AdminService: {
    listPins: vi.fn().mockResolvedValue([]),
    upsertPin: vi.fn(),
    deletePin: vi.fn(),
  },
  OpsService: {
    runRecommendations: vi.fn(),
  }
}));

vi.mock('../hooks/useToast', () => ({
  useToast: () => ({ show: vi.fn() }),
}));

vi.mock('../hooks/useConfirmation', () => ({
  useConfirmation: () => ({ confirm: vi.fn(), dialogProps: {} }),
}));

describe('Recommendations', () => {
  it('renders recommendation fingerprint in playground cards', async () => {
    const mockResult = {
      examples: [
        {
          question: "Test Q",
          source: "golden",
          metadata: {
            fingerprint: "1234567890abcdef",
            status: "approved",
            pinned: false
          }
        },
        {
          question: "Test Q 2",
          source: "ai",
          metadata: {
            fingerprint: "", // Empty/missing
            status: "pending",
            pinned: false
          }
        }
      ],
      metadata: {
        count_total: 2,
        count_approved: 1,
        count_seeded: 0,
        count_fallback: 0,
        pins_selected_count: 0,
        pins_matched_rules: [],
        truncated: false
      },
      fallback_used: false
    };

    // @ts-ignore
    OpsService.runRecommendations.mockResolvedValue(mockResult);

    render(<Recommendations />);

    // Switch to Playground tab
    const playgroundTab = screen.getByText('Playground');
    fireEvent.click(playgroundTab);

    // Type a query
    const textarea = screen.getByLabelText(/natural language query/i);
    fireEvent.change(textarea, { target: { value: 'test query' } });

    // Run
    const runBtn = await screen.findByText('Run Inspection');
    await waitFor(() => {
      expect(runBtn).toBeEnabled();
    });
    fireEvent.click(runBtn);

    await waitFor(() => {
        expect(OpsService.runRecommendations).toHaveBeenCalled();
    });

    // Check for truncated fingerprint "12345678"
    expect(screen.getByText('12345678')).toBeInTheDocument();

    // Check for "N/A" for the second example
    expect(screen.getByText('N/A')).toBeInTheDocument();
  });
});
