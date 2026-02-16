
import React from 'react';
import { PHASE_ORDER } from '../../types/runLifecycle';

const PHASES: Record<string, string> = {
  'router': 'Routing',
  'plan': 'Planning',
  'execute': 'Executing SQL',
  'synthesize': 'Synthesizing',
  'visualize': 'Visualizing',
  'correct': 'Correcting',
  'clarify': 'Clarifying'
};

interface ExecutionProgressProps {
  currentPhase: string | null;
  completedPhases?: string[];
  correctionAttempt?: number;
  className?: string;
}

export function ExecutionProgress({ currentPhase, completedPhases = [], correctionAttempt, className }: ExecutionProgressProps) {
  if (!currentPhase) return null;

  const label = PHASES[currentPhase] || currentPhase;

  return (
    <div className={`execution-progress ${className || ''}`} style={{
      margin: '16px 0',
      padding: '12px 16px',
      background: 'var(--surface-muted)',
      borderRadius: '8px',
      fontSize: '0.9rem',
      color: 'var(--ink)',
      display: 'flex',
      flexDirection: 'column',
      gap: '12px',
    }}>
      {/* Stepper rail */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
      }}>
        {PHASE_ORDER.map((phase, idx) => {
          const isCompleted = completedPhases.includes(phase);
          const isCurrent = phase === currentPhase;
          const isFuture = !isCompleted && !isCurrent;
          const phaseLabel = PHASES[phase] || phase;

          return (
            <React.Fragment key={phase}>
              {idx > 0 && (
                <div style={{
                  flex: 1,
                  height: '2px',
                  background: isCompleted ? 'var(--accent, #6366f1)' : 'var(--border, #e2e8f0)',
                  minWidth: '12px',
                }} />
              )}
              <div
                data-testid={`phase-${phase}`}
                title={phaseLabel}
                style={{
                  width: '24px',
                  height: '24px',
                  borderRadius: '50%',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: '0.65rem',
                  fontWeight: 600,
                  flexShrink: 0,
                  ...(isCompleted
                    ? {
                        background: 'var(--accent, #6366f1)',
                        color: '#fff',
                      }
                    : isCurrent
                      ? {
                          background: 'transparent',
                          border: '2px solid var(--accent, #6366f1)',
                          color: 'var(--accent, #6366f1)',
                          animation: 'pulse-phase 1.5s ease-in-out infinite',
                        }
                      : {
                          background: 'var(--border, #e2e8f0)',
                          color: 'var(--muted, #94a3b8)',
                        }),
                }}
              >
                {isCompleted ? '\u2713' : isFuture ? '' : ''}
              </div>
            </React.Fragment>
          );
        })}
      </div>

      {/* Current phase label with spinner */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <div className="spinner" style={{
          width: '16px',
          height: '16px',
          border: '2px solid var(--border)',
          borderTopColor: 'var(--accent)',
          borderRadius: '50%',
          animation: 'spin 1s linear infinite',
          flexShrink: 0,
        }} />
        <span style={{ fontWeight: 500, fontSize: '0.85rem' }}>
          {label}...
        </span>
      </div>

      {currentPhase === 'correct' && correctionAttempt != null && correctionAttempt > 0 && (
        <div data-testid="correction-attempt" style={{
          fontSize: '0.8rem',
          color: 'var(--muted, #94a3b8)',
          fontStyle: 'italic',
        }}>
          Attempt {correctionAttempt}: correcting SQL
        </div>
      )}

      <style>{`
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        @keyframes pulse-phase { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
      `}</style>
    </div>
  );
}
