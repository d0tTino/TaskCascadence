import React from 'react';

export interface Suggestion {
  id: string;
  text: string;
}

interface SuggestionsPanelProps {
  suggestions: Suggestion[];
  onAccept: (s: Suggestion) => void;
  onSnooze: (s: Suggestion) => void;
  onDismiss: (s: Suggestion) => void;
}

const SuggestionsPanel: React.FC<SuggestionsPanelProps> = ({
  suggestions,
  onAccept,
  onSnooze,
  onDismiss,
}) => {
  if (!suggestions.length) {
    return <div>No suggestions</div>;
  }

  return (
    <div>
      {suggestions.map((s) => (
        <div key={s.id} data-testid="suggestion">
          <span>{s.text}</span>
          <button onClick={() => onAccept(s)}>Accept</button>
          <button onClick={() => onSnooze(s)}>Snooze</button>
          <button onClick={() => onDismiss(s)}>Dismiss</button>
        </div>
      ))}
    </div>
  );
};

export default SuggestionsPanel;
