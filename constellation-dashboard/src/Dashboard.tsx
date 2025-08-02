import React, { useEffect, useState } from 'react';
import SuggestionsPanel, { Suggestion } from './components/SuggestionsPanel';

interface DashboardProps {
  suggestionsEnabled?: boolean;
}

const Dashboard: React.FC<DashboardProps> = ({ suggestionsEnabled = true }) => {
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);

  useEffect(() => {
    if (suggestionsEnabled) {
      fetch('/suggestions')
        .then((res) => res.json())
        .then((data) => setSuggestions(data))
        .catch(() => setSuggestions([]));
    }
  }, [suggestionsEnabled]);

  const handleAction = (s: Suggestion) => {
    setSuggestions((prev) => prev.filter((item) => item.id !== s.id));
  };

  if (!suggestionsEnabled) {
    return null;
  }

  return (
    <div>
      <SuggestionsPanel
        suggestions={suggestions}
        onAccept={handleAction}
        onSnooze={handleAction}
        onDismiss={handleAction}
      />
    </div>
  );
};

export default Dashboard;
