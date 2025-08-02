import React from 'react';
import { render, fireEvent } from '@testing-library/react';
import SuggestionsPanel, { Suggestion } from './SuggestionsPanel';

const suggestion: Suggestion = { id: '1', text: 'Test suggestion' };

test('calls action handlers on buttons', () => {
  const accept = jest.fn();
  const snooze = jest.fn();
  const dismiss = jest.fn();

  const { getByText } = render(
    <SuggestionsPanel
      suggestions={[suggestion]}
      onAccept={accept}
      onSnooze={snooze}
      onDismiss={dismiss}
    />
  );

  fireEvent.click(getByText('Accept'));
  expect(accept).toHaveBeenCalledWith(suggestion);

  fireEvent.click(getByText('Snooze'));
  expect(snooze).toHaveBeenCalledWith(suggestion);

  fireEvent.click(getByText('Dismiss'));
  expect(dismiss).toHaveBeenCalledWith(suggestion);
});
