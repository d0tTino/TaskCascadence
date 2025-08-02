import React from 'react';
import { render } from '@testing-library/react';
import Dashboard from './Dashboard';

const suggestions = [{ id: '1', text: 'Hello' }];

afterEach(() => {
  (global.fetch as jest.Mock | undefined)?.mockClear();
});

test('renders suggestions panel when enabled', async () => {
  global.fetch = jest.fn(() =>
    Promise.resolve({ json: () => Promise.resolve(suggestions) })
  ) as jest.Mock;

  const { findAllByTestId } = render(<Dashboard suggestionsEnabled />);
  const items = await findAllByTestId('suggestion');
  expect(items).toHaveLength(1);
});

test('does not render suggestions panel when disabled', () => {
  global.fetch = jest.fn(() => Promise.resolve({ json: () => suggestions })) as jest.Mock;

  const { queryByTestId } = render(<Dashboard suggestionsEnabled={false} />);
  expect(global.fetch).not.toHaveBeenCalled();
  expect(queryByTestId('suggestion')).toBeNull();
});
