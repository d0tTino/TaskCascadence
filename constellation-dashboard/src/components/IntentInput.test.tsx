import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';
import IntentInput from './IntentInput';

afterEach(() => {
  // @ts-ignore
  global.fetch = undefined;
});

test('handles clarification flow', async () => {
  const fetchMock = jest
    .fn()
    .mockResolvedValueOnce({ json: async () => ({ clarification: 'Which account?' }) })
    .mockResolvedValueOnce({ json: async () => ({ tasks: ['Task A'] }) });
  // @ts-ignore
  global.fetch = fetchMock;

  const { getByTestId, getByText } = render(<IntentInput />);

  fireEvent.change(getByTestId('intent-input'), { target: { value: 'pay bills' } });
  fireEvent.click(getByText('Submit'));

  await waitFor(() => getByTestId('clar-input'));
  fireEvent.change(getByTestId('clar-input'), { target: { value: 'Personal' } });
  fireEvent.click(getByText('Submit'));

  await waitFor(() => getByTestId('task-preview'));
  expect(fetchMock).toHaveBeenNthCalledWith(
    1,
    '/intent',
    expect.objectContaining({ body: JSON.stringify({ text: 'pay bills' }) })
  );
  expect(fetchMock).toHaveBeenNthCalledWith(
    2,
    '/intent',
    expect.objectContaining({ body: JSON.stringify({ text: 'pay bills', clarification: 'Personal' }) })
  );
  expect(getByTestId('task-preview').textContent).toContain('Task A');
});

test('shows tasks on success', async () => {
  const fetchMock = jest.fn().mockResolvedValue({ json: async () => ({ tasks: ['Task B'] }) });
  // @ts-ignore
  global.fetch = fetchMock;

  const { getByTestId, getByText } = render(<IntentInput />);

  fireEvent.change(getByTestId('intent-input'), { target: { value: 'buy milk' } });
  fireEvent.click(getByText('Submit'));

  await waitFor(() => getByTestId('task-preview'));
  expect(getByTestId('task-preview').textContent).toContain('Task B');
});
