import React, { useState } from 'react';

const IntentInput: React.FC = () => {
  const [text, setText] = useState('');
  const [clarPrompt, setClarPrompt] = useState<string | null>(null);
  const [clarAnswer, setClarAnswer] = useState('');
  const [tasks, setTasks] = useState<string[]>([]);

  const submit = async () => {
    const payload: Record<string, unknown> = { text };
    if (clarPrompt) {
      payload.clarification = clarAnswer;
    }
    const res = await fetch('/intent', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.clarification) {
      setClarPrompt(data.clarification);
      setClarAnswer('');
      setTasks([]);
    } else if (data.tasks) {
      setTasks(data.tasks);
      setClarPrompt(null);
    }
  };

  return (
    <div>
      {!clarPrompt && (
        <input
          data-testid="intent-input"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
      )}
      {clarPrompt && (
        <div>
          <div>{clarPrompt}</div>
          <input
            data-testid="clar-input"
            value={clarAnswer}
            onChange={(e) => setClarAnswer(e.target.value)}
          />
        </div>
      )}
      <button onClick={submit}>Submit</button>
      {tasks.length > 0 && (
        <ul data-testid="task-preview">
          {tasks.map((t, i) => (
            <li key={i}>{t}</li>
          ))}
        </ul>
      )}
    </div>
  );
};

export default IntentInput;
