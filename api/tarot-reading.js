export const config = { runtime: 'edge' };

const MODEL = 'gemini-2.5-flash';

const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60_000;
const RATE_LIMIT_MAX = 20;

function checkRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);
  if (!entry || now - entry.start > RATE_LIMIT_WINDOW) {
    rateLimitMap.set(ip, { start: now, count: 1 });
    return true;
  }
  entry.count++;
  return entry.count <= RATE_LIMIT_MAX;
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

function jsonRes(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
  if (req.method !== 'POST') return jsonRes(405, { error: 'Method not allowed' });

  const ip = req.headers.get('x-forwarded-for') || req.headers.get('x-real-ip') || 'unknown';
  if (!checkRateLimit(ip)) return jsonRes(429, { error: 'Too many requests' });

  const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
  if (!apiKey) return jsonRes(500, { error: 'API key not configured' });

  let body;
  try { body = await req.json(); } catch { return jsonRes(400, { error: 'Invalid JSON' }); }

  const prompt = body?.prompt;
  if (!prompt || typeof prompt !== 'string' || prompt.length > 4000) {
    return jsonRes(400, { error: 'Invalid prompt' });
  }
  const wantStream = body.stream === true;

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:streamGenerateContent?alt=sse&key=${apiKey}`;
  const payload = {
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: { maxOutputTokens: 2048, temperature: 0.9, topP: 0.95 },
    safetySettings: [
      { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_ONLY_HIGH' },
      { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_ONLY_HIGH' },
      { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_ONLY_HIGH' },
      { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_ONLY_HIGH' },
    ],
  };

  let upstream;
  try {
    upstream = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    return jsonRes(500, { error: 'Server error', detail: err.message });
  }

  if (!upstream.ok) {
    const errText = await upstream.text();
    return jsonRes(upstream.status, { error: 'API error', detail: errText.slice(0, 500) });
  }

  if (wantStream) {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const enc = new TextEncoder();

    (async () => {
      const reader = upstream.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          const lines = buf.split('\n');
          buf = lines.pop();
          for (const line of lines) {
            const ln = line.trim();
            if (!ln.startsWith('data:')) continue;
            const data = ln.slice(5).trim();
            if (!data || data === '[DONE]') continue;
            try {
              const parsed = JSON.parse(data);
              const t = parsed?.candidates?.[0]?.content?.parts?.[0]?.text;
              if (t) await writer.write(enc.encode('data: ' + JSON.stringify({ t }) + '\n\n'));
            } catch {}
          }
        }
        await writer.write(enc.encode('data: [DONE]\n\n'));
      } catch (e) {
        await writer.write(enc.encode('data: ' + JSON.stringify({ error: e.message }) + '\n\n'));
      } finally {
        await writer.close();
      }
    })();

    return new Response(readable, {
      status: 200,
      headers: {
        ...CORS,
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'X-Accel-Buffering': 'no',
      },
    });
  }

  const reader = upstream.body.getReader();
  const dec = new TextDecoder();
  let buf = '', text = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop();
    for (const line of lines) {
      const ln = line.trim();
      if (!ln.startsWith('data:')) continue;
      const data = ln.slice(5).trim();
      if (!data || data === '[DONE]') continue;
      try {
        const parsed = JSON.parse(data);
        const t = parsed?.candidates?.[0]?.content?.parts?.[0]?.text;
        if (t) text += t;
      } catch {}
    }
  }
  return jsonRes(200, { text });
}
