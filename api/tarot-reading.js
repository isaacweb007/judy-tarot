export const config = { runtime: 'edge' };

// Groq hosts Gemma + Llama models with a generous free tier (~14,400 req/day).
// Default to Gemma; override via GROQ_MODEL env var.
//   gemma2-9b-it           — Google Gemma 2 9B (good Korean, what the user wanted)
//   llama-3.3-70b-versatile — stronger Korean, also free
//   llama-3.1-8b-instant   — fastest, lighter
const MODEL = process.env.GROQ_MODEL || 'gemma2-9b-it';
const GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';

const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60000;
const RATE_LIMIT_MAX = 30;

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
const jsonHeaders = { ...CORS, 'Content-Type': 'application/json' };

function jsonRes(status, body) {
  return new Response(JSON.stringify(body), { status, headers: jsonHeaders });
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
  if (req.method !== 'POST') return jsonRes(405, { error: 'Method not allowed' });

  const ip = req.headers.get('x-forwarded-for') || req.headers.get('x-real-ip') || 'unknown';
  if (!checkRateLimit(ip)) return jsonRes(429, { error: 'Too many requests' });

  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) return jsonRes(500, { error: 'API key not configured' });

  let body;
  try { body = await req.json(); } catch { return jsonRes(400, { error: 'Invalid JSON' }); }

  const prompt = body?.prompt;
  if (!prompt || typeof prompt !== 'string' || prompt.length > 4000) {
    return jsonRes(400, { error: 'Invalid prompt' });
  }
  const wantStream = body.stream === true;

  const payload = {
    model: MODEL,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 1200,
    temperature: 0.85,
    top_p: 0.95,
    stream: wantStream,
  };

  let upstream;
  try {
    upstream = await fetch(GROQ_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + apiKey,
      },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    return jsonRes(500, { error: 'Server error', detail: err.message });
  }

  if (!upstream.ok) {
    const errText = await upstream.text();
    return jsonRes(upstream.status, { error: 'API error', detail: errText.slice(0, 500) });
  }

  // Non-streaming: parse OpenAI-format JSON response
  if (!wantStream) {
    try {
      const data = await upstream.json();
      const text = data?.choices?.[0]?.message?.content || '';
      return jsonRes(200, { text });
    } catch (err) {
      return jsonRes(500, { error: 'Parse error', detail: err.message });
    }
  }

  // Streaming: translate OpenAI SSE -> our {t:"..."} SSE format
  const enc = new TextEncoder();
  const readable = new ReadableStream({
    async start(controller) {
      // Initial flush keeps slow mobile connections alive
      controller.enqueue(enc.encode(': ready\n\n'));

      const reader = upstream.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      let lastFlush = Date.now();
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
            if (!data) continue;
            if (data === '[DONE]') continue;
            try {
              const parsed = JSON.parse(data);
              const t = parsed?.choices?.[0]?.delta?.content;
              if (t) {
                controller.enqueue(enc.encode('data: ' + JSON.stringify({ t }) + '\n\n'));
                lastFlush = Date.now();
              }
            } catch {}
          }
          if (Date.now() - lastFlush > 5000) {
            controller.enqueue(enc.encode(': hb\n\n'));
            lastFlush = Date.now();
          }
        }
        controller.enqueue(enc.encode('data: [DONE]\n\n'));
      } catch (e) {
        controller.enqueue(enc.encode('data: ' + JSON.stringify({ error: e.message || 'stream error' }) + '\n\n'));
      } finally {
        controller.close();
      }
    },
  });

  return new Response(readable, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'X-Accel-Buffering': 'no',
      'Connection': 'keep-alive',
    },
  });
}
