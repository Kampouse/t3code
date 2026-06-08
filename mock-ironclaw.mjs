/**
 * Mock IronClaw server for testing the driver.
 * Implements the real Web Gateway API surface.
 */
import http from "node:http";

const PORT = 3000;

// In-memory state
let messageIdCounter = 0;
const eventSubscribers = [];

function broadcast(eventType, data) {
  const payload = JSON.stringify({ type: eventType, ...data });
  for (const sub of eventSubscribers) {
    sub(eventType, { type: eventType, ...data });
  }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);

  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "*");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  // Auth check
  const auth = req.headers["authorization"];
  if (auth && auth !== "Bearer test-token") {
    res.writeHead(401); res.end(JSON.stringify({ error: "Unauthorized" })); return;
  }

  if (url.pathname === "/api/health" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", channel: "gateway" }));
    return;
  }

  if (url.pathname === "/api/chat/send" && req.method === "POST") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const parsed = JSON.parse(body);
      const msgId = `msg-${++messageIdCounter}`;
      console.log(`[mock] Received message: "${parsed.content}" → ${msgId}`);

      // Simulate agent processing: emit stream_chunk events then response
      const threadId = parsed.thread_id ?? "thread-default";
      const responseText = `Hello! You said: "${parsed.content}". I am a mock IronClaw agent.`;

      // Emit thinking
      setTimeout(() => broadcast("thinking", { message: "Processing your request...", thread_id: threadId }), 100);

      // Emit stream chunks
      const words = responseText.split(" ");
      words.forEach((word, i) => {
        setTimeout(() => {
          broadcast("stream_chunk", { content: (i > 0 ? " " : "") + word, thread_id: threadId });
        }, 300 + i * 80);
      });

      // Emit response (final)
      setTimeout(() => {
        broadcast("response", { content: responseText, thread_id: threadId });
        broadcast("status", { message: "idle", thread_id: threadId });
      }, 300 + words.length * 80 + 100);

      res.writeHead(202, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ message_id: msgId, status: "accepted" }));
    });
    return;
  }

  if (url.pathname === "/api/chat/events" && req.method === "GET") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
    });

    const subscriber = (_eventType, data) => {
      res.write(`event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`);
    };

    eventSubscribers.push(subscriber);

    // Send heartbeat every 15s
    const heartbeat = setInterval(() => {
      res.write(`event: heartbeat\ndata: {"type":"heartbeat"}\n\n`);
    }, 15000);

    req.on("close", () => {
      clearInterval(heartbeat);
      const idx = eventSubscribers.indexOf(subscriber);
      if (idx >= 0) eventSubscribers.splice(idx, 1);
    });
    return;
  }

  if (url.pathname === "/api/chat/history" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      thread_id: "thread-default",
      turns: [{
        turn_number: 1,
        user_input: "test",
        response: "Hello! You said: test. I am a mock IronClaw agent.",
        state: "Completed",
        started_at: new Date().toISOString(),
        completed_at: new Date().toISOString(),
        tool_calls: [],
      }],
      has_more: false,
    }));
    return;
  }

  if (url.pathname === "/api/jobs" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ jobs: [] }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Mock IronClaw server running on http://localhost:${PORT}`);
  console.log("Endpoints:");
  console.log(`  GET  /api/health`);
  console.log(`  POST /api/chat/send`);
  console.log(`  GET  /api/chat/events (SSE)`);
  console.log(`  GET  /api/chat/history`);
  console.log(`  GET  /api/jobs`);
  console.log(`\nAuth: Bearer test-token`);
});
