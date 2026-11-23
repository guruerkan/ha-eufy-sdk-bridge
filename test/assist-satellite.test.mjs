import { test } from "node:test";
import assert from "node:assert/strict";

import { audioFrame, buildRunMessage, createAssistSatellite, describeEvent } from "../src/assist-satellite.mjs";
import { loadConfig } from "../src/config.mjs";

test("run message: wake_word → intent, 16 kHz input, pipeline only when set", () => {
  assert.deepEqual(buildRunMessage(5), {
    id: 5,
    type: "assist_pipeline/run",
    start_stage: "wake_word",
    end_stage: "intent",
    input: { sample_rate: 16000, timeout: 3, noise_suppression_level: 0 },
  });
  assert.equal(buildRunMessage(6, { pipelineId: "abc" }).pipeline, "abc");
});

test("binary frames carry the handler id byte first; a bare byte ends the stream", () => {
  assert.deepEqual([...audioFrame(3, Buffer.from([1, 2]))], [3, 1, 2]);
  assert.deepEqual([...audioFrame(3)], [3]);
});

test("event lines: wake word, STT text, intent; routine timeout stays quiet", () => {
  assert.equal(
    describeEvent({ type: "wake_word-end", data: { wake_word_output: { wake_word_id: "hey_jarvis" } } }),
    "wake word detected: hey_jarvis",
  );
  assert.equal(describeEvent({ type: "wake_word-end", data: { wake_word_output: {} } }), null);
  assert.equal(
    describeEvent({ type: "stt-end", data: { stt_output: { text: "spiel Queen auf der Terrasse" } } }),
    'heard: "spiel Queen auf der Terrasse"',
  );
  assert.equal(
    describeEvent({
      type: "intent-end",
      data: { intent_output: { response: { response_type: "action_done", speech: { plain: { speech: "Ok" } } } } },
    }),
    'intent: action_done — "Ok"',
  );
  assert.equal(describeEvent({ type: "error", data: { code: "wake-word-timeout" } }), null);
  assert.match(describeEvent({ type: "error", data: { code: "stt-no-text-recognized", message: "x" } }), /stt-no-text/);
});

test("config: satellite off by default, defaults for wake word", () => {
  const base = { EUFY_EMAIL: "x@y.z", EUFY_PASSWORD: "pw" };
  const off = loadConfig(base).cfg.assist;
  assert.equal(off.enabled, false);
  assert.equal(off.wakeWordId, "hey_jarvis");
  assert.equal(off.wakeWordEntity, "wake_word.openwakeword");
  assert.equal(off.pipelineId, undefined);
  assert.equal(off.wsUrl, "ws://supervisor/core/websocket");
  const on = loadConfig({ ...base, ASSIST_SATELLITE: "true", ASSIST_CAMERA_SN: " SN1 ", ASSIST_PIPELINE_ID: "" }).cfg
    .assist;
  assert.equal(on.enabled, true);
  assert.equal(on.cameraSn, "SN1");
  assert.equal(on.pipelineId, undefined);
});

class FakeWs {
  static last;
  constructor(url) {
    this.url = url;
    this.readyState = 1;
    this.sent = [];
    this.listeners = {};
    this.bufferedAmount = 0;
    FakeWs.last = this;
  }
  addEventListener(type, fn) {
    (this.listeners[type] ??= []).push(fn);
  }
  send(data) {
    this.sent.push(typeof data === "string" ? JSON.parse(data) : Buffer.from(data));
  }
  close() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    for (const fn of this.listeners.close ?? []) fn({});
  }
  recv(obj) {
    for (const fn of this.listeners.message ?? []) fn({ data: JSON.stringify(obj) });
  }
  json() {
    return this.sent.filter((m) => !Buffer.isBuffer(m));
  }
  binary() {
    return this.sent.filter((m) => Buffer.isBuffer(m));
  }
}

const tick = () => new Promise((r) => setImmediate(r));

test("satellite: auth, run, audio framing, stt end-of-stream, next run", async () => {
  const lines = [];
  let pushAudio;
  const audio = {
    async *[Symbol.asyncIterator]() {
      while (true) {
        const chunk = await new Promise((r) => (pushAudio = r));
        if (!chunk) return;
        yield chunk;
      }
    },
  };
  const sat = createAssistSatellite({
    cfg: { selfHost: "127.0.0.1", port: 3000 },
    sn: "SN1",
    token: "secret-token",
    WebSocketImpl: FakeWs,
    openAudio: async () => audio,
    log: (m) => lines.push(m),
  });
  try {
    const ws = FakeWs.last;
    assert.equal(ws.url, "ws://supervisor/core/websocket");
    ws.recv({ type: "auth_required" });
    assert.deepEqual(ws.json()[0], { type: "auth", access_token: "secret-token" });
    ws.recv({ type: "auth_ok" });
    const [, getMsg, runMsg] = ws.json();
    assert.equal(getMsg.type, "assist_pipeline/pipeline/get");
    assert.equal(runMsg.type, "assist_pipeline/run");
    ws.recv({
      id: getMsg.id,
      type: "result",
      success: true,
      result: { name: "Jarvis", wake_word_entity: "wake_word.openwakeword", wake_word_id: "hey_jarvis" },
    });
    ws.recv({ id: runMsg.id, type: "result", success: true, result: null });

    await tick();
    pushAudio(Buffer.alloc(1000, 1)); // before run-start: dropped
    await tick();
    assert.equal(ws.binary().length, 0);

    ws.recv({
      id: runMsg.id,
      type: "event",
      event: { type: "run-start", data: { runner_data: { stt_binary_handler_id: 7 } } },
    });
    pushAudio(Buffer.alloc(3000, 2)); // 1000 carried + 3000 → one 2048-byte frame + remainder
    await tick();
    pushAudio(Buffer.alloc(1200, 3));
    await tick();
    const frames = ws.binary();
    assert.ok(frames.length >= 1);
    for (const f of frames) {
      assert.equal(f[0], 7);
      assert.equal(f.length, 2049);
    }

    ws.recv({
      id: runMsg.id,
      type: "event",
      event: { type: "wake_word-end", data: { wake_word_output: { wake_word_id: "hey_jarvis" } } },
    });
    ws.recv({
      id: runMsg.id,
      type: "event",
      event: { type: "stt-end", data: { stt_output: { text: "spiel Queen" } } },
    });
    const last = ws.binary().at(-1);
    assert.deepEqual([...last], [7]); // end of audio stream
    const before = ws.binary().length;
    pushAudio(Buffer.alloc(4096, 4)); // after stt-end: not streamed
    await tick();
    assert.equal(ws.binary().length, before);

    ws.recv({
      id: runMsg.id,
      type: "event",
      event: { type: "intent-end", data: { intent_output: { response: { response_type: "action_done" } } } },
    });
    ws.recv({ id: runMsg.id, type: "event", event: { type: "run-end", data: null } });
    const next = ws.json().at(-1);
    assert.equal(next.type, "assist_pipeline/run");
    assert.notEqual(next.id, runMsg.id);

    assert.ok(lines.includes("wake word detected: hey_jarvis"));
    assert.ok(lines.includes('heard: "spiel Queen"'));
    assert.ok(lines.includes("intent: action_done"));
    assert.ok(!lines.some((l) => l.includes("secret-token")));
  } finally {
    sat.stop();
    pushAudio?.(null);
  }
});

test("satellite: routine wake-word timeout restarts the run without a log line", async () => {
  const lines = [];
  const sat = createAssistSatellite({
    cfg: {},
    sn: "SN1",
    token: "t",
    WebSocketImpl: FakeWs,
    openAudio: () => new Promise(() => {}),
    log: (m) => lines.push(m),
  });
  try {
    const ws = FakeWs.last;
    ws.recv({ type: "auth_ok" });
    const run = ws.json().find((m) => m.type === "assist_pipeline/run");
    const count = lines.length;
    ws.recv({ id: run.id, type: "event", event: { type: "error", data: { code: "wake-word-timeout", message: "x" } } });
    ws.recv({ id: run.id, type: "event", event: { type: "run-end", data: null } });
    assert.equal(lines.length, count);
    const runs = ws.json().filter((m) => m.type === "assist_pipeline/run");
    assert.equal(runs.length, 2);
  } finally {
    sat.stop();
  }
});
