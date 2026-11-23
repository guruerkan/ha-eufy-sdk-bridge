// Wake-word satellite: turn one camera's microphone into a Home Assistant Assist voice input.
//
// The camera's audio is read from the bridge's OWN /audio/<sn> route (the exact source go2rtc pulls),
// which already normalises every eufy codec to signed 16-bit LE, 16 kHz, mono — the format Assist wants,
// so no extra transcoding happens here. The PCM is streamed to Home Assistant Core over its WebSocket API
// (`assist_pipeline/run`, start_stage "wake_word", end_stage "intent"). Each run waits for the wake word
// configured ON THE PIPELINE, transcribes the command and executes the intent; a new run starts as soon
// as the previous one ends. No reply audio is played through the camera.
//
// Protocol (HA assist_pipeline/websocket_api.py):
//   → {type:"auth", access_token}                       ← auth_ok
//   → {id, type:"assist_pipeline/run", start_stage:"wake_word", end_stage:"intent", input:{…}, pipeline?}
//   ← {id, type:"result", success:true}
//   ← event run-start   data.runner_data.stt_binary_handler_id = H
//   → binary frames [H, …pcm]   (a frame of just [H] ends the audio stream)
//   ← event wake_word-start / wake_word-end / stt-start / stt-end / intent-start / intent-end
//   ← event error {code, message}   (wake-word-timeout = routine: nothing was said)
//   ← event run-end                 → next run
//
// The token comes from SUPERVISOR_TOKEN and is never logged.

const RUN_WATCHDOG_MS = 330_000; // HA's own pipeline timeout is 300 s; past this the WS is presumed wedged
const FRAME_BYTES = 2048; // 64 ms of 16 kHz s16le mono per binary frame (even → sample aligned)
const MAX_WS_BUFFERED = 512 * 1024; // drop audio rather than queue unboundedly behind a slow socket
const QUIET_ERRORS = new Set(["wake-word-timeout"]);

/** The `assist_pipeline/run` command for one wake-word → intent run. */
export function buildRunMessage(id, { pipelineId, wakeTimeoutS = 60, noiseSuppressionLevel = 0 } = {}) {
  const msg = {
    id,
    type: "assist_pipeline/run",
    start_stage: "wake_word",
    end_stage: "intent",
    input: { sample_rate: 16000, timeout: wakeTimeoutS, noise_suppression_level: noiseSuppressionLevel },
  };
  if (pipelineId) msg.pipeline = pipelineId;
  return msg;
}

/** A binary WS frame: the run's stt_binary_handler_id byte followed by raw PCM. */
export function audioFrame(handlerId, pcm) {
  const frame = new Uint8Array(1 + (pcm?.length ?? 0));
  frame[0] = handlerId;
  if (pcm?.length) frame.set(pcm, 1);
  return frame;
}

/** Re-chunk an arbitrary byte stream into fixed, sample-aligned frames. */
export function createRechunker(size, onFrame) {
  let pending = Buffer.alloc(0);
  return (chunk) => {
    pending = pending.length ? Buffer.concat([pending, chunk]) : Buffer.from(chunk);
    while (pending.length >= size) {
      onFrame(pending.subarray(0, size));
      pending = pending.subarray(size);
    }
  };
}

const short = (s, n = 160) => {
  const t = String(s ?? "")
    .replace(/\s+/g, " ")
    .trim();
  return t.length > n ? `${t.slice(0, n)}…` : t;
};

/** One concise log line for a pipeline event, or null when it is not worth a line. */
export function describeEvent(event) {
  const data = event?.data ?? {};
  switch (event?.type) {
    case "wake_word-end": {
      const out = data.wake_word_output ?? {};
      if (!out.wake_word_id && !out.wake_word_phrase) return null;
      return `wake word detected: ${out.wake_word_phrase || out.wake_word_id}`;
    }
    case "stt-end":
      return `heard: "${short(data.stt_output?.text)}"`;
    case "intent-end": {
      const resp = data.intent_output?.response ?? {};
      const speech = resp.speech?.plain?.speech;
      const code = resp.data?.code;
      return `intent: ${resp.response_type ?? "?"}${code ? ` (${code})` : ""}${speech ? ` — "${short(speech)}"` : ""}`;
    }
    case "error":
      if (QUIET_ERRORS.has(data.code)) return null;
      return `pipeline error: ${data.code ?? "?"} — ${short(data.message)}`;
    default:
      return null;
  }
}

/**
 * Start the satellite. Returns { stop }. Dependencies are injectable for tests:
 *   openAudio(signal) → Promise<AsyncIterable<Uint8Array>>  (default: fetch the bridge's /audio/<sn>)
 *   WebSocketImpl                                            (default: Node's global WebSocket)
 */
export function createAssistSatellite({
  cfg,
  sn,
  token,
  wsUrl = "ws://supervisor/core/websocket",
  pipelineId,
  wakeWordId,
  wakeWordEntity,
  isReady = () => true,
  openAudio,
  WebSocketImpl = globalThis.WebSocket,
  log = (m) => console.log(`[bridge:assist] ${m}`),
  dbg = () => {},
  timers = { setTimeout, clearTimeout },
  backoff = { baseMs: 2_000, maxMs: 60_000, suspendedMs: 45_000, readyPollMs: 5_000 },
}) {
  if (!WebSocketImpl) throw new Error("no WebSocket implementation (Node >= 22 required)");
  if (!token) throw new Error("SUPERVISOR_TOKEN is not set (homeassistant_api must be enabled)");
  if (!sn) throw new Error("assist_camera_sn is required");

  let stopped = false;
  const sleepers = new Set();
  const sleep = (ms) =>
    new Promise((resolve) => {
      if (stopped) return resolve();
      const s = { resolve };
      s.t = timers.setTimeout(() => {
        sleepers.delete(s);
        resolve();
      }, ms);
      sleepers.add(s);
    });

  // ── Home Assistant side ──────────────────────────────────────────────────────────────────────────
  let ws;
  let nextId = 1;
  let run; // { id, handlerId, streaming, watchdog }
  let wsFailures = 0;
  let runFailures = 0;
  let checkedPipeline = false;
  const pending = new Map(); // id -> one-shot result handler

  const send = (obj) => ws?.readyState === 1 && ws.send(JSON.stringify(obj));

  function endAudio() {
    if (run?.streaming && run.handlerId != null && ws?.readyState === 1) ws.send(audioFrame(run.handlerId));
    if (run) run.streaming = false;
  }

  function finishRun(delayMs = 0) {
    if (!run) return;
    timers.clearTimeout(run.watchdog);
    run = undefined;
    if (delayMs) sleep(delayMs).then(startRun);
    else startRun();
  }

  function startRun() {
    if (stopped || run || ws?.readyState !== 1) return;
    const id = nextId++;
    const watchdog = timers.setTimeout(() => {
      log("run did not finish in time — reconnecting");
      ws?.close();
    }, RUN_WATCHDOG_MS);
    run = { id, handlerId: null, streaming: false, woke: false, watchdog };
    send(buildRunMessage(id, { pipelineId }));
  }

  function checkPipeline() {
    if (checkedPipeline) return;
    checkedPipeline = true;
    const id = nextId++;
    pending.set(id, (msg) => {
      if (!msg.success) return log(`could not read pipeline (${msg.error?.code ?? "?"}) — wake word not verified`);
      const p = msg.result ?? {};
      const entity = p.wake_word_entity ?? null;
      const word = p.wake_word_id ?? null;
      log(`pipeline "${p.name ?? p.id}" wake word: ${entity ?? "(default engine)"} / ${word ?? "(any)"}`);
      if ((wakeWordEntity && entity && entity !== wakeWordEntity) || (wakeWordId && word && word !== wakeWordId))
        log(
          `note: configured ${wakeWordEntity || "?"} / ${wakeWordId || "?"} differs — Assist always uses the PIPELINE's wake word; set it under Settings → Voice assistants`,
        );
      else if (!entity || !word)
        log(
          `note: set the pipeline's streaming wake word to ${wakeWordEntity || "?"} / ${wakeWordId || "?"} under Settings → Voice assistants`,
        );
    });
    send({ id, type: "assist_pipeline/pipeline/get", ...(pipelineId ? { pipeline_id: pipelineId } : {}) });
  }

  function onMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(typeof raw === "string" ? raw : Buffer.from(raw).toString("utf8"));
    } catch {
      return;
    }
    if (msg.type === "auth_required") return send({ type: "auth", access_token: token });
    if (msg.type === "auth_invalid") {
      log("Home Assistant rejected the add-on token");
      return ws?.close();
    }
    if (msg.type === "auth_ok") {
      wsFailures = 0;
      log("connected to Home Assistant — listening for the wake word");
      checkPipeline();
      return startRun();
    }
    if (msg.type === "result" && pending.has(msg.id)) {
      const handler = pending.get(msg.id);
      pending.delete(msg.id);
      return handler(msg);
    }
    if (!run || msg.id !== run.id) return; // a late event of an already-finished run
    if (msg.type === "result") {
      if (msg.success) return;
      runFailures++;
      log(`run rejected: ${msg.error?.code ?? "?"} — ${short(msg.error?.message)}`);
      return finishRun(Math.min(backoff.baseMs * 2 ** (runFailures - 1), backoff.maxMs));
    }
    if (msg.type !== "event") return;
    const event = msg.event ?? {};
    const line = describeEvent(event);
    if (line) log(line);
    else if (event.type === "error") dbg(`assist ${event.data?.code}`);
    switch (event.type) {
      case "run-start": {
        const h = event.data?.runner_data?.stt_binary_handler_id;
        if (typeof h === "number") {
          run.handlerId = h;
          run.streaming = true;
        }
        break;
      }
      case "wake_word-end": {
        const out = event.data?.wake_word_output ?? {};
        if (out.wake_word_id || out.wake_word_phrase) {
          run.woke = true;
          runFailures = 0;
        }
        break;
      }
      case "stt-end":
        endAudio(); // the command is transcribed; nothing more to stream for this run
        break;
      case "error":
        endAudio();
        // Only a run that fails BEFORE hearing anything (no wake engine, STT provider missing, …) is a
        // configuration problem worth backing off from; a timeout or a misheard command is not.
        if (QUIET_ERRORS.has(event.data?.code) || run.woke) runFailures = 0;
        else runFailures++;
        break;
      case "run-end":
        endAudio();
        finishRun(runFailures ? Math.min(backoff.baseMs * 2 ** (runFailures - 1), backoff.maxMs) : 0);
        break;
    }
  }

  async function wsLoop() {
    while (!stopped) {
      await new Promise((resolve) => {
        let socket;
        try {
          socket = new WebSocketImpl(wsUrl);
        } catch (e) {
          log(`websocket failed: ${e?.message ?? e}`);
          return resolve();
        }
        ws = socket;
        socket.binaryType = "arraybuffer";
        socket.addEventListener("message", (ev) => onMessage(ev.data));
        socket.addEventListener("error", () => {});
        socket.addEventListener("close", () => {
          if (ws === socket) ws = undefined;
          if (run) timers.clearTimeout(run.watchdog);
          run = undefined;
          pending.clear();
          resolve();
        });
      });
      if (stopped) return;
      wsFailures++;
      const wait = Math.min(backoff.baseMs * 2 ** (wsFailures - 1), backoff.maxMs);
      log(`Home Assistant connection closed — reconnecting in ${Math.round(wait / 1000)}s`);
      await sleep(wait);
    }
  }

  // ── camera side ──────────────────────────────────────────────────────────────────────────────────
  const forward = createRechunker(FRAME_BYTES, (pcm) => {
    if (!run?.streaming || run.handlerId == null || ws?.readyState !== 1) return; // between runs: drop
    if ((ws.bufferedAmount ?? 0) > MAX_WS_BUFFERED) return;
    ws.send(audioFrame(run.handlerId, pcm));
  });

  let audioAbort;
  const defaultOpenAudio = async (signal) => {
    const res = await fetch(`http://${cfg.selfHost}:${cfg.port}/audio/${encodeURIComponent(sn)}`, {
      signal,
      headers: { "user-agent": "eufy-bridge-assist-satellite" },
    });
    if (!res.ok || !res.body) {
      const err = new Error(`/audio answered HTTP ${res.status}`);
      err.status = res.status;
      await res.body?.cancel().catch(() => {});
      throw err;
    }
    return res.body;
  };
  const open = openAudio ?? defaultOpenAudio;

  async function audioLoop() {
    let failures = 0;
    while (!stopped) {
      if (!isReady()) {
        await sleep(backoff.readyPollMs); // never open before login: a failed open arms the stream backoff
        continue;
      }
      audioAbort = new AbortController();
      let gotData = false;
      let wait;
      try {
        const body = await open(audioAbort.signal);
        log(`camera ${sn} audio connected`);
        for await (const chunk of body) {
          if (stopped) break;
          if (!gotData) {
            gotData = true;
            failures = 0;
          }
          forward(chunk);
        }
        if (!stopped) log(`camera ${sn} audio ended — reopening`);
      } catch (e) {
        if (stopped) return;
        log(`camera ${sn} audio unavailable: ${short(e?.message ?? e, 120)}`);
        // 503 = idle-suspended / backing off. Each request refreshes the "consumer still pulling" clock,
        // so stay away long enough for the suspension to lift on its own (SUSPEND_RELEASE_MS = 30 s).
        if (e?.status === 503) wait = backoff.suspendedMs;
      } finally {
        audioAbort.abort();
      }
      if (stopped) return;
      if (!gotData) failures++;
      wait ??= gotData ? backoff.baseMs : Math.min(backoff.baseMs * 2 ** Math.max(failures - 1, 0), backoff.maxMs);
      await sleep(wait);
    }
  }

  log(`enabled for camera ${sn}${pipelineId ? ` (pipeline ${pipelineId})` : " (preferred pipeline)"}`);
  void wsLoop();
  void audioLoop();

  return {
    stop() {
      stopped = true;
      audioAbort?.abort();
      for (const s of sleepers) {
        timers.clearTimeout(s.t);
        s.resolve();
      }
      sleepers.clear();
      if (run) timers.clearTimeout(run.watchdog);
      try {
        ws?.close();
      } catch {
        // best effort
      }
    },
  };
}
