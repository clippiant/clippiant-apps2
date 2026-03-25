import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";
import RunwayML from "@runwayml/sdk";
import { fal } from "@fal-ai/client";

const app = express();
app.use(express.json({ limit: "2mb" }));

const WORKER_SECRET = process.env.WORKER_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const RUNWAYML_API_SECRET = process.env.RUNWAYML_API_SECRET || "";
const FAL_KEY = process.env.FAL_KEY || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || "";

const DEFAULT_SCENE_SECONDS = Number(process.env.DEFAULT_SCENE_SECONDS || "4");
const MAX_SCENES_PER_EXPORT = Number(process.env.MAX_SCENES_PER_EXPORT || "4");

const RUNWAY_MODEL = process.env.RUNWAY_MODEL || "veo3.1";
const RUNWAY_RATIO = process.env.RUNWAY_RATIO || "1280:720";

const PIKA_ENDPOINT =
  process.env.PIKA_ENDPOINT || "fal-ai/pika/v2.1/text-to-video";
const PIKA_ASPECT_RATIO = process.env.PIKA_ASPECT_RATIO || "16:9";
const PIKA_RESOLUTION = process.env.PIKA_RESOLUTION || "720p";

const BACKGROUND_MUSIC_PATH = process.env.BACKGROUND_MUSIC_PATH || "";
const BGM_VOLUME = Number(process.env.BGM_VOLUME || "0.12");
const TRANSITION_DURATION = Number(process.env.TRANSITION_DURATION || "0.4");
const ENABLE_SUBTITLES = String(process.env.ENABLE_SUBTITLES || "false") === "true";

const ENABLE_GENERATED_SFX =
  String(process.env.ENABLE_GENERATED_SFX || "true") === "true";

const PROVIDER_CONFIG = {
  video: {
    primary: "runway",
    fallback: "pika",
  },
  voice: {
    primary: "openai",
    fallback: "elevenlabs",
  },
  sfx: {
    primary: "elevenlabs",
    fallback: "internal",
  },
};

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

if (!OPENAI_API_KEY) {
  console.error("Missing OPENAI_API_KEY");
  process.exit(1);
}

if (!RUNWAYML_API_SECRET && !FAL_KEY) {
  console.error("At least one video provider key is required: RUNWAYML_API_SECRET or FAL_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

const runway = RUNWAYML_API_SECRET
  ? new RunwayML({ apiKey: RUNWAYML_API_SECRET })
  : null;

if (FAL_KEY) {
  fal.config({ credentials: FAL_KEY });
}

function requireSecret(req, res) {
  if (!WORKER_SECRET) {
    res.status(500).json({ error: "Worker missing WORKER_SECRET" });
    return true;
  }

  if (req.headers["x-worker-secret"] !== WORKER_SECRET) {
    res.status(401).json({ error: "Unauthorized" });
    return true;
  }

  return false;
}

function runProcess(command, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";

    p.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    p.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    p.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(
          new Error(`${command} exited with code ${code}\nSTDERR:\n${stderr}`)
        );
      }
    });
  });
}

function runFfmpeg(args) {
  return runProcess("ffmpeg", args);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getMediaDuration(filePath) {
  const { stdout } = await runProcess("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ]);

  const value = Number(String(stdout).trim());
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Unable to read media duration for ${filePath}`);
  }

  return value;
}

async function checkIfVideoHasAudio(filePath) {
  try {
    const { stdout } = await runProcess("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "a",
      "-show_entries",
      "stream=codec_type",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      filePath,
    ]);

    return String(stdout).trim().includes("audio");
  } catch {
    return false;
  }
}

async function updateExport(exportId, values) {
  const { error } = await supabase.from("exports").update(values).eq("id", exportId);
  if (error) {
    throw new Error(`Failed to update export ${exportId}: ${error.message}`);
  }
}

function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function normalizeVideoSeconds(value) {
  const n = Number(value);
  if (n >= 10) return 10;
  if (n >= 8) return 8;
  if (n >= 6) return 6;
  if (n >= 4) return 4;
  return 4;
}

function normalizeScenes(project) {
  const rawScenes = Array.isArray(project?.scenes) ? project.scenes : [];

  if (!rawScenes.length) {
    return [
      {
        title: project?.title || "Scene 1",
        narration: project?.script || project?.title || "",
        base_prompt:
          "A cinematic realistic video scene with strong visual continuity, consistent subject identity, consistent environment, consistent lighting, and realistic motion.",
        continuity_rules:
          "Continue naturally from the previous scene. Keep the same subjects, environment, lighting, style, and motion continuity unless explicitly changed.",
        duration_seconds: DEFAULT_SCENE_SECONDS,
        dialogue: [],
        sound_effects: [],
      },
    ];
  }

  return rawScenes.map((scene, sceneIndex) => {
    return {
      title: scene?.title || `Scene ${sceneIndex + 1}`,
      narration:
        typeof scene?.narration === "string"
          ? scene.narration
          : typeof scene?.voiceover === "string"
          ? scene.voiceover
          : typeof scene?.text === "string"
          ? scene.text
          : "",
      base_prompt:
        scene?.base_prompt ||
        scene?.visual ||
        `A cinematic realistic video scene for ${scene?.title || `scene ${sceneIndex + 1}`}.`,
      continuity_rules:
        scene?.continuity_rules ||
        "Continue naturally from the previous scene. Keep the same subjects, environment, lighting, style, and motion continuity unless explicitly changed.",
      duration_seconds: toPositiveInt(
        scene?.duration_seconds,
        DEFAULT_SCENE_SECONDS
      ),
      dialogue: Array.isArray(scene?.dialogue) ? scene.dialogue : [],
      sound_effects: Array.isArray(scene?.sound_effects) ? scene.sound_effects : [],
    };
  });
}

function buildSceneVideoPrompt(scene, sceneIndex, totalScenes) {
  const positionHint =
    sceneIndex === 0
      ? "This is the opening scene of the video."
      : sceneIndex === totalScenes - 1
      ? "This is the closing scene of the video."
      : "This scene should feel like a natural continuation of the surrounding scenes.";

  const dialogueText = Array.isArray(scene?.dialogue)
    ? scene.dialogue
        .filter((line) => typeof line?.text === "string" && line.text.trim())
        .map((line, idx) => {
          const speaker = line.speaker || `Speaker ${idx + 1}`;
          return `${speaker}: "${line.text}"`;
        })
        .join("\n")
    : "";

  const sfxText = Array.isArray(scene?.sound_effects)
    ? scene.sound_effects
        .filter((fx) => typeof fx?.prompt === "string" && fx.prompt.trim())
        .map((fx) => `- ${fx.prompt}`)
        .join("\n")
    : "";

  return [
    "Create a cinematic AI video shot.",
    "This should feel like a real video clip, not a slideshow, not a storyboard, and not a sequence of still images.",
    "If a visible character speaks, their lip movements and facial performance should match the spoken line as closely as the model allows.",
    "If an on-screen action creates a sound, the timing of that sound should feel aligned with the action.",
    "",
    `SCENE TITLE: ${scene.title || ""}`,
    `BASE SCENE: ${scene.base_prompt || ""}`,
    `CONTINUITY RULES: ${scene.continuity_rules || ""}`,
    `POSITION IN VIDEO: Scene ${sceneIndex + 1} of ${totalScenes}`,
    `POSITION HINT: ${positionHint}`,
    scene.narration ? `SCENE CONTEXT: ${scene.narration}` : "",
    "",
    "DIALOGUE:",
    dialogueText || "No spoken dialogue.",
    "",
    "SOUND EFFECTS / ACTION AUDIO:",
    sfxText || "- natural ambient audio only",
    "",
    "Requirements:",
    "- cinematic composition",
    "- coherent natural motion",
    "- stable subject identity",
    "- stable environment",
    "- stable lighting and palette",
    "- no subtitles or on-screen text",
  ]
    .filter(Boolean)
    .join("\n");
}

function getNarrationText(project, scenes) {
  if (Array.isArray(scenes) && scenes.length) {
    const joined = scenes
      .map((scene) => scene?.narration)
      .filter(Boolean)
      .join(" ");

    if (joined.trim()) return joined;
  }

  return project?.script || project?.title || "Clippiant video";
}

function computeSceneGenerationProgress(completedScenes, totalScenes) {
  if (!totalScenes) return 20;
  return Math.min(20 + Math.floor((completedScenes / totalScenes) * 45), 65);
}

function getAudioMode(job, project) {
  const value = job?.audio_mode || project?.audio_mode || "narration";
  if (value === "narration" || value === "dialogue" || value === "both") {
    return value;
  }
  return "narration";
}

function shouldGenerateNarration(audioMode) {
  return audioMode === "narration" || audioMode === "both";
}

function shouldGenerateDialogue(audioMode) {
  return audioMode === "dialogue" || audioMode === "both";
}

function stableHash(str) {
  let hash = 0;
  const input = String(str || "");
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function selectOpenAIVoiceForSpeaker(speaker) {
  const voices = ["alloy", "echo", "fable", "nova", "onyx", "shimmer"];
  const index = stableHash(speaker || "default") % voices.length;
  return voices[index];
}

function chooseVideoProvider() {
  if (PROVIDER_CONFIG.video.primary === "runway" && runway) return "runway";
  if (PROVIDER_CONFIG.video.primary === "pika" && FAL_KEY) return "pika";
  if (PROVIDER_CONFIG.video.fallback === "runway" && runway) return "runway";
  if (PROVIDER_CONFIG.video.fallback === "pika" && FAL_KEY) return "pika";
  throw new Error("No configured video provider is available");
}

function chooseVoiceProvider() {
  if (PROVIDER_CONFIG.voice.primary === "openai" && OPENAI_API_KEY) return "openai";
  if (PROVIDER_CONFIG.voice.primary === "elevenlabs" && ELEVENLABS_API_KEY) return "elevenlabs";
  if (PROVIDER_CONFIG.voice.fallback === "openai" && OPENAI_API_KEY) return "openai";
  if (PROVIDER_CONFIG.voice.fallback === "elevenlabs" && ELEVENLABS_API_KEY) return "elevenlabs";
  throw new Error("No configured voice provider is available");
}

function chooseSfxProvider() {
  if (PROVIDER_CONFIG.sfx.primary === "elevenlabs" && ELEVENLABS_API_KEY) return "elevenlabs";
  return "internal";
}

async function downloadToFile(url, outputPath) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to download file: ${res.status}`);
  }
  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
}

function extractRunwayVideoUrl(task) {
  return (
    task?.output?.[0] ||
    task?.output?.video ||
    task?.output?.video_url ||
    task?.video_url ||
    task?.url ||
    null
  );
}

async function generateSceneVideoWithRunway({ scene, sceneIndex, totalScenes, outputPath }) {
  if (!runway) throw new Error("Runway provider not configured");

  const prompt = buildSceneVideoPrompt(scene, sceneIndex, totalScenes);
  const duration = Math.max(2, Math.min(10, normalizeVideoSeconds(scene.duration_seconds)));

  const task = await runway.textToVideo
    .create({
      model: RUNWAY_MODEL,
      promptText: prompt,
      ratio: RUNWAY_RATIO,
      duration,
    })
    .waitForTaskOutput();

  const videoUrl = extractRunwayVideoUrl(task);
  if (!videoUrl) {
    throw new Error("Runway completed without a usable output URL");
  }

  await downloadToFile(videoUrl, outputPath);
  return outputPath;
}

async function generateSceneVideoWithPika({ scene, sceneIndex, totalScenes, outputPath }) {
  if (!FAL_KEY) throw new Error("Pika/Fal provider not configured");

  const prompt = buildSceneVideoPrompt(scene, sceneIndex, totalScenes);
  const duration = Math.max(5, Math.min(10, Number(scene.duration_seconds) || 5));

  const result = await fal.subscribe(PIKA_ENDPOINT, {
    input: {
      prompt,
      aspect_ratio: PIKA_ASPECT_RATIO,
      resolution: PIKA_RESOLUTION,
      duration,
    },
    logs: true,
  });

  const videoUrl =
    result?.data?.video?.url ||
    result?.data?.videos?.[0]?.url ||
    result?.video?.url ||
    null;

  if (!videoUrl) {
    throw new Error("Pika completed without a usable output URL");
  }

  await downloadToFile(videoUrl, outputPath);
  return outputPath;
}

async function generateSceneVideo({ scene, sceneIndex, totalScenes, outputPath }) {
  const primary = chooseVideoProvider();
  const fallback =
    primary === "runway" ? "pika" : "runway";

  try {
    if (primary === "runway") {
      return await generateSceneVideoWithRunway({
        scene,
        sceneIndex,
        totalScenes,
        outputPath,
      });
    }

    return await generateSceneVideoWithPika({
      scene,
      sceneIndex,
      totalScenes,
      outputPath,
    });
  } catch (primaryErr) {
    console.error(`Primary video provider ${primary} failed:`, primaryErr?.message || primaryErr);

    if (fallback === "runway" && runway) {
      return await generateSceneVideoWithRunway({
        scene,
        sceneIndex,
        totalScenes,
        outputPath,
      });
    }

    if (fallback === "pika" && FAL_KEY) {
      return await generateSceneVideoWithPika({
        scene,
        sceneIndex,
        totalScenes,
        outputPath,
      });
    }

    throw primaryErr;
  }
}

async function synthesizeSpeechOpenAIToFile({
  text,
  voice = "alloy",
  outputPath,
}) {
  const speech = await openai.audio.speech.create({
    model: "gpt-4o-mini-tts",
    voice,
    input: text,
  });

  const buffer = Buffer.from(await speech.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
}

async function synthesizeSpeechElevenLabsToFile({
  text,
  outputPath,
}) {
  if (!ELEVENLABS_API_KEY || !ELEVENLABS_VOICE_ID) {
    throw new Error("ElevenLabs TTS requires ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID");
  }

  const res = await fetch(
    `https://api.elevenlabs.io/v1/text-to-speech/${ELEVENLABS_VOICE_ID}`,
    {
      method: "POST",
      headers: {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        Accept: "audio/mpeg",
      },
      body: JSON.stringify({
        text,
        model_id: "eleven_multilingual_v2",
      }),
    }
  );

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`ElevenLabs TTS failed: ${txt}`);
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
}

async function synthesizeSpeechToFile({ text, voice = "alloy", outputPath }) {
  const primary = chooseVoiceProvider();
  const fallback = primary === "openai" ? "elevenlabs" : "openai";

  try {
    if (primary === "openai") {
      return await synthesizeSpeechOpenAIToFile({ text, voice, outputPath });
    }

    return await synthesizeSpeechElevenLabsToFile({ text, outputPath });
  } catch (primaryErr) {
    console.error(`Primary voice provider ${primary} failed:`, primaryErr?.message || primaryErr);

    if (fallback === "openai") {
      return await synthesizeSpeechOpenAIToFile({ text, voice, outputPath });
    }

    return await synthesizeSpeechElevenLabsToFile({ text, outputPath });
  }
}

async function createSilentAudio(outputPath, durationSeconds) {
  await runFfmpeg([
    "-y",
    "-f",
    "lavfi",
    "-i",
    "anullsrc=channel_layout=stereo:sample_rate=44100",
    "-t",
    String(durationSeconds),
    "-c:a",
    "aac",
    outputPath,
  ]);
}

function buildSimpleFallbackSoundEffects(scene) {
  const sceneText = [
    scene?.title || "",
    scene?.base_prompt || "",
    scene?.narration || "",
  ]
    .join(" ")
    .toLowerCase();

  const effects = [];

  if (
    sceneText.includes("kitchen") ||
    sceneText.includes("cook") ||
    sceneText.includes("oven") ||
    sceneText.includes("cookie")
  ) {
    effects.push({
      prompt: "soft kitchen ambience, distant oven hum, subtle ceramic clink",
      start_seconds: 0,
      duration_seconds: 3,
      volume: 0.8,
    });
  }

  if (
    sceneText.includes("city") ||
    sceneText.includes("street") ||
    sceneText.includes("traffic")
  ) {
    effects.push({
      prompt: "soft city ambience, distant traffic, light urban atmosphere",
      start_seconds: 0,
      duration_seconds: 3,
      volume: 0.8,
    });
  }

  if (
    sceneText.includes("forest") ||
    sceneText.includes("woods") ||
    sceneText.includes("nature")
  ) {
    effects.push({
      prompt: "gentle forest ambience, birds, subtle wind through leaves",
      start_seconds: 0,
      duration_seconds: 3,
      volume: 0.8,
    });
  }

  if (!effects.length) {
    effects.push({
      prompt: "subtle cinematic ambient room tone with light movement",
      start_seconds: 0,
      duration_seconds: 3,
      volume: 0.8,
    });
  }

  return effects;
}

async function generateSoundEffectToFile({
  prompt,
  outputPath,
  durationSeconds = 3,
}) {
  const provider = chooseSfxProvider();

  if (provider !== "elevenlabs") {
    throw new Error("No external SFX provider configured");
  }

  const res = await fetch("https://api.elevenlabs.io/v1/sound-generation", {
    method: "POST",
    headers: {
      "xi-api-key": ELEVENLABS_API_KEY,
      "Content-Type": "application/json",
      Accept: "audio/mpeg",
    },
    body: JSON.stringify({
      text: prompt,
      duration_seconds: Math.max(1, Math.min(22, Math.round(durationSeconds))),
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`ElevenLabs SFX failed: ${text}`);
  }

  const arrayBuffer = await res.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  fs.writeFileSync(outputPath, buffer);
}

async function buildSceneDialogueTrack({
  scene,
  sceneIndex,
  tmpDir,
}) {
  const lines = Array.isArray(scene?.dialogue) ? scene.dialogue : [];
  const durationSeconds = Number(scene?.duration_seconds || DEFAULT_SCENE_SECONDS);

  const silentBase = path.join(tmpDir, `scene-${sceneIndex}-dialogue-base.m4a`);
  await createSilentAudio(silentBase, durationSeconds);

  if (!lines.length) return silentBase;

  const usableLines = lines.filter(
    (line) => typeof line?.text === "string" && line.text.trim()
  );

  if (!usableLines.length) return silentBase;

  const inputArgs = ["-i", silentBase];
  const filterParts = ["[0:a]volume=1.0[a0]"];
  const mixInputs = ["[a0]"];

  for (let i = 0; i < usableLines.length; i++) {
    const line = usableLines[i];
    const speechPath = path.join(tmpDir, `scene-${sceneIndex}-dialogue-${i}.mp3`);
    const speaker = line.speaker || `Speaker ${i + 1}`;
    const voice = line.voice || selectOpenAIVoiceForSpeaker(speaker);

    try {
      await synthesizeSpeechToFile({
        text: line.text || "",
        voice,
        outputPath: speechPath,
      });
    } catch (err) {
      console.error(`Skipping failed dialogue line ${i}:`, err?.message || err);
      continue;
    }

    inputArgs.push("-i", speechPath);

    const delayMs = Math.max(
      0,
      Math.floor((Number(line.start_seconds) || 0) * 1000)
    );
    const volume = Number(line.volume ?? 1);

    filterParts.push(
      `[${mixInputs.length}:a]adelay=${delayMs}|${delayMs},volume=${volume}[a${mixInputs.length}]`
    );
    mixInputs.push(`[a${mixInputs.length}]`);
  }

  if (mixInputs.length === 1) {
    return silentBase;
  }

  const outputPath = path.join(tmpDir, `scene-${sceneIndex}-dialogue-mixed.m4a`);
  const filterComplex = [
    ...filterParts,
    `${mixInputs.join("")}amix=inputs=${mixInputs.length}:duration=longest:dropout_transition=0[aout]`,
  ].join(";");

  await runFfmpeg([
    "-y",
    ...inputArgs,
    "-filter_complex",
    filterComplex,
    "-map",
    "[aout]",
    "-c:a",
    "aac",
    outputPath,
  ]);

  return outputPath;
}

async function buildSceneSfxTrack({
  scene,
  sceneIndex,
  tmpDir,
}) {
  const effects = Array.isArray(scene?.sound_effects) && scene.sound_effects.length
    ? scene.sound_effects
    : buildSimpleFallbackSoundEffects(scene);

  const durationSeconds = Number(scene?.duration_seconds || DEFAULT_SCENE_SECONDS);

  const silentBase = path.join(tmpDir, `scene-${sceneIndex}-sfx-base.m4a`);
  await createSilentAudio(silentBase, durationSeconds);

  if (!effects.length || !ENABLE_GENERATED_SFX) {
    return silentBase;
  }

  const usableEffects = [];

  for (let i = 0; i < effects.length; i++) {
    const fx = effects[i];
    if (!(typeof fx?.prompt === "string" && fx.prompt.trim())) continue;

    const generatedPath = path.join(
      tmpDir,
      `scene-${sceneIndex}-generated-sfx-${i}.mp3`
    );

    try {
      await generateSoundEffectToFile({
        prompt: fx.prompt,
        outputPath: generatedPath,
        durationSeconds: Math.max(
          1,
          Math.min(8, Number(fx.duration_seconds) || 3)
        ),
      });

      usableEffects.push({
        ...fx,
        resolvedPath: generatedPath,
      });
    } catch (err) {
      console.error(`Skipping failed SFX ${i}:`, err?.message || err);
    }
  }

  if (!usableEffects.length) {
    return silentBase;
  }

  const inputArgs = ["-i", silentBase];
  const filterParts = ["[0:a]volume=1.0[a0]"];
  const mixInputs = ["[a0]"];

  for (let i = 0; i < usableEffects.length; i++) {
    const fx = usableEffects[i];
    inputArgs.push("-i", fx.resolvedPath);

    const delayMs = Math.max(
      0,
      Math.floor((Number(fx.start_seconds) || 0) * 1000)
    );
    const volume = Number(fx.volume ?? 1);

    filterParts.push(
      `[${i + 1}:a]adelay=${delayMs}|${delayMs},volume=${volume}[a${i + 1}]`
    );
    mixInputs.push(`[a${i + 1}]`);
  }

  const outputPath = path.join(tmpDir, `scene-${sceneIndex}-sfx-mixed.m4a`);
  const filterComplex = [
    ...filterParts,
    `${mixInputs.join("")}amix=inputs=${mixInputs.length}:duration=longest:dropout_transition=0[aout]`,
  ].join(";");

  await runFfmpeg([
    "-y",
    ...inputArgs,
    "-filter_complex",
    filterComplex,
    "-map",
    "[aout]",
    "-c:a",
    "aac",
    outputPath,
  ]);

  return outputPath;
}

async function buildFinalAudioTrack({
  audioMode,
  narrationPath,
  scenes,
  tmpDir,
}) {
  const sceneAudioPaths = [];

  for (let i = 0; i < scenes.length; i++) {
    const scene = scenes[i];
    const durationSeconds = Number(scene?.duration_seconds || DEFAULT_SCENE_SECONDS);

    const dialoguePath = shouldGenerateDialogue(audioMode)
      ? await buildSceneDialogueTrack({ scene, sceneIndex: i, tmpDir })
      : null;

    const sfxPath = await buildSceneSfxTrack({ scene, sceneIndex: i, tmpDir });

    const sceneBase = path.join(tmpDir, `scene-${i}-audio-base.m4a`);
    await createSilentAudio(sceneBase, durationSeconds);

    const inputs = ["-i", sceneBase];
    const mixParts = ["[0:a]volume=1.0[a0]"];
    const mixInputs = ["[a0]"];

    let inputIndex = 1;

    if (dialoguePath) {
      inputs.push("-i", dialoguePath);
      mixParts.push(`[${inputIndex}:a]volume=1.0[a${inputIndex}]`);
      mixInputs.push(`[a${inputIndex}]`);
      inputIndex++;
    }

    if (sfxPath) {
      inputs.push("-i", sfxPath);
      mixParts.push(`[${inputIndex}:a]volume=1.0[a${inputIndex}]`);
      mixInputs.push(`[a${inputIndex}]`);
      inputIndex++;
    }

    const sceneOutput = path.join(tmpDir, `scene-${i}-final-audio.m4a`);
    const filterComplex = [
      ...mixParts,
      `${mixInputs.join("")}amix=inputs=${mixInputs.length}:duration=longest:dropout_transition=0[aout]`,
    ].join(";");

    await runFfmpeg([
      "-y",
      ...inputs,
      "-filter_complex",
      filterComplex,
      "-map",
      "[aout]",
      "-c:a",
      "aac",
      sceneOutput,
    ]);

    sceneAudioPaths.push(sceneOutput);
  }

  const concatList = path.join(tmpDir, "scene-audio-list.txt");
  fs.writeFileSync(
    concatList,
    sceneAudioPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n")
  );

  const combinedSceneAudio = path.join(tmpDir, "combined-scene-audio.m4a");

  await runFfmpeg([
    "-y",
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    concatList,
    "-c",
    "copy",
    combinedSceneAudio,
  ]);

  if (shouldGenerateNarration(audioMode) && narrationPath && fs.existsSync(narrationPath)) {
    const finalAudio = path.join(tmpDir, "final-audio.m4a");

    await runFfmpeg([
      "-y",
      "-i",
      combinedSceneAudio,
      "-i",
      narrationPath,
      "-filter_complex",
      "[0:a]volume=1.0[scene];[1:a]volume=0.35[narr];[scene][narr]amix=inputs=2:duration=longest:dropout_transition=0[aout]",
      "-map",
      "[aout]",
      "-c:a",
      "aac",
      finalAudio,
    ]);

    return finalAudio;
  }

  return combinedSceneAudio;
}

async function mergeSceneClipsWithTransitions(
  sceneClipPaths,
  outputPath,
  transitionDuration
) {
  if (sceneClipPaths.length === 1) {
    fs.copyFileSync(sceneClipPaths[0], outputPath);
    return;
  }

  const durations = [];
  for (const clip of sceneClipPaths) {
    durations.push(await getMediaDuration(clip));
  }

  const shortestClip = Math.min(...durations);
  const safeTransition = Math.min(
    transitionDuration,
    Math.max(0.1, shortestClip / 2)
  );

  const args = ["-y"];
  for (const clip of sceneClipPaths) {
    args.push("-i", clip);
  }

  const parts = [];

  for (let i = 0; i < sceneClipPaths.length; i++) {
    parts.push(`[${i}:v]format=yuv420p,setpts=PTS-STARTPTS[v${i}]`);
  }

  let cumulativeOffset = durations[0] - safeTransition;
  let currentLabel = "[v0]";

  for (let i = 1; i < sceneClipPaths.length; i++) {
    const nextLabel = `[v${i}]`;
    const outLabel = i === sceneClipPaths.length - 1 ? "[vout]" : `[vx${i}]`;

    parts.push(
      `${currentLabel}${nextLabel}xfade=transition=fade:duration=${safeTransition}:offset=${cumulativeOffset}${outLabel}`
    );

    currentLabel = outLabel;
    cumulativeOffset += durations[i] - safeTransition;
  }

  const filter = parts.join(";");

  await runFfmpeg([
    ...args,
    "-filter_complex",
    filter,
    "-map",
    currentLabel === "[vout]" ? "[vout]" : currentLabel,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    outputPath,
  ]);
}

function srtTimestamp(seconds) {
  const totalMs = Math.max(0, Math.floor(seconds * 1000));
  const hours = Math.floor(totalMs / 3600000);
  const minutes = Math.floor((totalMs % 3600000) / 60000);
  const secs = Math.floor((totalMs % 60000) / 1000);
  const ms = totalMs % 1000;

  return (
    [
      String(hours).padStart(2, "0"),
      String(minutes).padStart(2, "0"),
      String(secs).padStart(2, "0"),
    ].join(":") + `,${String(ms).padStart(3, "0")}`
  );
}

function writeSceneSubtitles(srtPath, scenes, transitionDuration) {
  let cursor = 0;
  const blocks = [];
  let counter = 1;

  for (let i = 0; i < scenes.length; i++) {
    const scene = scenes[i];
    const subtitleText = String(scene?.narration || "").trim();

    if (subtitleText) {
      const start = cursor;
      const end = cursor + Number(scene.duration_seconds || 0);

      blocks.push(
        `${counter}`,
        `${srtTimestamp(start)} --> ${srtTimestamp(
          Math.max(start + 0.6, end - transitionDuration * 0.25)
        )}`,
        subtitleText,
        ""
      );

      counter += 1;
    }

    cursor += Number(scene.duration_seconds || 0);
    if (i < scenes.length - 1) {
      cursor -= transitionDuration;
    }
  }

  fs.writeFileSync(srtPath, blocks.join("\n"), "utf8");
}

async function applySubtitles(inputPath, outputPath, srtPath) {
  if (!fs.existsSync(srtPath) || fs.statSync(srtPath).size === 0) {
    fs.copyFileSync(inputPath, outputPath);
    return;
  }

  const subtitlePathForFfmpeg = srtPath
    .replace(/\\/g, "/")
    .replace(/:/g, "\\:");

  const subtitleFilter =
    `subtitles=${subtitlePathForFfmpeg}` +
    `:force_style=FontName=DejaVuSans,FontSize=20,PrimaryColour=&HFFFFFF&,OutlineColour=&H000000&,BorderStyle=3,Outline=1,Shadow=0,MarginV=28`;

  await runFfmpeg([
    "-y",
    "-i",
    inputPath,
    "-vf",
    subtitleFilter,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-an",
    outputPath,
  ]);
}

async function mergeVideoWithFinalAudio({
  videoPath,
  finalAudioPath,
  outputPath,
  backgroundMusicPath,
  bgmVolume,
}) {
  const hasMusic =
    backgroundMusicPath &&
    fs.existsSync(backgroundMusicPath) &&
    fs.statSync(backgroundMusicPath).isFile();

  const videoHasAudio = await checkIfVideoHasAudio(videoPath);

  if (videoHasAudio && hasMusic) {
    await runFfmpeg([
      "-y",
      "-i",
      videoPath,
      "-i",
      finalAudioPath,
      "-stream_loop",
      "-1",
      "-i",
      backgroundMusicPath,
      "-filter_complex",
      `[0:a]volume=1.0[videoaud];` +
        `[1:a]volume=0.45[main];` +
        `[2:a]volume=${bgmVolume}[bgm];` +
        `[videoaud][main][bgm]amix=inputs=3:duration=first:dropout_transition=2[aout]`,
      "-map",
      "0:v:0",
      "-map",
      "[aout]",
      "-c:v",
      "copy",
      "-c:a",
      "aac",
      "-shortest",
      outputPath,
    ]);
    return;
  }

  if (videoHasAudio && !hasMusic) {
    await runFfmpeg([
      "-y",
      "-i",
      videoPath,
      "-i",
      finalAudioPath,
      "-filter_complex",
      `[0:a]volume=1.0[videoaud];` +
        `[1:a]volume=0.45[main];` +
        `[videoaud][main]amix=inputs=2:duration=first:dropout_transition=2[aout]`,
      "-map",
      "0:v:0",
      "-map",
      "[aout]",
      "-c:v",
      "copy",
      "-c:a",
      "aac",
      "-shortest",
      outputPath,
    ]);
    return;
  }

  if (!videoHasAudio && hasMusic) {
    await runFfmpeg([
      "-y",
      "-i",
      videoPath,
      "-i",
      finalAudioPath,
      "-stream_loop",
      "-1",
      "-i",
      backgroundMusicPath,
      "-filter_complex",
      `[1:a]volume=1.0[main];` +
        `[2:a]volume=${bgmVolume}[bgm];` +
        `[main][bgm]amix=inputs=2:duration=first:dropout_transition=2[aout]`,
      "-map",
      "0:v:0",
      "-map",
      "[aout]",
      "-c:v",
      "copy",
      "-c:a",
      "aac",
      "-shortest",
      outputPath,
    ]);
    return;
  }

  await runFfmpeg([
    "-y",
    "-i",
    videoPath,
    "-i",
    finalAudioPath,
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-shortest",
    outputPath,
  ]);
}

app.get("/", (_req, res) => {
  res.send("clippiant-worker ok");
});

app.post("/render", async (req, res) => {
  if (requireSecret(req, res)) return;

  const exportId = req.body?.exportId;
  if (!exportId) {
    return res.status(400).json({ error: "Missing exportId" });
  }

  res.json({ ok: true });

  let tmpDir = null;

  try {
    await updateExport(exportId, {
      status: "rendering",
      progress: 5,
      stage: "starting",
      error: null,
      video_url: null,
    });

    const { data: job, error: jobError } = await supabase
      .from("exports")
      .select("*")
      .eq("id", exportId)
      .single();

    if (jobError || !job) {
      throw new Error(jobError?.message || "Export job not found");
    }

    const { data: project, error: projectError } = await supabase
      .from("projects")
      .select("id, title, script, scenes, audio_mode")
      .eq("id", job.project_id)
      .single();

    if (projectError || !project) {
      throw new Error(projectError?.message || "Project not found");
    }

    let scenes = normalizeScenes(project);
    if (scenes.length > MAX_SCENES_PER_EXPORT) {
      scenes = scenes.slice(0, MAX_SCENES_PER_EXPORT);
    }

    const audioMode = getAudioMode(job, project);

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "clippiant-"));

    let narrationPath = null;
    const mergedScenesPath = path.join(tmpDir, "merged-scenes.mp4");
    const subtitledVideoPath = path.join(tmpDir, "subtitled-scenes.mp4");
    const finalAudioPath = path.join(tmpDir, "final-audio-or-dialogue.m4a");
    const finalVideoPath = path.join(tmpDir, `${exportId}.mp4`);
    const subtitlesPath = path.join(tmpDir, "subtitles.srt");

    if (shouldGenerateNarration(audioMode)) {
      await updateExport(exportId, {
        progress: 10,
        stage: "generating_narration",
      });

      narrationPath = path.join(tmpDir, "narration.mp3");
      const narrationText = getNarrationText(project, scenes);

      try {
        await synthesizeSpeechToFile({
          text: narrationText,
          voice: "alloy",
          outputPath: narrationPath,
        });
      } catch (err) {
        console.error("Narration failed, continuing without it:", err?.message || err);
        narrationPath = null;
      }
    } else {
      await updateExport(exportId, {
        progress: 10,
        stage: "audio_optional_skipped",
      });
    }

    await updateExport(exportId, {
      progress: 20,
      stage: "generating_scene_videos",
    });

    const sceneClipPaths = [];

    for (let sceneIndex = 0; sceneIndex < scenes.length; sceneIndex++) {
      const scene = scenes[sceneIndex];
      const sceneClipPath = path.join(
        tmpDir,
        `scene-clip-${String(sceneIndex + 1).padStart(2, "0")}.mp4`
      );

      await generateSceneVideo({
        scene,
        sceneIndex,
        totalScenes: scenes.length,
        outputPath: sceneClipPath,
      });

      sceneClipPaths.push(sceneClipPath);

      await updateExport(exportId, {
        progress: computeSceneGenerationProgress(sceneIndex + 1, scenes.length),
        stage: "generating_scene_videos",
      });
    }

    await updateExport(exportId, {
      progress: 70,
      stage: "merging_video",
    });

    await mergeSceneClipsWithTransitions(
      sceneClipPaths,
      mergedScenesPath,
      TRANSITION_DURATION
    );

    let videoForAudio = mergedScenesPath;

    if (ENABLE_SUBTITLES) {
      await updateExport(exportId, {
        progress: 78,
        stage: "adding_subtitles",
      });

      writeSceneSubtitles(subtitlesPath, scenes, TRANSITION_DURATION);
      await applySubtitles(mergedScenesPath, subtitledVideoPath, subtitlesPath);
      videoForAudio = subtitledVideoPath;
    }

    await updateExport(exportId, {
      progress: 86,
      stage: "building_audio",
    });

    const builtAudioPath = await buildFinalAudioTrack({
      audioMode,
      narrationPath,
      scenes,
      tmpDir,
    });

    fs.copyFileSync(builtAudioPath, finalAudioPath);

    await updateExport(exportId, {
      progress: 92,
      stage: "adding_audio",
    });

    await mergeVideoWithFinalAudio({
      videoPath: videoForAudio,
      finalAudioPath,
      outputPath: finalVideoPath,
      backgroundMusicPath: BACKGROUND_MUSIC_PATH,
      bgmVolume: BGM_VOLUME,
    });

    await updateExport(exportId, {
      progress: 96,
      stage: "uploading",
    });

    const fileBytes = fs.readFileSync(finalVideoPath);
    const storagePath = `${exportId}.mp4`;

    const { error: uploadError } = await supabase.storage
      .from("exports")
      .upload(storagePath, fileBytes, {
        contentType: "video/mp4",
        upsert: true,
      });

    if (uploadError) {
      throw new Error(`Upload failed: ${uploadError.message}`);
    }

    const { data: publicUrlData } = supabase.storage
      .from("exports")
      .getPublicUrl(storagePath);

    await updateExport(exportId, {
      status: "done",
      progress: 100,
      stage: "done",
      video_url: publicUrlData?.publicUrl || null,
      error: null,
    });
  } catch (e) {
    const message = e?.message || String(e);
    console.error("Render failed:", message);

    try {
      await updateExport(exportId, {
        status: "failed",
        stage: "failed",
        error: message,
      });
    } catch (updateError) {
      console.error(
        "Failed to write failed status:",
        updateError?.message || updateError
      );
    }
  } finally {
    if (tmpDir) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Worker listening on ${port}`);
});
