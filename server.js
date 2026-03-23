import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

const app = express();
app.use(express.json({ limit: "2mb" }));

const WORKER_SECRET = process.env.WORKER_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const VIDEO_MODEL = process.env.OPENAI_VIDEO_MODEL || "sora-2";
const VIDEO_SIZE = process.env.OPENAI_VIDEO_SIZE || "1280x720";
const DEFAULT_SCENE_SECONDS = Number(process.env.DEFAULT_SCENE_SECONDS || "4");
const MAX_SCENES_PER_EXPORT = Number(process.env.MAX_SCENES_PER_EXPORT || "4");
const VIDEO_POLL_INTERVAL_MS = Number(process.env.VIDEO_POLL_INTERVAL_MS || "5000");
const VIDEO_POLL_TIMEOUT_MS = Number(process.env.VIDEO_POLL_TIMEOUT_MS || "900000");

const BACKGROUND_MUSIC_PATH = process.env.BACKGROUND_MUSIC_PATH || "";
const BGM_VOLUME = Number(process.env.BGM_VOLUME || "0.12");
const TRANSITION_DURATION = Number(process.env.TRANSITION_DURATION || "0.4");
const ENABLE_SUBTITLES = String(process.env.ENABLE_SUBTITLES || "false") === "true";

const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ENABLE_GENERATED_SFX =
  String(process.env.ENABLE_GENERATED_SFX || "true") === "true";
const AUTO_GENERATE_SOUND_PLAN =
  String(process.env.AUTO_GENERATE_SOUND_PLAN || "true") === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

if (!OPENAI_API_KEY) {
  console.error("Missing OPENAI_API_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

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
  if (n >= 20) return "20";
  if (n >= 16) return "16";
  if (n >= 12) return "12";
  if (n >= 8) return "8";
  return "4";
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
          "Keep the same subject identity, same environment layout, same lighting, same color palette, and same overall style. Avoid sudden visual changes.",
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
        "Keep the same subject identity, same environment layout, same lighting, same color palette, and same overall style. Avoid sudden visual changes.",
      duration_seconds: toPositiveInt(
        scene?.duration_seconds,
        DEFAULT_SCENE_SECONDS
      ),
      dialogue: Array.isArray(scene?.dialogue) ? scene.dialogue : [],
      sound_effects: Array.isArray(scene?.sound_effects) ? scene.sound_effects : [],
      reference_image_url:
        typeof scene?.reference_image_url === "string"
          ? scene.reference_image_url
          : null,
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
        .map((fx) => fx.prompt)
        .join("; ")
    : "";

  return [
    "Create a cinematic AI video shot with natural synced audio.",
    "This should feel like a real video clip, not a slideshow, not a storyboard, and not a sequence of still images.",
    "",
    `SCENE TITLE: ${scene.title}`,
    `BASE SCENE: ${scene.base_prompt}`,
    `CONTINUITY RULES: ${scene.continuity_rules}`,
    `POSITION IN VIDEO: Scene ${sceneIndex + 1} of ${totalScenes}`,
    `POSITION HINT: ${positionHint}`,
    scene.narration ? `SCENE NARRATION CONTEXT: ${scene.narration}` : "",
    dialogueText ? `DIALOGUE:\n${dialogueText}` : "",
    sfxText ? `SOUND DESIGN INTENT: ${sfxText}` : "",
    "",
    "Audio requirements:",
    "- include realistic ambient sound appropriate to the setting",
    "- include subtle environmental motion sound",
    "- if people are present, include natural human movement sounds when appropriate",
    "- if there is dialogue, sync spoken dialogue naturally to the scene",
    "- avoid total silence unless the scene is intentionally quiet",
    "",
    "Visual requirements:",
    "- coherent natural motion",
    "- realistic camera behavior",
    "- stable subject identity",
    "- stable environment",
    "- stable lighting and color palette",
    "- cinematic composition",
    "- no captions or text on screen",
    "- no comic-book style",
    "- no storyboard style",
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

function selectVoiceForSpeaker(speaker) {
  const voices = ["alloy", "echo", "fable", "nova", "onyx", "shimmer"];
  const index = stableHash(speaker || "default") % voices.length;
  return voices[index];
}

async function synthesizeSpeechToFile({ text, voice = "alloy", outputPath }) {
  const speech = await openai.audio.speech.create({
    model: "gpt-4o-mini-tts",
    voice,
    input: text,
  });

  const buffer = Buffer.from(await speech.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
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

async function generateUniversalSoundPlan(scene) {
  if (!AUTO_GENERATE_SOUND_PLAN) {
    return [];
  }

  const prompt = [
    "Create a compact sound design plan for one cinematic video scene.",
    "Return JSON only.",
    "Return an array of 1 to 3 sound effects.",
    "Each sound effect must have:",
    "- prompt",
    "- start_seconds",
    "- duration_seconds",
    "- volume",
    "The prompts should describe realistic cinematic sound effects or ambience.",
    "Do not invent dialogue.",
    "",
    `Scene title: ${scene.title || ""}`,
    `Scene base prompt: ${scene.base_prompt || ""}`,
    `Scene narration: ${scene.narration || ""}`,
    `Duration seconds: ${scene.duration_seconds || DEFAULT_SCENE_SECONDS}`,
  ].join("\n");

  try {
    const response = await openai.responses.create({
      model: "gpt-4.1-mini",
      input: prompt,
      text: {
        format: {
          type: "json_schema",
          name: "sound_plan",
          schema: {
            type: "object",
            additionalProperties: false,
            properties: {
              sound_effects: {
                type: "array",
                maxItems: 3,
                items: {
                  type: "object",
                  additionalProperties: false,
                  properties: {
                    prompt: { type: "string" },
                    start_seconds: { type: "number" },
                    duration_seconds: { type: "number" },
                    volume: { type: "number" },
                  },
                  required: [
                    "prompt",
                    "start_seconds",
                    "duration_seconds",
                    "volume",
                  ],
                },
              },
            },
            required: ["sound_effects"],
          },
        },
      },
    });

    const raw = response.output_text || "{}";
    const parsed = JSON.parse(raw);
    const effects = Array.isArray(parsed?.sound_effects) ? parsed.sound_effects : [];

    return effects.map((fx) => ({
      prompt: String(fx.prompt || "").trim(),
      start_seconds: Math.max(0, Number(fx.start_seconds) || 0),
      duration_seconds: Math.max(
        1,
        Math.min(8, Number(fx.duration_seconds) || 3)
      ),
      volume: Math.max(0.1, Math.min(1.2, Number(fx.volume) || 0.8)),
    }));
  } catch (err) {
    console.error("Auto sound plan generation failed:", err?.message || err);
    return [];
  }
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

  if (
    sceneText.includes("explosion") ||
    sceneText.includes("blast")
  ) {
    effects.push({
      prompt: "loud cinematic explosion with echo and debris",
      start_seconds: 0,
      duration_seconds: 3,
      volume: 1,
    });
  }

  if (
    sceneText.includes("door") ||
    sceneText.includes("open") ||
    sceneText.includes("enter")
  ) {
    effects.push({
      prompt: "door opening sound with subtle room ambience shift",
      start_seconds: 0.5,
      duration_seconds: 2,
      volume: 0.9,
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
  if (!ELEVENLABS_API_KEY) {
    throw new Error("Missing ELEVENLABS_API_KEY");
  }

  const res = await fetch("https://api.elevenlabs.io/v1/sound-generation", {
    method: "POST",
    headers: {
      "xi-api-key": ELEVENLABS_API_KEY,
      "Content-Type": "application/json",
      "Accept": "audio/mpeg",
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

  if (!lines.length) {
    return silentBase;
  }

  const usableLines = lines.filter(
    (line) => typeof line?.text === "string" && line.text.trim()
  );

  if (!usableLines.length) {
    return silentBase;
  }

  const inputArgs = ["-i", silentBase];
  const filterParts = ["[0:a]volume=1.0[a0]"];
  const mixInputs = ["[a0]"];

  for (let i = 0; i < usableLines.length; i++) {
    const line = usableLines[i];
    const speechPath = path.join(tmpDir, `scene-${sceneIndex}-dialogue-${i}.mp3`);
    const speaker = line.speaker || `Speaker ${i + 1}`;
    const voice = line.voice || selectVoiceForSpeaker(speaker);

    await synthesizeSpeechToFile({
      text: line.text || "",
      voice,
      outputPath: speechPath,
    });

    inputArgs.push("-i", speechPath);

    const delayMs = Math.max(
      0,
      Math.floor((Number(line.start_seconds) || 0) * 1000)
    );
    const volume = Number(line.volume ?? 1);

    filterParts.push(
      `[${i + 1}:a]adelay=${delayMs}|${delayMs},volume=${volume}[a${i + 1}]`
    );
    mixInputs.push(`[a${i + 1}]`);
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
  const savedEffects = Array.isArray(scene?.sound_effects) ? scene.sound_effects : [];
  const autoEffects = savedEffects.length
    ? []
    : await generateUniversalSoundPlan(scene);

  const effects = savedEffects.length
    ? savedEffects
    : autoEffects.length
    ? autoEffects
    : buildSimpleFallbackSoundEffects(scene);

  console.log(
    `Scene ${sceneIndex} using ${savedEffects.length ? "saved" : autoEffects.length ? "auto-generated" : "fallback"} sound effects:`,
    JSON.stringify(effects, null, 2)
  );
  console.log("ELEVENLABS KEY EXISTS:", !!ELEVENLABS_API_KEY);

  const durationSeconds = Number(scene?.duration_seconds || DEFAULT_SCENE_SECONDS);

  const silentBase = path.join(tmpDir, `scene-${sceneIndex}-sfx-base.m4a`);
  await createSilentAudio(silentBase, durationSeconds);

  if (!effects.length) {
    console.log(`No usable sound effects for scene ${sceneIndex}`);
    return silentBase;
  }

  const usableEffects = [];

  for (let i = 0; i < effects.length; i++) {
    const fx = effects[i];

    if (
      ENABLE_GENERATED_SFX &&
      typeof fx?.prompt === "string" &&
      fx.prompt.trim()
    ) {
      const generatedPath = path.join(
        tmpDir,
        `scene-${sceneIndex}-generated-sfx-${i}.mp3`
      );

      console.log(`Generating SFX for scene ${sceneIndex}, effect ${i}:`, fx.prompt);

      await generateSoundEffectToFile({
        prompt: fx.prompt,
        outputPath: generatedPath,
        durationSeconds: Math.max(
          1,
          Math.min(8, Number(fx.duration_seconds) || 3)
        ),
      });

      console.log(`Generated SFX path: ${generatedPath}`);
      console.log(`Generated SFX exists:`, fs.existsSync(generatedPath));
      if (fs.existsSync(generatedPath)) {
        console.log(`Generated SFX bytes:`, fs.statSync(generatedPath).size);
      }

      usableEffects.push({
        ...fx,
        resolvedPath: generatedPath,
      });
      continue;
    }
  }

  if (!usableEffects.length) {
    console.log(`No usable sound effects for scene ${sceneIndex}`);
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

  console.log(`Built mixed SFX track for scene ${sceneIndex}: ${outputPath}`);
  if (fs.existsSync(outputPath)) {
    console.log(`Mixed SFX bytes:`, fs.statSync(outputPath).size);
  }

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
      "[0:a]volume=1.0[scene];[1:a]volume=0.4[narr];[scene][narr]amix=inputs=2:duration=longest:dropout_transition=0[aout]",
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
      `[0:a]volume=0.9[videoaud];` +
        `[1:a]volume=0.9[main];` +
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
      `[0:a]volume=0.9[videoaud];` +
        `[1:a]volume=0.9[main];` +
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

async function waitForVideoCompletion(videoId) {
  const startedAt = Date.now();
  let current = await openai.videos.retrieve(videoId);

  while (
    current.status === "queued" ||
    current.status === "in_progress" ||
    current.status === "processing"
  ) {
    if (Date.now() - startedAt > VIDEO_POLL_TIMEOUT_MS) {
      throw new Error(`Timed out waiting for video job ${videoId}`);
    }

    await sleep(VIDEO_POLL_INTERVAL_MS);
    current = await openai.videos.retrieve(videoId);
  }

  if (current.status === "failed") {
    const apiError = current.error?.message || "Unknown Sora video failure";
    throw new Error(`Video generation failed for ${videoId}: ${apiError}`);
  }

  if (current.status !== "completed") {
    throw new Error(
      `Unexpected video status for ${videoId}: ${current.status || "unknown"}`
    );
  }

  return current;
}

async function createSceneVideoClip({
  scene,
  sceneIndex,
  totalScenes,
  outputPath,
}) {
  const prompt = buildSceneVideoPrompt(scene, sceneIndex, totalScenes);

  const videoJob = await openai.videos.create({
    model: VIDEO_MODEL,
    prompt,
    seconds: normalizeVideoSeconds(
      scene.duration_seconds || DEFAULT_SCENE_SECONDS
    ),
    size: VIDEO_SIZE,
  });

  const completed = await waitForVideoCompletion(videoJob.id);
  const content = await openai.videos.downloadContent(completed.id);
  const arrayBuffer = await content.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);

  fs.writeFileSync(outputPath, buffer);
  return outputPath;
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

  console.log("Received render request for exportId:", exportId);
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

      console.log("Generating narration audio");

      narrationPath = path.join(tmpDir, "narration.mp3");
      const narrationText = getNarrationText(project, scenes);

      const narration = await openai.audio.speech.create({
        model: "gpt-4o-mini-tts",
        voice: "alloy",
        input: narrationText,
      });

      const audioBuffer = Buffer.from(await narration.arrayBuffer());
      fs.writeFileSync(narrationPath, audioBuffer);

      if (fs.existsSync(narrationPath)) {
        console.log("Narration bytes:", fs.statSync(narrationPath).size);
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

    console.log("Generating native AI video clips for scenes");

    const sceneClipPaths = [];

    for (let sceneIndex = 0; sceneIndex < scenes.length; sceneIndex++) {
      const scene = scenes[sceneIndex];
      const sceneClipPath = path.join(
        tmpDir,
        `scene-clip-${String(sceneIndex + 1).padStart(2, "0")}.mp4`
      );

      console.log(`Generating video for scene ${sceneIndex + 1}/${scenes.length}`);

      await createSceneVideoClip({
        scene,
        sceneIndex,
        totalScenes: scenes.length,
        outputPath: sceneClipPath,
      });

      const hasAudio = await checkIfVideoHasAudio(sceneClipPath);
      console.log(`Scene ${sceneIndex + 1} has native audio:`, hasAudio);

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

    console.log("Merging scene clips with transitions");

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

      console.log("Writing subtitles");
      writeSceneSubtitles(subtitlesPath, scenes, TRANSITION_DURATION);

      console.log("Burning subtitles");
      await applySubtitles(mergedScenesPath, subtitledVideoPath, subtitlesPath);
      videoForAudio = subtitledVideoPath;
    }

    await updateExport(exportId, {
      progress: 86,
      stage: "building_audio",
    });

    console.log("Building final audio");
    const builtAudioPath = await buildFinalAudioTrack({
      audioMode,
      narrationPath,
      scenes,
      tmpDir,
    });

    fs.copyFileSync(builtAudioPath, finalAudioPath);

    console.log("Final audio path:", finalAudioPath);
    console.log("Final audio exists:", fs.existsSync(finalAudioPath));
    if (fs.existsSync(finalAudioPath)) {
      console.log("Final audio bytes:", fs.statSync(finalAudioPath).size);
    }

    await updateExport(exportId, {
      progress: 92,
      stage: "adding_audio",
    });

    console.log("Merging final audio with optional background music");

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

    console.log("Uploading final video");

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

    console.log("Render completed:", exportId);
  } catch (e) {
    let message = e?.message || String(e);

    if (message.includes("Billing hard limit has been reached")) {
      message =
        "OpenAI billing limit reached. Increase your API billing limit or use a funded API key.";
    }

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
