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

// Optional polish controls
const BACKGROUND_MUSIC_PATH = process.env.BACKGROUND_MUSIC_PATH || "";
const BGM_VOLUME = Number(process.env.BGM_VOLUME || "0.12");
const TRANSITION_DURATION = Number(process.env.TRANSITION_DURATION || "0.4");

// IMPORTANT:
// If your exports table does not have a "stage" column yet, either:
// 1) add it in Supabase, or
// 2) remove all `stage:` fields from updateExport(...) calls below.

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

function defaultFramePrompt(index, count) {
  if (index === 0) return "Initial moment of the scene.";
  if (index === 1) return "A subtle continuation.";
  if (index === count - 1) return "Final moment of the scene.";
  return `Frame ${index + 1} continuation.`;
}

function makeDefaultFrames(count) {
  return Array.from({ length: count }).map((_, index) => ({
    frame_index: index,
    delta_prompt: defaultFramePrompt(index, count),
  }));
}

function normalizeScenes(project) {
  const rawScenes = Array.isArray(project?.scenes) ? project.scenes : [];

  if (!rawScenes.length) {
    return [
      {
        title: project?.title || "Scene 1",
        narration: project?.script || project?.title || "Clippiant video",
        base_prompt:
          "A cinematic realistic video scene with strong visual continuity, consistent subject identity, consistent environment, consistent lighting, and realistic video-frame style.",
        continuity_rules:
          "Keep the same subject identity, same environment layout, same camera angle, same lighting, same color palette, and same style across every frame. Do not make this look like a comic panel, storyboard, or separate illustration.",
        duration_seconds: 4,
        frame_count: 4,
        frames: makeDefaultFrames(4),
      },
    ];
  }

  return rawScenes.map((scene, sceneIndex) => {
    const initialCount = toPositiveInt(scene?.frame_count, 4);

    const rawFrames =
      Array.isArray(scene?.frames) && scene.frames.length > 0
        ? scene.frames
        : makeDefaultFrames(initialCount);

    const frames = rawFrames.map((frame, frameIndex, arr) => ({
      frame_index:
        typeof frame?.frame_index === "number" ? frame.frame_index : frameIndex,
      delta_prompt:
        typeof frame?.delta_prompt === "string" && frame.delta_prompt.trim()
          ? frame.delta_prompt
          : defaultFramePrompt(frameIndex, arr.length),
    }));

    return {
      title: scene?.title || `Scene ${sceneIndex + 1}`,
      narration:
        scene?.narration ||
        scene?.voiceover ||
        scene?.text ||
        `Scene ${sceneIndex + 1}`,
      base_prompt:
        scene?.base_prompt ||
        scene?.visual ||
        `A cinematic realistic video scene for ${scene?.title || `scene ${sceneIndex + 1}`}.`,
      continuity_rules:
        scene?.continuity_rules ||
        "Keep the same subject identity, same environment layout, same camera angle, same lighting, same color palette, and same style across every frame. Do not make this look like a comic panel, storyboard, or separate illustration.",
      duration_seconds: toPositiveInt(scene?.duration_seconds, 4),
      frame_count: frames.length,
      frames,
    };
  });
}

function buildFramePrompt(scene, frame, frameIndex, frameCount) {
  return [
    "Create a single frame from a cinematic AI video sequence.",
    "This frame must visually complement the other frames in the same scene.",
    "Do not create a comic panel, storyboard frame, split panel, or separate illustration.",
    "Maintain temporal continuity and near-identical scene identity across frames.",
    "",
    `SCENE TITLE: ${scene.title}`,
    `SCENE BASE: ${scene.base_prompt}`,
    `CONTINUITY RULES: ${scene.continuity_rules}`,
    `FRAME NUMBER: ${frameIndex + 1} of ${frameCount}`,
    `FRAME ACTION: ${frame.delta_prompt}`,
    "",
    "Requirements:",
    "- same subject identity",
    "- same environment layout",
    "- same camera angle unless explicitly changed slightly",
    "- same lighting and color palette",
    "- realistic cinematic frame",
    "- no text overlay",
    "- no comic-book look",
    "- no storyboard look",
    "- no split panels",
  ].join("\n");
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

function getTotalFrames(scenes) {
  return scenes.reduce((sum, scene) => {
    const count = Array.isArray(scene?.frames) ? scene.frames.length : 0;
    return sum + count;
  }, 0);
}

function computeFrameGenerationProgress(completedFrames, totalFrames) {
  if (!totalFrames) return 20;
  return Math.min(20 + Math.floor((completedFrames / totalFrames) * 25), 45);
}

function computeSceneRenderProgress(completedScenes, totalScenes) {
  if (!totalScenes) return 45;
  return Math.min(45 + Math.floor((completedScenes / totalScenes) * 20), 65);
}

async function generateImageFromPrompt(prompt) {
  const imageResult = await openai.images.generate({
    model: "gpt-image-1",
    prompt,
    size: "1536x1024",
  });

  const imageBase64 = imageResult.data?.[0]?.b64_json;
  if (!imageBase64) {
    throw new Error("Image generation failed: no b64_json returned");
  }

  return Buffer.from(imageBase64, "base64");
}

async function renderSceneClipFromFrames({
  sceneDir,
  outputPath,
  fps,
  durationSeconds,
}) {
  const filter = [
    // fit into 16:9
    "scale=1408:792:force_original_aspect_ratio=increase",
    "crop=1280:720",

    // gentle camera motion
    "zoompan=z='min(zoom+0.0008,1.06)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'",

    // interpolate between low-fps source frames into smoother motion
    "minterpolate=fps=24:mi_mode=mci:mc_mode=aobmc:me_mode=bidir",

    "format=yuv420p",
  ].join(",");

  await runFfmpeg([
    "-y",
    "-framerate",
    String(fps),
    "-i",
    path.join(sceneDir, "frame_%04d.png"),
    "-t",
    String(durationSeconds),
    "-vf",
    filter,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-r",
    "24",
    outputPath,
  ]);
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

  const args = ["-y"];
  for (const clip of sceneClipPaths) {
    args.push("-i", clip);
  }

  let filter = "";
  for (let i = 0; i < sceneClipPaths.length; i++) {
    filter += `[${i}:v]format=yuv420p,setpts=PTS-STARTPTS[v${i}];`;
  }

  let cumulativeOffset = durations[0] - transitionDuration;
  let currentLabel = "[v0]";

  for (let i = 1; i < sceneClipPaths.length; i++) {
    const nextLabel = `[v${i}]`;
    const outLabel = i === sceneClipPaths.length - 1 ? "[vout]" : `[vx${i}]`;

    filter += `${currentLabel}${nextLabel}xfade=transition=fade:duration=${transitionDuration}:offset=${cumulativeOffset}${outLabel};`;

    currentLabel = outLabel;
    cumulativeOffset += durations[i] - transitionDuration;
  }

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
  const lines = [];

  for (let i = 0; i < scenes.length; i++) {
    const scene = scenes[i];
    const subtitleText = String(scene?.narration || "").trim();

    if (subtitleText) {
      const start = cursor;
      const end = cursor + Number(scene.duration_seconds || 0);

      lines.push(
        `${lines.filter((x) => x === "").length + 1}`,
        `${srtTimestamp(start)} --> ${srtTimestamp(
          Math.max(start + 0.6, end - transitionDuration * 0.25)
        )}`,
        subtitleText,
        ""
      );
    }

    cursor += Number(scene.duration_seconds || 0);
    if (i < scenes.length - 1) {
      cursor -= transitionDuration;
    }
  }

  fs.writeFileSync(srtPath, lines.join("\n"), "utf8");
}

async function applySubtitles(inputPath, outputPath, srtPath) {
  const subtitlePathForFfmpeg = srtPath
    .replace(/\\/g, "/")
    .replace(/:/g, "\\:");

  await runFfmpeg([
    "-y",
    "-i",
    inputPath,
    "-vf",
    `subtitles='${subtitlePathForFfmpeg}':force_style='FontName=DejaVu Sans,FontSize=20,PrimaryColour=&HFFFFFF&,OutlineColour=&H000000&,BorderStyle=3,Outline=1,Shadow=0,MarginV=28'`,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-an",
    outputPath,
  ]);
}

async function mergeVideoWithNarrationAndMusic({
  videoPath,
  narrationPath,
  outputPath,
  backgroundMusicPath,
  bgmVolume,
}) {
  const hasMusic =
    backgroundMusicPath &&
    fs.existsSync(backgroundMusicPath) &&
    fs.statSync(backgroundMusicPath).isFile();

  if (!hasMusic) {
    await runFfmpeg([
      "-y",
      "-i",
      videoPath,
      "-i",
      narrationPath,
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
    return;
  }

  await runFfmpeg([
    "-y",
    "-i",
    videoPath,
    "-i",
    narrationPath,
    "-stream_loop",
    "-1",
    "-i",
    backgroundMusicPath,
    "-filter_complex",
    `[1:a]volume=1.0[narr];[2:a]volume=${bgmVolume}[bgm];[narr][bgm]amix=inputs=2:duration=first:dropout_transition=2[aout]`,
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

  // Respond immediately
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
      .select("id, title, script, scenes")
      .eq("id", job.project_id)
      .single();

    if (projectError || !project) {
      throw new Error(projectError?.message || "Project not found");
    }

    const scenes = normalizeScenes(project);
    const narrationText = getNarrationText(project, scenes);

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "clippiant-"));

    const narrationPath = path.join(tmpDir, "narration.mp3");
    const mergedScenesPath = path.join(tmpDir, "merged-scenes.mp4");
    const subtitledVideoPath = path.join(tmpDir, "subtitled-scenes.mp4");
    const finalVideoPath = path.join(tmpDir, `${exportId}.mp4`);
    const subtitlesPath = path.join(tmpDir, "subtitles.srt");

    await updateExport(exportId, {
      progress: 10,
      stage: "generating_narration",
    });

    console.log("Generating narration audio");

    const narration = await openai.audio.speech.create({
      model: "gpt-4o-mini-tts",
      voice: "alloy",
      input: narrationText,
    });

    const audioBuffer = Buffer.from(await narration.arrayBuffer());
    fs.writeFileSync(narrationPath, audioBuffer);

    await updateExport(exportId, {
      progress: 20,
      stage: "generating_frames",
    });

    console.log("Generating scene frames");

    const sceneClipPaths = [];
    const totalFrames = getTotalFrames(scenes);
    let completedFrames = 0;

    for (let sceneIndex = 0; sceneIndex < scenes.length; sceneIndex++) {
      const scene = scenes[sceneIndex];
      const sceneDir = path.join(tmpDir, `scene-${String(sceneIndex + 1).padStart(2, "0")}`);
      fs.mkdirSync(sceneDir, { recursive: true });

      const frameCount = Array.isArray(scene.frames) ? scene.frames.length : 0;

      for (let frameIndex = 0; frameIndex < frameCount; frameIndex++) {
        const frame = scene.frames[frameIndex];
        const framePath = path.join(
          sceneDir,
          `frame_${String(frameIndex + 1).padStart(4, "0")}.png`
        );

        const prompt = buildFramePrompt(scene, frame, frameIndex, frameCount);

        console.log(
          `Generating scene ${sceneIndex + 1}/${scenes.length}, frame ${frameIndex + 1}/${frameCount}`
        );

        const imageBuffer = await generateImageFromPrompt(prompt);
        fs.writeFileSync(framePath, imageBuffer);

        completedFrames += 1;
        await updateExport(exportId, {
          progress: computeFrameGenerationProgress(completedFrames, totalFrames),
          stage: "generating_frames",
        });
      }

      console.log(`Rendering video clip for scene ${sceneIndex + 1}`);

      const sceneClipPath = path.join(
        tmpDir,
        `scene-clip-${String(sceneIndex + 1).padStart(2, "0")}.mp4`
      );

      const fps = Math.max(
        1,
        frameCount / Math.max(1, Number(scene.duration_seconds || 4))
      );

      await renderSceneClipFromFrames({
        sceneDir,
        outputPath: sceneClipPath,
        fps,
        durationSeconds: Number(scene.duration_seconds || 4),
      });

      sceneClipPaths.push(sceneClipPath);

      await updateExport(exportId, {
        progress: computeSceneRenderProgress(sceneIndex + 1, scenes.length),
        stage: "rendering_scene_clips",
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

    await updateExport(exportId, {
      progress: 78,
      stage: "adding_subtitles",
    });

    console.log("Writing subtitles");
    writeSceneSubtitles(subtitlesPath, scenes, TRANSITION_DURATION);

    console.log("Burning subtitles");
    await applySubtitles(mergedScenesPath, subtitledVideoPath, subtitlesPath);

    await updateExport(exportId, {
      progress: 88,
      stage: "adding_audio",
    });

    console.log("Merging narration and optional background music");

    await mergeVideoWithNarrationAndMusic({
      videoPath: subtitledVideoPath,
      narrationPath,
      outputPath: finalVideoPath,
      backgroundMusicPath: BACKGROUND_MUSIC_PATH,
      bgmVolume: BGM_VOLUME,
    });

    await updateExport(exportId, {
      progress: 94,
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
      console.error("Failed to write failed status:", updateError?.message || updateError);
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
