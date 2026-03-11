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

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });

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
        reject(new Error(`ffmpeg exited with code ${code}\nSTDERR:\n${stderr}`));
      }
    });
  });
}

async function updateExport(exportId, values) {
  const { error } = await supabase.from("exports").update(values).eq("id", exportId);
  if (error) {
    console.error("Failed to update export:", exportId, error.message);
  }
}

function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
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
        frames: [
          { frame_index: 0, delta_prompt: "Initial moment of the scene." },
          { frame_index: 1, delta_prompt: "A subtle continuation with slight motion." },
          { frame_index: 2, delta_prompt: "Another small motion progression in the same composition." },
          { frame_index: 3, delta_prompt: "Final moment of the scene with subtle progression." },
        ],
      },
    ];
  }

  return rawScenes.map((scene, sceneIndex) => {
    const durationSeconds = toPositiveInt(scene?.duration_seconds, 4);
    const fallbackFrameCount = toPositiveInt(scene?.frame_count, 4);

    let frames = Array.isArray(scene?.frames) ? scene.frames : [];

    if (!frames.length) {
      frames = Array.from({ length: fallbackFrameCount }).map((_, frameIndex) => ({
        frame_index: frameIndex,
        delta_prompt:
          frameIndex === 0
            ? "Initial moment of the scene."
            : frameIndex === fallbackFrameCount - 1
              ? "Final moment of the scene with subtle progression."
              : `A subtle continuation of the same scene, frame ${frameIndex + 1}.`,
      }));
    }

    frames = frames.map((frame, frameIndex) => ({
      frame_index: frameIndex,
      delta_prompt:
        frame?.delta_prompt ||
        `A subtle continuation of the same scene, frame ${frameIndex + 1}.`,
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
      duration_seconds: durationSeconds,
      frame_count: frames.length,
      frames,
    };
  });
}

function buildFramePrompt(scene, frame, isReferenceBased = false) {
  return [
    "Create a single frame from a cinematic AI video sequence.",
    "This frame must visually complement the other frames in the same scene.",
    isReferenceBased
      ? "Use the provided reference image to preserve the same subject identity, environment, framing, lighting, and overall composition."
      : "Establish the visual identity of the scene clearly and consistently.",
    "Do not create a comic panel, storyboard frame, split panel, or separate illustration.",
    "Maintain temporal continuity and near-identical scene identity across frames.",
    "",
    `SCENE TITLE: ${scene.title}`,
    `SCENE BASE: ${scene.base_prompt}`,
    `CONTINUITY RULES: ${scene.continuity_rules}`,
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
    "- no caption boxes",
    "- no split panels",
    isReferenceBased
      ? "- preserve the visual identity of the reference image while making only the requested subtle motion change"
      : "- establish a strong, stable visual identity that later frames can follow",
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

function computeImageProgress(completedFrames, totalFrames) {
  if (!totalFrames) return 20;
  return Math.min(20 + Math.floor((completedFrames / totalFrames) * 30), 50);
}

function computeSceneRenderProgress(completedScenes, totalScenes) {
  if (!totalScenes) return 50;
  return Math.min(50 + Math.floor((completedScenes / totalScenes) * 20), 70);
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

async function editImageWithReference({ referencePath, prompt, size = "1536x1024" }) {
  const form = new FormData();
  form.append("model", "gpt-image-1");
  form.append("prompt", prompt);
  form.append("size", size);

  const referenceBytes = fs.readFileSync(referencePath);
  const referenceBlob = new Blob([referenceBytes], { type: "image/png" });

  // Official Images edit endpoint
  form.append("image[]", referenceBlob, "reference.png");

  const response = await fetch("https://api.openai.com/v1/images/edits", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
    },
    body: form,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Image edit failed: ${response.status} ${text}`);
  }

  const json = await response.json();
  const imageBase64 = json?.data?.[0]?.b64_json;

  if (!imageBase64) {
    throw new Error("Image edit failed: no b64_json returned");
  }

  return Buffer.from(imageBase64, "base64");
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

  // Return immediately so the app doesn't wait on render completion.
  res.json({ ok: true });

  let tmpDir = null;

  try {
    await updateExport(exportId, {
      status: "rendering",
      progress: 5,
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

    await updateExport(exportId, { progress: 10 });

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "clippiant-"));

    const narrationPath = path.join(tmpDir, "narration.mp3");
    const combinedScenesPath = path.join(tmpDir, "combined-scenes.mp4");
    const finalVideoPath = path.join(tmpDir, `${exportId}.mp4`);

    console.log("Generating narration audio");

    const narration = await openai.audio.speech.create({
      model: "gpt-4o-mini-tts",
      voice: "alloy",
      input: narrationText,
    });

    const audioBuffer = Buffer.from(await narration.arrayBuffer());
    fs.writeFileSync(narrationPath, audioBuffer);

    await updateExport(exportId, { progress: 20 });

    console.log("Generating AI frame sequences for scenes");

    const sceneClipPaths = [];
    const totalFrames = getTotalFrames(scenes);
    let completedFrames = 0;

    for (let sceneIndex = 0; sceneIndex < scenes.length; sceneIndex++) {
      const scene = scenes[sceneIndex];
      const sceneDir = path.join(tmpDir, `scene-${sceneIndex + 1}`);
      fs.mkdirSync(sceneDir, { recursive: true });

      let referenceFramePath = null;

      for (let frameIndex = 0; frameIndex < scene.frames.length; frameIndex++) {
        const frame = scene.frames[frameIndex];
        const framePath = path.join(
          sceneDir,
          `frame_${String(frameIndex + 1).padStart(4, "0")}.png`
        );

        const isFirstFrame = frameIndex === 0;

        console.log(
          `Generating image for scene ${sceneIndex + 1}/${scenes.length}, frame ${frameIndex + 1}/${scene.frames.length}`
        );

        let imageBuffer;

        if (isFirstFrame) {
          const prompt = buildFramePrompt(scene, frame, false);
          console.log("Using base generation for first frame");
          console.log("Prompt used:", prompt);
          imageBuffer = await generateImageFromPrompt(prompt);
          fs.writeFileSync(framePath, imageBuffer);
          referenceFramePath = framePath;
        } else {
          const prompt = buildFramePrompt(scene, frame, true);
          console.log("Using reference-based edit for subsequent frame");
          console.log("Reference frame used:", referenceFramePath);
          console.log("Prompt used:", prompt);
          imageBuffer = await editImageWithReference({
            referencePath: referenceFramePath,
            prompt,
            size: "1536x1024",
          });
          fs.writeFileSync(framePath, imageBuffer);
        }

        console.log(`Saved frame: ${framePath}`);

        completedFrames += 1;
        await updateExport(exportId, {
          progress: computeImageProgress(completedFrames, totalFrames),
        });
      }

      console.log(`Rendering stop-motion clip for scene ${sceneIndex + 1}`);

      const sceneClipPath = path.join(tmpDir, `scene-${sceneIndex + 1}.mp4`);
      sceneClipPaths.push(sceneClipPath);

      const fps = Math.max(
        1,
        scene.frames.length / Math.max(1, scene.duration_seconds)
      );

      await runFfmpeg([
        "-y",
        "-framerate",
        String(fps),
        "-i",
        path.join(sceneDir, "frame_%04d.png"),
        "-vf",
        "scale=1280:720:force_original_aspect_ratio=increase,crop=1280:720,format=yuv420p",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-r",
        "30",
        sceneClipPath,
      ]);

      await updateExport(exportId, {
        progress: computeSceneRenderProgress(sceneIndex + 1, scenes.length),
      });
    }

    console.log("Concatenating scene clips");

    const listPath = path.join(tmpDir, "list.txt");
    fs.writeFileSync(
      listPath,
      sceneClipPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n")
    );

    await runFfmpeg([
      "-y",
      "-f",
      "concat",
      "-safe",
      "0",
      "-i",
      listPath,
      "-c",
      "copy",
      combinedScenesPath,
    ]);

    await updateExport(exportId, { progress: 75 });

    console.log("Merging narration with final video");

    await runFfmpeg([
      "-y",
      "-i",
      combinedScenesPath,
      "-i",
      narrationPath,
      "-c:v",
      "copy",
      "-c:a",
      "aac",
      "-shortest",
      finalVideoPath,
    ]);

    await updateExport(exportId, { progress: 90 });

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
      video_url: publicUrlData?.publicUrl || null,
      error: null,
    });

    console.log("Render completed:", exportId);
  } catch (e) {
    const message = e?.message || String(e);
    console.error("Render failed:", message);

    await updateExport(exportId, {
      status: "failed",
      error: message,
    });
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
