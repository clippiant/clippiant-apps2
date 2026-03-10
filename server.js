import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

const app = express();
app.use(express.json());

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
    await supabase
      .from("exports")
      .update({ status: "rendering", progress: 5, error: null })
      .eq("id", exportId);

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

    await supabase.from("exports").update({ progress: 10 }).eq("id", exportId);

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "clippiant-"));

    const narrationPath = path.join(tmpDir, "narration.mp3");
    const slideshowPath = path.join(tmpDir, "slideshow.mp4");
    const finalVideoPath = path.join(tmpDir, `${exportId}.mp4`);

    console.log("Generating narration audio");

    const narration = await openai.audio.speech.create({
      model: "gpt-4o-mini-tts",
      voice: "alloy",
      input: project.script || project.title || "Clippiant video",
    });

    const audioBuffer = Buffer.from(await narration.arrayBuffer());
    fs.writeFileSync(narrationPath, audioBuffer);

    await supabase.from("exports").update({ progress: 20 }).eq("id", exportId);

    const scenes = Array.isArray(project.scenes) ? project.scenes : [];
    const sceneTexts = scenes.length
      ? scenes.map((s, i) => {
          if (s?.visual) return `Scene ${i + 1}: ${s.visual}`;
          return `Scene ${i + 1}`;
        })
      : [project.script || project.title || "Clippiant video"];

    console.log("Generating AI images for scenes");

    const imagePaths = [];

    for (let i = 0; i < sceneTexts.length; i++) {
      const imagePath = path.join(tmpDir, `scene-${i}.png`);
      imagePaths.push(imagePath);

      const prompt = scenes[i]?.visual || sceneTexts[i];

      const imageResult = await openai.images.generate({
        model: "gpt-image-1",
        prompt: `Create a cinematic, high-quality visual for this video scene: ${prompt}`,
        size: "1536x1024",
      });

      const imageBase64 = imageResult.data?.[0]?.b64_json;
      if (!imageBase64) {
        throw new Error(`Image generation failed for scene ${i + 1}`);
      }

      fs.writeFileSync(imagePath, Buffer.from(imageBase64, "base64"));

      const progress = Math.min(
        20 + Math.floor(((i + 1) / sceneTexts.length) * 20),
        40
      );
      await supabase.from("exports").update({ progress }).eq("id", exportId);
    }

    console.log("Rendering scene video segments from AI images");

    const segmentPaths = [];
    const fontPath = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

    for (let i = 0; i < imagePaths.length; i++) {
      const segPath = path.join(tmpDir, `seg-${i}.mp4`);
      segmentPaths.push(segPath);

      const overlayText = String(sceneTexts[i])
        .replace(/\\/g, "\\\\")
        .replace(/:/g, "\\:")
        .replace(/'/g, "\\\\'")
        .replace(/\[/g, "\\[")
        .replace(/\]/g, "\\]")
        .replace(/,/g, "\\,")
        .replace(/\n/g, " ");

      await runFfmpeg([
        "-y",
        "-loop", "1",
        "-i", imagePaths[i],
        "-vf",
        `scale=1280:720,zoompan=z='min(zoom+0.0008,1.08)':d=90:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)',drawtext=fontfile=${fontPath}:fontcolor=white:fontsize=32:box=1:boxcolor=black@0.45:boxborderw=12:x=(w-text_w)/2:y=h-(text_h*2):text='${overlayText}'`,
        "-t", "3",
        "-r", "30",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        segPath,
      ]);

      const progress = Math.min(
        40 + Math.floor(((i + 1) / imagePaths.length) * 25),
        65
      );
      await supabase.from("exports").update({ progress }).eq("id", exportId);
    }

    console.log("Concatenating slideshow segments");

    const listPath = path.join(tmpDir, "list.txt");
    fs.writeFileSync(
      listPath,
      segmentPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n")
    );

    await runFfmpeg([
      "-y",
      "-f", "concat",
      "-safe", "0",
      "-i", listPath,
      "-c", "copy",
      slideshowPath,
    ]);

    await supabase.from("exports").update({ progress: 75 }).eq("id", exportId);

    console.log("Merging narration with slideshow");

    await runFfmpeg([
      "-y",
      "-i", slideshowPath,
      "-i", narrationPath,
      "-c:v", "copy",
      "-c:a", "aac",
      "-shortest",
      finalVideoPath,
    ]);

    await supabase.from("exports").update({ progress: 90 }).eq("id", exportId);

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

    await supabase
      .from("exports")
      .update({
        status: "done",
        progress: 100,
        video_url: publicUrlData.publicUrl,
      })
      .eq("id", exportId);

    console.log("Render completed:", exportId);
  } catch (e) {
    const message = e?.message || String(e);
    console.error("Render failed:", message);

    await supabase
      .from("exports")
      .update({
        status: "failed",
        error: message,
      })
      .eq("id", exportId);
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
