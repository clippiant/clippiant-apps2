import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json());

const WORKER_SECRET = process.env.WORKER_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireSecret(req, res) {
  if (!WORKER_SECRET) return res.status(500).json({ error: "Worker missing WORKER_SECRET" });
  if (req.headers["x-worker-secret"] !== WORKER_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}

app.post("/render", async (req, res) => {
  const authErr = requireSecret(req, res);
  if (authErr) return;

  const exportId = req.body?.exportId;
  if (!exportId) return res.status(400).json({ error: "Missing exportId" });

  // Respond immediately, do work async
  res.json({ ok: true });

  try {
    // mark rendering
    await supabase.from("exports").update({ status: "rendering", progress: 5, error: null }).eq("id", exportId);

    // fetch export job
    const { data: job } = await supabase.from("exports").select("*").eq("id", exportId).single();
    if (!job) throw new Error("Export job not found");

    // fetch project
    const { data: project } = await supabase
      .from("projects")
      .select("id,title,script,scenes")
      .eq("id", job.project_id)
      .single();

    if (!project) throw new Error("Project not found");

    const scenes = Array.isArray(project.scenes) ? project.scenes : [];
    const title = project.title || "Clippiant Video";

    await supabase.from("exports").update({ progress: 15 }).eq("id", exportId);

    // Create temp paths
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "clippiant-"));
    const outPath = path.join(tmpDir, `${exportId}.mp4`);

    // Build a simple slideshow with drawtext (no images)
    // Each scene lasts 3 seconds. Total = scenes*3. If no scenes, 1 scene.
    const sceneTexts = scenes.length
      ? scenes.map((s, i) => (s?.visual ? `Scene ${i + 1}: ${s.visual}` : `Scene ${i + 1}`))
      : [project.script || title];

    // Write a concat script? Instead we use one long color source and drawtext changing per time is hard.
    // So we generate segments and concat them.
    const segmentPaths = [];

    for (let i = 0; i < sceneTexts.length; i++) {
      const segPath = path.join(tmpDir, `seg-${i}.mp4`);
      segmentPaths.push(segPath);

const fontPath = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

const text = String(sceneTexts[i])
  .replace(/\\/g, "\\\\")
  .replace(/:/g, "\\:")
  .replace(/'/g, "\\\\'")
  .replace(/\[/g, "\\[")
  .replace(/\]/g, "\\]")
  .replace(/,/g, "\\,")
  .replace(/\n/g, " ");

const args = [
  "-y",
  "-f", "lavfi",
  "-i", "color=c=#0b0b0b:s=1280x720:d=3",
  "-vf",
  `drawtext=fontfile=${fontPath}:fontcolor=white:fontsize=42:x=(w-text_w)/2:y=(h-text_h)/2:text='${text}'`,
  "-c:v", "libx264",
  "-pix_fmt", "yuv420p",
  segPath
];

      await runFfmpeg(args);
      await supabase.from("exports").update({ progress: Math.min(15 + Math.floor(((i + 1) / sceneTexts.length) * 55), 70) }).eq("id", exportId);
    }

    // concat segments
    const listPath = path.join(tmpDir, "list.txt");
    fs.writeFileSync(listPath, segmentPaths.map(p => `file '${p.replace(/\\/g, "/")}'`).join("\n"));

    await runFfmpeg([
      "-y",
      "-f", "concat",
      "-safe", "0",
      "-i", listPath,
      "-c", "copy",
      outPath
    ]);

    await supabase.from("exports").update({ progress: 85 }).eq("id", exportId);

    // upload to Supabase Storage
    const fileBytes = fs.readFileSync(outPath);
    const storagePath = `${exportId}.mp4`;

    const { error: upErr } = await supabase.storage
      .from("exports")
      .upload(storagePath, fileBytes, { contentType: "video/mp4", upsert: true });

    if (upErr) throw new Error(`Upload failed: ${upErr.message}`);

    const { data: pub } = supabase.storage.from("exports").getPublicUrl(storagePath);
    const videoUrl = pub?.publicUrl;

    await supabase.from("exports").update({ status: "done", progress: 100, video_url: videoUrl }).eq("id", exportId);

    // cleanup
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch (e) {
    const msg = e?.message || String(e);
    await supabase.from("exports").update({ status: "failed", error: msg }).eq("id", exportId);
    console.error("Render failed:", msg);
  }
});

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

app.get("/", (_req, res) => res.send("clippiant-worker ok"));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Worker listening on ${port}`));
