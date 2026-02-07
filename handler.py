"""
RunPod Serverless Handler: MoviePy Video Stitcher

Deploy this to RunPod as a custom serverless endpoint.
It downloads video segments from URLs and concatenates them using MoviePy.
"""

import os
import tempfile
import requests
import runpod
from moviepy.editor import VideoFileClip, concatenate_videoclips
from typing import List
import base64


def download_video(url: str, temp_dir: str, index: int) -> str:
    """Download a video from URL to a temporary file."""
    print(f"[{index}] Downloading: {url[:80]}...")
    
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    
    content_type = response.headers.get("content-type", "video/mp4")
    ext = ".webm" if "webm" in content_type or url.endswith(".webm") else ".mp4"
    
    filepath = os.path.join(temp_dir, f"segment_{index:03d}{ext}")
    
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = os.path.getsize(filepath)
    print(f"[{index}] Downloaded: {file_size:,} bytes -> {filepath}")
    
    return filepath


def stitch_videos(segment_paths: List[str], output_path: str) -> dict:
    """Concatenate video segments using MoviePy."""
    print(f"Stitching {len(segment_paths)} segments...")
    
    clips = []
    total_duration = 0
    
    try:
        for i, path in enumerate(segment_paths):
            print(f"Loading clip {i+1}: {path}")
            clip = VideoFileClip(path)
            print(f"  -> Duration: {clip.duration:.2f}s, Size: {clip.size}")
            clips.append(clip)
            total_duration += clip.duration
        
        print(f"Concatenating... Total duration: {total_duration:.2f}s")
        final_clip = concatenate_videoclips(clips, method="compose")
        
        print(f"Writing output to: {output_path}")
        
        if output_path.endswith(".webm"):
            final_clip.write_videofile(
                output_path,
                codec="libvpx",
                audio_codec="libvorbis",
                fps=30,
                preset="medium",
                threads=4,
                logger=None
            )
        else:
            final_clip.write_videofile(
                output_path,
                codec="libx264",
                audio_codec="aac",
                fps=30,
                preset="medium",
                threads=4,
                logger=None
            )
        
        output_size = os.path.getsize(output_path)
        print(f"Output written: {output_size:,} bytes")
        
        return {
            "success": True,
            "duration": final_clip.duration,
            "size": final_clip.size,
            "file_size_bytes": output_size,
            "segments_count": len(clips)
        }
        
    finally:
        for clip in clips:
            try:
                clip.close()
            except:
                pass
        if 'final_clip' in locals():
            try:
                final_clip.close()
            except:
                pass


def handler(job: dict) -> dict:
    """RunPod handler for video stitching."""
    job_input = job.get("input", {})
    
    segments = job_input.get("segments", [])
    if not segments or len(segments) < 2:
        return {"error": "At least 2 video segment URLs are required"}
    
    output_format = job_input.get("output_format", "mp4")
    
    print(f"Job received: {len(segments)} segments, format={output_format}")
    
    temp_dir = tempfile.mkdtemp(prefix="stitch_")
    output_path = os.path.join(temp_dir, f"stitched.{output_format}")
    
    try:
        segment_paths = []
        for i, url in enumerate(segments):
            path = download_video(url, temp_dir, i)
            segment_paths.append(path)
        
        result = stitch_videos(segment_paths, output_path)
        
        if not result.get("success"):
            return {"error": result.get("error", "Stitching failed")}
        
        with open(output_path, "rb") as f:
            video_bytes = f.read()
        
        print(f"Encoding {len(video_bytes):,} bytes as base64...")
        
        mime_type = "video/webm" if output_format == "webm" else "video/mp4"
        video_base64 = base64.b64encode(video_bytes).decode("utf-8")
        
        return {
            "video_base64": f"data:{mime_type};base64,{video_base64}",
            "duration": result["duration"],
            "fil
