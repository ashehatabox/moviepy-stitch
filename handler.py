"""
RunPod Serverless Handler: FFmpeg Video Stitcher

Uses pure FFmpeg (no MoviePy) - simpler and more reliable.
"""

import os
import tempfile
import subprocess
import requests
import runpod
import base64
from typing import List


def download_video(url: str, temp_dir: str, index: int) -> str:
    """Download a video from URL to a temporary file."""
    print(f"[{index}] Downloading: {url[:80]}...")
    
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    
    # Detect extension from URL or content-type
    content_type = response.headers.get("content-type", "video/mp4")
    ext = ".webm" if "webm" in content_type or url.endswith(".webm") else ".mp4"
    
    filepath = os.path.join(temp_dir, f"segment_{index:03d}{ext}")
    
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = os.path.getsize(filepath)
    print(f"[{index}] Downloaded: {file_size:,} bytes -> {filepath}")
    
    return filepath


def get_video_duration(filepath: str) -> float:
    """Get video duration using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return float(result.stdout.strip())
    except:
        return 0.0


def stitch_videos_ffmpeg(segment_paths: List[str], output_path: str) -> dict:
    """Concatenate video segments using FFmpeg concat demuxer."""
    print(f"Stitching {len(segment_paths)} segments with FFmpeg...")
    
    # Create concat file list
    temp_dir = os.path.dirname(output_path)
    concat_file = os.path.join(temp_dir, "concat_list.txt")
    
    with open(concat_file, "w") as f:
        for path in segment_paths:
            # FFmpeg concat requires escaped paths
            escaped_path = path.replace("'", "'\\''")
            f.write(f"file '{escaped_path}'\n")
    
    print(f"Concat file created: {concat_file}")
    
    # Run FFmpeg concat
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", concat_file,
        "-c", "copy",  # Stream copy (fast, no re-encoding)
        output_path
    ]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"FFmpeg stderr: {result.stderr}")
        # Try with re-encoding if stream copy fails
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_file,
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-b:a", "128k",
            output_path
        ]
        print(f"Retrying with re-encode: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"FFmpeg failed: {result.stderr}")
    
    output_size = os.path.getsize(output_path)
    duration = get_video_duration(output_path)
    
    print(f"Output: {output_size:,} bytes, {duration:.2f}s")
    
    return {
        "success": True,
        "duration": duration,
        "file_size_bytes": output_size,
        "segments_count": len(segment_paths)
    }


def handler(job: dict) -> dict:
    """
    RunPod handler for video stitching.
    
    Input:
        segments: List[str] - URLs of video segments to concatenate
        output_format: str - Output format (mp4 or webm), default: mp4
        
    Output:
        video_base64: str - Base64-encoded video with data URI prefix
        duration: float - Total duration in seconds
        file_size_bytes: int - Output file size
    """
    job_input = job.get("input", {})
    
    # Validate input
    segments = job_input.get("segments", [])
    if not segments or len(segments) < 2:
        return {"error": "At least 2 video segment URLs are required"}
    
    output_format = job_input.get("output_format", "mp4")
    
    print(f"Job received: {len(segments)} segments, format={output_format}")
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix="stitch_")
    output_path = os.path.join(temp_dir, f"stitched.{output_format}")
    
    try:
        # Download all segments
        segment_paths = []
        for i, url in enumerate(segments):
            path = download_video(url, temp_dir, i)
            segment_paths.append(path)
        
        # Stitch videos
        result = stitch_videos_ffmpeg(segment_paths, output_path)
        
        if not result.get("success"):
            return {"error": result.get("error", "Stitching failed")}
        
        # Read output and encode as base64
        with open(output_path, "rb") as f:
            video_bytes = f.read()
        
        print(f"Encoding {len(video_bytes):,} bytes as base64...")
        
        mime_type = "video/webm" if output_format == "webm" else "video/mp4"
        video_base64 = base64.b64encode(video_bytes).decode("utf-8")
        
        return {
            "video_base64": f"data:{mime_type};base64,{video_base64}",
            "duration": result["duration"],
            "file_size_bytes": result["file_size_bytes"],
            "segments_count": result["segments_count"],
            "format": output_format
        }
            
    except Exception as e:
        import traceback
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}
        
    finally:
        # Cleanup temp files
        import shutil
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass


# RunPod serverless entry point
runpod.serverless.start({"handler": handler})
