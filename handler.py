"""
RunPod Serverless Handler: FFmpeg Video Stitcher with Audio Overlay

Uses pure FFmpeg (no MoviePy) - simpler and more reliable.
Supports optional audio track muxing for narration/music overlay.

Dockerfile requirements:
- Python 3.10+
- ffmpeg (system package)
- runpod>=1.3.0
- requests>=2.28.0
"""

import os
import tempfile
import subprocess
import requests
import runpod
import base64
from typing import List, Optional


def download_file(url: str, temp_dir: str, prefix: str, index: int = 0) -> str:
    """Download a file from URL to a temporary file."""
    print(f"[{prefix}{index}] Downloading: {url[:80]}...")
    
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    
    # Detect extension from URL or content-type
    content_type = response.headers.get("content-type", "")
    
    if "audio" in content_type or any(ext in url.lower() for ext in [".wav", ".mp3", ".aac", ".m4a"]):
        if ".wav" in url.lower() or "wav" in content_type:
            ext = ".wav"
        elif ".mp3" in url.lower() or "mp3" in content_type:
            ext = ".mp3"
        elif ".m4a" in url.lower() or "m4a" in content_type:
            ext = ".m4a"
        else:
            ext = ".aac"
    elif "webm" in content_type or url.endswith(".webm"):
        ext = ".webm"
    else:
        ext = ".mp4"
    
    filepath = os.path.join(temp_dir, f"{prefix}_{index:03d}{ext}")
    
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = os.path.getsize(filepath)
    print(f"[{prefix}{index}] Downloaded: {file_size:,} bytes -> {filepath}")
    
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


def mux_audio_to_video(
    video_path: str, 
    audio_path: str, 
    output_path: str,
    audio_volume: float = 1.0,
    fade_out_seconds: float = 0.0
) -> dict:
    """
    Mux audio track into video using FFmpeg.
    
    Args:
        video_path: Path to input video
        audio_path: Path to audio file (wav, mp3, aac, m4a)
        output_path: Path for output video with audio
        audio_volume: Volume multiplier (1.0 = original, 0.5 = half)
        fade_out_seconds: Apply fade out at end (0 = no fade)
    
    Returns:
        dict with success status and metadata
    """
    print(f"Muxing audio into video...")
    print(f"  Video: {video_path}")
    print(f"  Audio: {audio_path}")
    
    video_duration = get_video_duration(video_path)
    
    # Build audio filter chain
    audio_filters = []
    
    # Volume adjustment
    if audio_volume != 1.0:
        audio_filters.append(f"volume={audio_volume}")
    
    # Fade out at end of video
    if fade_out_seconds > 0 and video_duration > fade_out_seconds:
        fade_start = video_duration - fade_out_seconds
        audio_filters.append(f"afade=t=out:st={fade_start}:d={fade_out_seconds}")
    
    # Build FFmpeg command
    # -shortest ensures output matches shorter of video/audio
    # -map 0:v takes video from first input
    # -map 1:a takes audio from second input
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", audio_path,
        "-map", "0:v:0",      # Video from first input
        "-map", "1:a:0",      # Audio from second input
        "-c:v", "copy",       # Copy video codec (no re-encode)
        "-c:a", "aac",        # Encode audio to AAC
        "-b:a", "192k",       # Audio bitrate
        "-shortest",          # Match video duration
    ]
    
    # Add audio filter if needed
    if audio_filters:
        cmd.extend(["-af", ",".join(audio_filters)])
    
    cmd.append(output_path)
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"FFmpeg mux stderr: {result.stderr}")
        
        # Fallback: try with video re-encoding
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", audio_path,
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-b:a", "192k",
            "-shortest",
        ]
        if audio_filters:
            cmd.extend(["-af", ",".join(audio_filters)])
        cmd.append(output_path)
        
        print(f"Retrying with video re-encode: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"FFmpeg mux failed: {result.stderr}")
    
    output_size = os.path.getsize(output_path)
    final_duration = get_video_duration(output_path)
    
    print(f"Muxed output: {output_size:,} bytes, {final_duration:.2f}s")
    
    return {
        "success": True,
        "duration": final_duration,
        "file_size_bytes": output_size,
        "has_audio": True
    }


def handler(job: dict) -> dict:
    """
    RunPod handler for video stitching with optional audio overlay.
    
    Input:
        segments: List[str] - URLs of video segments to concatenate
        audio_url: str (optional) - URL of audio file to overlay (narration/music)
        audio_volume: float (optional) - Volume multiplier, default 1.0
        fade_out: float (optional) - Fade out duration in seconds, default 0
        output_format: str - Output format (mp4 or webm), default: mp4
        
    Output:
        video_base64: str - Base64-encoded video with data URI prefix
        duration: float - Total duration in seconds
        file_size_bytes: int - Output file size
        has_audio: bool - Whether audio was muxed
    """
    job_input = job.get("input", {})
    
    # Validate input
    segments = job_input.get("segments", [])
    if not segments or len(segments) < 2:
        return {"error": "At least 2 video segment URLs are required"}
    
    output_format = job_input.get("output_format", "mp4")
    audio_url = job_input.get("audio_url")
    audio_volume = float(job_input.get("audio_volume", 1.0))
    fade_out = float(job_input.get("fade_out", 0.0))
    
    print(f"Job received: {len(segments)} segments, format={output_format}")
    if audio_url:
        print(f"Audio overlay requested: {audio_url[:80]}...")
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix="stitch_")
    stitched_path = os.path.join(temp_dir, f"stitched.{output_format}")
    final_path = os.path.join(temp_dir, f"final.{output_format}")
    
    try:
        # Download all video segments
        segment_paths = []
        for i, url in enumerate(segments):
            path = download_file(url, temp_dir, "segment", i)
            segment_paths.append(path)
        
        # Stitch videos
        stitch_result = stitch_videos_ffmpeg(segment_paths, stitched_path)
        
        if not stitch_result.get("success"):
            return {"error": stitch_result.get("error", "Stitching failed")}
        
        # Mux audio if provided
        has_audio = False
        output_path = stitched_path
        
        if audio_url:
            try:
                audio_path = download_file(audio_url, temp_dir, "audio", 0)
                mux_result = mux_audio_to_video(
                    stitched_path, 
                    audio_path, 
                    final_path,
                    audio_volume=audio_volume,
                    fade_out_seconds=fade_out
                )
                if mux_result.get("success"):
                    output_path = final_path
                    has_audio = True
                    print("Audio muxing successful")
                else:
                    print(f"Audio muxing failed, returning video without audio")
            except Exception as audio_err:
                print(f"Audio muxing error: {str(audio_err)}")
                print("Continuing with video-only output")
        
        # Read output and encode as base64
        with open(output_path, "rb") as f:
            video_bytes = f.read()
        
        print(f"Encoding {len(video_bytes):,} bytes as base64...")
        
        mime_type = "video/webm" if output_format == "webm" else "video/mp4"
        video_base64 = base64.b64encode(video_bytes).decode("utf-8")
        
        final_duration = get_video_duration(output_path)
        final_size = os.path.getsize(output_path)
        
        return {
            "video_base64": f"data:{mime_type};base64,{video_base64}",
            "duration": final_duration,
            "file_size_bytes": final_size,
            "segments_count": stitch_result["segments_count"],
            "format": output_format,
            "has_audio": has_audio
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
