"""
Telemetry Listener — Main entry point.

Starts the 30Hz polling loop that:
  1. Reads ACC shared memory
  2. Feeds snapshots to the SessionManager (for event detection)
  3. Writes raw telemetry to CSV (Week 1) or publishes to Kafka (Week 2)
  4. Writes per-lap Parquet files via LapSegmenter

Usage:
    python -m src.listener.main

Press Ctrl+C to stop recording.
"""

import signal
import sys
import time
from datetime import datetime, timezone
from loguru import logger

from .acc_reader import ACCReader
from .session_manager import SessionManager
from .telemetry_publisher import CSVTelemetryPublisher
from .lap_segmenter import LapSegmenter
from config.settings import POLLING_INTERVAL_S, POLLING_RATE_HZ


def setup_logging():
    """Configure loguru for clean terminal output."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<7}</level> | {message}",
        level="INFO",
    )
    logger.add(
        "logs/listener_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="7 days",
        level="DEBUG",
    )


def main():
    """Main telemetry listener loop."""
    setup_logging()
    logger.info("=" * 60)
    logger.info("🏎️  ACC Telemetry Listener — Starting")
    logger.info(f"   Polling rate: {POLLING_RATE_HZ}Hz ({POLLING_INTERVAL_S*1000:.1f}ms)")
    logger.info("=" * 60)
    
    # Initialize components
    reader = ACCReader()
    session_mgr = SessionManager()
    publisher = CSVTelemetryPublisher()
    segmenter = None  # Created per session
    
    # Wire up session manager events → publisher + segmenter
    def on_session_start(event):
        nonlocal segmenter
        publisher.start_session(event.session_id)
        publisher.write_session_event(event)
        segmenter = LapSegmenter(session_id=event.session_id, platform="ACC")
    
    def on_session_end(event):
        nonlocal segmenter
        publisher.write_session_event(event)
        publisher.end_session()
        if segmenter:
            segmenter.close()
            segmenter = None
        reader.refresh_static()  # Force re-read static on next session
    
    session_mgr.on_session_start(on_session_start)
    session_mgr.on_session_end(on_session_end)
    session_mgr.on_lap_completed(lambda e: publisher.write_lap_completion(e))
    session_mgr.on_setup_snapshot(lambda e: publisher.write_setup_snapshot(e))
    
    # Graceful shutdown
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("\n⏹️  Stopping listener...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Stats tracking
    total_reads = 0
    missed_reads = 0
    last_stats_time = time.time()
    waiting_for_acc = True
    
    # ── Main polling loop ──
    try:
        while running:
            loop_start = time.perf_counter()
            
            snapshot = reader.read()
            
            if snapshot is None:
                if waiting_for_acc:
                    pass  # Silently wait
                elif not waiting_for_acc:
                    waiting_for_acc = True
                    logger.info("⏳ Waiting for ACC...")
                
                # Don't spin at 60Hz while waiting — back off to 1Hz
                time.sleep(1.0)
                continue
            
            if waiting_for_acc:
                logger.info("ACC detected — reading telemetry")
                waiting_for_acc = False
            
            # Feed to session manager (handles event detection)
            session_mgr.update(snapshot)
            
            # Write raw telemetry if session is active
            if session_mgr.is_session_active:
                publisher.write_telemetry(snapshot)
                if segmenter:
                    segmenter.add_sample(snapshot)
                total_reads += 1
            
            # Print stats every 10 seconds
            now = time.time()
            if now - last_stats_time >= 10.0 and session_mgr.is_session_active:
                rate = total_reads / (now - last_stats_time) if (now - last_stats_time) > 0 else 0
                logger.info(
                    f"  📊 {total_reads} reads | {rate:.1f} Hz | "
                    f"Lap {snapshot.completed_laps} | "
                    f"Fuel {snapshot.fuel_remaining:.1f}L | "
                    f"Speed {snapshot.speed_kmh:.0f} km/h"
                )
                total_reads = 0
                last_stats_time = now
            
            # Sleep to maintain target rate
            elapsed = time.perf_counter() - loop_start
            sleep_time = POLLING_INTERVAL_S - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                missed_reads += 1
    
    finally:
        # Clean shutdown
        if session_mgr.is_session_active:
            logger.info("Closing active session...")
            publisher.end_session()
            if segmenter:
                segmenter.close()
        
        reader.close()
        publisher.close()
        logger.info(f"Listener stopped. Missed reads: {missed_reads}")


if __name__ == "__main__":
    main()
