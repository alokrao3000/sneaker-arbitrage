"""Process-wide runtime flags, importable from any layer without cycles.

shutdown_event is set exactly once, when the app begins shutting down
(app/services/scheduler.py:stop_scheduler). Long-running loops — supplier
scrapes, StockX lookup chunks, retry/backoff sleeps — check it so worker
threads drain within seconds instead of hanging interpreter exit (the
KeyboardInterrupt-during-thread-join tracebacks seen on Ctrl-C).
"""
import threading

shutdown_event = threading.Event()
