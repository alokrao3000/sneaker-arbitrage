"""
Shared stealth-browser session layer for scraping StockX/GOAT directly.

This masks *automation signals* (navigator.webdriver, missing plugins, the
absent `window.chrome` object) using a real Chromium instance via Playwright,
which also gives a genuine browser TLS/JA3 fingerprint for free — the main
edge over a plain httpx client hitting the same endpoints.

It does NOT solve *network-reputation* blocking. Cloudflare Bot Management
and PerimeterX both weigh the request's source IP heavily — a cloud/VM IP
range gets flagged regardless of how clean the browser fingerprint is. The
`proxy_url` parameter threads a residential/mobile proxy through end-to-end,
but is empty by default since none is configured yet. Expect meaningfully
worse reliability running headless on a server with no proxy than running
locally during development.

`use_stealth` exists per-platform because stealth patching isn't uniformly
safe: live testing found playwright-stealth's overrides collide with GOAT's
own bot-detection script (it reads `navigator.userAgent` in a way our patch
breaks), throwing a ReferenceError that crashes React hydration before any
page content loads. GOAT's session should be built with `use_stealth=False`.
StockX did not show this issue.

IMPORTANT thread-affinity note, confirmed live: starting a Playwright sync
session leaves that thread's asyncio state such that `asyncio.run()` calls
elsewhere in the SAME thread start raising "asyncio.run() cannot be called
from a running event loop" — even in code with no direct Playwright
involvement. `ShopifyScraper.scrape()` uses `asyncio.run()` internally, so
Playwright-based scraping must never share a thread with it. Use
`ThreadBoundProxy` (below) to confine all Playwright activity to one
dedicated worker thread for the life of a scrape run.
"""
import concurrent.futures
import json
import logging
from pathlib import Path
from typing import Callable, Optional

from playwright.sync_api import sync_playwright, Page, BrowserContext, Response

try:
    from playwright_stealth import stealth_sync
except ImportError:  # pragma: no cover - fallback if the package is ever removed/broken
    stealth_sync = None

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Hand-rolled fallback masking, used only if playwright-stealth is unavailable.
_MANUAL_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""


class BrowserSession:
    """One persistent Chromium context for a single platform (e.g. "stockx").

    Held for the lifetime of a scrape run — reopening a full browser per SKU
    would be slow and would defeat storage-state (cookie) reuse mid-run.

    Playwright's sync API only supports one `sync_playwright()` driver
    instance per thread at a time — starting a second one while the first is
    still open raises a misleading "inside the asyncio loop" error (confirmed
    live: it's actually just this single-instance-per-thread limitation, not
    an asyncio conflict). Whenever more than one platform is needed in the
    same thread (e.g. StockX + GOAT together), create ONE shared Playwright
    instance via `sync_playwright().start()` and pass it to each BrowserSession
    as `playwright=`; each session will then skip owning/stopping it.
    """

    def __init__(
        self,
        platform: str,
        headless: bool = True,
        proxy_url: str = "",
        state_dir: str = "data/browser_state",
        nav_timeout_ms: int = 30000,
        use_stealth: bool = True,
        playwright=None,
    ):
        self.platform = platform
        self.headless = headless
        self.proxy_url = proxy_url
        self.nav_timeout_ms = nav_timeout_ms
        self.use_stealth = use_stealth
        self._state_path = Path(state_dir) / f"{platform}.json"

        self._playwright = playwright
        self._owns_playwright = playwright is None
        self._browser = None
        self._context: Optional[BrowserContext] = None

    def start(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)

        if self._playwright is None:
            self._playwright = sync_playwright().start()
            self._owns_playwright = True
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )

        context_kwargs = {
            "user_agent": _USER_AGENT,
            "viewport": {"width": 1920, "height": 1080},
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }
        if self._state_path.exists():
            context_kwargs["storage_state"] = str(self._state_path)
        if self.proxy_url:
            context_kwargs["proxy"] = {"server": self.proxy_url}

        self._context = self._browser.new_context(**context_kwargs)
        self._context.set_default_navigation_timeout(self.nav_timeout_ms)
        logger.info(
            f"[{self.platform}] browser session started "
            f"(headless={self.headless}, proxy={'set' if self.proxy_url else 'none'}, "
            f"resumed_state={self._state_path.exists()})"
        )

    def new_page(self) -> Page:
        page = self._context.new_page()
        if self.use_stealth:
            if stealth_sync is not None:
                stealth_sync(page)
            else:
                page.add_init_script(_MANUAL_STEALTH_SCRIPT)
        return page

    def save_state(self) -> None:
        if self._context is None:
            return
        try:
            self._context.storage_state(path=str(self._state_path))
        except Exception:
            logger.exception(f"[{self.platform}] failed to persist browser storage state")

    def close(self) -> None:
        self.save_state()
        try:
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
            if self._owns_playwright and self._playwright:
                self._playwright.stop()
        except Exception:
            logger.exception(f"[{self.platform}] error tearing down browser session")

    def __enter__(self) -> "BrowserSession":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def capture_json_response(
    page: Page,
    url_predicate: Callable[[str], bool],
    trigger: Callable[[], None],
    timeout_ms: int = 15000,
) -> Optional[dict]:
    """Run `trigger()` (e.g. a page.goto) while watching for the first response
    whose URL matches `url_predicate`; return its parsed JSON body, or None if
    no matching response arrives (or it isn't valid JSON) within timeout_ms.
    """
    result: dict = {}

    def _on_response(response: Response) -> None:
        if "matched" in result:
            return
        if url_predicate(response.url):
            try:
                result["matched"] = response.json()
            except Exception:
                logger.debug(f"Response matched {response.url} but body wasn't JSON")

    page.on("response", _on_response)
    try:
        trigger()
        elapsed = 0
        step = 200
        while "matched" not in result and elapsed < timeout_ms:
            page.wait_for_timeout(step)
            elapsed += step
        return result.get("matched")
    finally:
        page.remove_listener("response", _on_response)


class ThreadBoundProxy:
    """Forwards every method call on `obj` onto a single dedicated worker
    thread, so Playwright calls never share a thread with `asyncio.run()`
    based code elsewhere (see module docstring — confirmed live that the two
    can't coexist in one thread). `setup()`/`teardown()` also run on that
    same thread, so the wrapped object's whole lifecycle — including
    `sync_playwright().start()` and browser launch — stays thread-consistent.
    """

    def __init__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="browser-worker"
        )
        self._obj = None

    def setup(self, factory: Callable[[], object]) -> None:
        self._obj = self._executor.submit(factory).result()

    def __getattr__(self, name):
        attr = getattr(self._obj, name)
        if not callable(attr):
            return attr

        def _call(*args, **kwargs):
            return self._executor.submit(attr, *args, **kwargs).result()

        return _call

    def teardown(self, fn: Callable[[], None]) -> None:
        self._executor.submit(fn).result()
        self._executor.shutdown(wait=True)
