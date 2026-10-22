"""The "Hey JOI" wake word: an offline, opt-in microphone listener.

Uses a small Vosk model (~40 MB, downloaded once on first enable, checksum pinned) with
a tiny grammar, so it only ever tries to match a handful of phrases. Audio is processed
in memory on this machine and never stored or sent anywhere. Off unless the user turns
it on in Settings. On a match, `on_wake()` is called; the API server pushes that to the
desktop app, which opens the quick assistant and starts listening.
"""

import json
import logging
import os
import queue
import shutil
import tempfile
import threading
import time
import hashlib
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

MODEL_NAME = "vosk-model-small-en-us-0.15"
MODEL_URL = f"https://alphacephei.com/vosk/models/{MODEL_NAME}.zip"
MODEL_SHA256 = "30f26242c4eb449f948e42cb302dd7a686cb29a3423a8367f99ff41780942498"
SAMPLE_RATE = 16000

# "JOI" is said like "joy". The extra words soak up near-misses ("enjoy", "joey") so they
# don't get forced onto the wake phrase; [unk] catches everything else.
GRAMMAR = ["hey joy", "hi joy", "okay joy", "hey", "hi", "okay", "joy", "joey", "enjoy", "boy", "toy", "[unk]"]
WAKE_PHRASES = {"hey joy", "hi joy", "okay joy"}
MIN_CONFIDENCE = 0.80
COOLDOWN_SECONDS = 8.0  # ignore the mic while the user speaks their request


def models_dir() -> Path:
    base = os.environ.get("JOI_MODELS_DIR") or os.path.join(
        os.environ.get("LOCALAPPDATA") or str(Path.home()), "JOI", "models"
    )
    return Path(base)


def model_path() -> Path:
    return models_dir() / MODEL_NAME


def model_ready() -> bool:
    return (model_path() / "am" / "final.mdl").exists()


def ensure_model() -> Path:
    """Download and unpack the model once. Raises on a checksum mismatch."""
    if model_ready():
        return model_path()
    models_dir().mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=models_dir()) as tmp:
        archive = Path(tmp) / "model.zip"
        logger.info("Downloading wake-word model...")
        with urllib.request.urlopen(MODEL_URL, timeout=60) as resp, open(archive, "wb") as out:
            shutil.copyfileobj(resp, out)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        if digest != MODEL_SHA256:
            raise RuntimeError("Wake-word model download failed its integrity check.")
        with zipfile.ZipFile(archive) as zf:
            root = Path(tmp).resolve()
            for name in zf.namelist():
                if not (root / name).resolve().is_relative_to(root):
                    raise RuntimeError("Unexpected file in the wake-word model archive.")
            zf.extractall(tmp)
        shutil.move(str(Path(tmp) / MODEL_NAME), str(model_path()))
    return model_path()


def is_wake(result_json: str) -> bool:
    """True when a Vosk final result is a confident wake phrase."""
    try:
        res = json.loads(result_json)
    except json.JSONDecodeError:
        return False
    words = res.get("result") or []
    for phrase in WAKE_PHRASES:
        parts = phrase.split()
        for i in range(len(words) - len(parts) + 1):
            window = words[i:i + len(parts)]
            if [w.get("word") for w in window] == parts and min(w.get("conf", 0) for w in window) >= MIN_CONFIDENCE:
                return True
    return False


def new_recognizer(model):
    from vosk import KaldiRecognizer
    rec = KaldiRecognizer(model, SAMPLE_RATE, json.dumps(GRAMMAR))
    rec.SetWords(True)
    return rec


def load_model():
    from vosk import Model, SetLogLevel
    SetLogLevel(-1)
    return Model(str(ensure_model()))


class WakeWordListener:
    """One background thread reading the default microphone."""

    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.status = "off"  # off | loading | listening | error
        self.error = ""
        self.owner_id = ""

    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def start(self, owner_id: str, on_wake: Callable[[str], None]) -> None:
        if self.running() and self.owner_id == owner_id:
            return
        self.stop()
        self.owner_id = owner_id
        self._stop.clear()
        self.status, self.error = "loading", ""
        self._thread = threading.Thread(target=self._run, args=(on_wake,), name="joi-wake-word", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None
        self.status = "off"

    def _run(self, on_wake: Callable[[str], None]) -> None:
        try:
            import sounddevice as sd
            model = load_model()
            rec = new_recognizer(model)
            audio: "queue.Queue[bytes]" = queue.Queue(maxsize=50)

            def callback(indata, _frames, _time, _status):
                try:
                    audio.put_nowait(bytes(indata))
                except queue.Full:
                    pass

            with sd.RawInputStream(samplerate=SAMPLE_RATE, blocksize=4000, dtype="int16", channels=1, callback=callback):
                self.status = "listening"
                logger.info("Wake word listening.")
                quiet_until = 0.0
                while not self._stop.is_set():
                    try:
                        chunk = audio.get(timeout=0.5)
                    except queue.Empty:
                        continue
                    if time.monotonic() < quiet_until:
                        continue
                    if rec.AcceptWaveform(chunk) and is_wake(rec.Result()):
                        quiet_until = time.monotonic() + COOLDOWN_SECONDS
                        rec = new_recognizer(model)
                        try:
                            on_wake(self.owner_id)
                        except Exception as exc:  # never let a callback kill the listener
                            logger.warning("Wake callback failed: %s", exc)
        except Exception as exc:
            logger.warning("Wake word stopped: %s", exc)
            self.status, self.error = "error", str(exc)[:200]
            return
        self.status = "off"


listener = WakeWordListener()
