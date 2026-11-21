from __future__ import annotations

from pathlib import Path


def synthesize_voice(client, script: str, *, language: str = "en", audio_path: str | None = None) -> dict:
    audio = client.synthesize(script, language=language)
    if audio is None:
        return {
            "provider": client.provider,
            "audio_path": None,
            "reason": "grok_api_key_absent" if client.provider == "mock" else "no_audio",
            "language": language,
        }
    if audio_path:
        destination = Path(audio_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(audio)
        return {"provider": client.provider, "audio_path": str(destination), "reason": None, "language": language}
    return {"provider": client.provider, "audio_path": None, "reason": "not_requested", "language": language, "audio_bytes": len(audio)}
