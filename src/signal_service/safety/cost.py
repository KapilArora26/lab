from __future__ import annotations

from dataclasses import dataclass, field


class CircuitBreakerOpen(RuntimeError):
    """The daily spend cap would be exceeded by the next call."""


@dataclass
class SpendGuard:
    daily_cap_usd: float
    input_usd_per_million: float
    output_usd_per_million: float
    tts_usd_per_million_chars: float
    spent_usd: float = 0.0
    halted: bool = False
    events: list[dict] = field(default_factory=list)

    def estimate_tokens(self, input_tokens: int, output_tokens: int) -> float:
        return (
            input_tokens * self.input_usd_per_million
            + output_tokens * self.output_usd_per_million
        ) / 1_000_000

    def estimate_speech(self, text: str) -> float:
        return len(text) / 1_000_000 * self.tts_usd_per_million_chars

    def authorize(self, estimated_usd: float) -> None:
        if self.halted or self.spent_usd + estimated_usd > self.daily_cap_usd + 1e-12:
            self.halted = True
            raise CircuitBreakerOpen(
                f"daily spend cap {self.daily_cap_usd:.4f} USD would be exceeded "
                f"(spent {self.spent_usd:.4f}, next {estimated_usd:.4f})"
            )

    def record(self, usd: float, *, kind: str, input_tokens: int = 0, output_tokens: int = 0) -> None:
        self.spent_usd += usd
        self.events.append(
            {
                "kind": kind,
                "estimated_usd": round(usd, 8),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )
        if self.spent_usd > self.daily_cap_usd + 1e-12:
            self.halted = True
