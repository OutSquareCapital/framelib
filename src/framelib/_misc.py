# TODO
import math
from dataclasses import dataclass
from typing import Literal, TypedDict

TimeDelta = Literal["ns", "us", "ms", "s", "m", "h", "d", "w", "mo", "q", "y", "i"]


class WindowArgs(TypedDict):
    window_size: int
    min_samples: int


@dataclass(slots=True)
class WindowManager:
    """
    Periods list:
        1ns (1 nanosecond)
        1us (1 microsecond)
        1ms (1 millisecond)
        1s (1 second)
        1m (1 minute)
        1h (1 hour)
        1d (1 calendar day)
        1w (1 calendar week)
        1mo (1 calendar month)
        1q (1 calendar quarter)
        1y (1 calendar year)
        1i (1 index count)
    """

    window_size: int
    min_samples: int
    every: TimeDelta = "d"
    each: int = 1

    @property
    def window_args(self) -> WindowArgs:
        return WindowArgs(window_size=self.window_size, min_samples=self.min_samples)

    @property
    def group_by_arg(self) -> str:
        return f"{self.each}{self.every}"

    @property
    def annualization_factor(self) -> float:
        periods_in_year: float
        match self.every:
            case "h":
                periods_in_year = 5040
            case "d":
                periods_in_year = 252.0
            case "w":
                periods_in_year = 52.0
            case "mo":
                periods_in_year = 12.0
            case "q":
                periods_in_year = 4.0
            case "y":
                periods_in_year = 1.0
            case _:
                raise ValueError(
                    f"La fréquence '{self.every}' n'est pas supportée pour l'annualisation."
                )
        return math.sqrt(periods_in_year / self.each)
