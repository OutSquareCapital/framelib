import math
from dataclasses import dataclass
from types import ModuleType
from typing import Self

import polars as pl

from ._types import ColorMap, HexColor, Palette


@dataclass(slots=True)
class RGBColor:
    r: int
    g: int
    b: int

    @classmethod
    def from_hex(cls, hex_color: HexColor) -> Self:
        hex_color = hex_color.lstrip("#")
        result = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
        return cls(*result)

    def to_hex(self) -> HexColor:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def interpolate(self, other: Self, factor: float) -> "RGBColor":
        return RGBColor(
            r=math.floor(self.r + (other.r - self.r) * factor),
            g=math.floor(self.g + (other.g - self.g) * factor),
            b=math.floor(self.b + (other.b - self.b) * factor),
        )


def generate_palette(n_colors: int, *base_palette: HexColor) -> Palette:
    palette_len = len(base_palette)

    segments: int = palette_len - 1

    if segments < 1:
        return [base_palette[0]] * n_colors

    result: list[HexColor] = []
    total_interval: int = (n_colors - 1) if n_colors > 1 else 1
    for i in range(n_colors):
        pos: float = (i / total_interval) * segments if total_interval > 0 else 0.0
        index: int = math.floor(pos)
        factor: float = pos - index

        c2_hex = base_palette[min(index + 1, segments)]

        result.append(
            RGBColor.from_hex(base_palette[index])
            .interpolate(RGBColor.from_hex(c2_hex), factor)
            .to_hex()
        )

    return result


def extract_color_scales(module: ModuleType) -> dict[str, list[str]]:
    return {
        key: value
        for key, value in module.__dict__.items()
        if isinstance(value, list) and not key.startswith("_")
    }


def combine_palettes(*palettes: Palette) -> Palette:
    return [color for palette in palettes for color in palette]


def get_color_map(serie: pl.Series, base_palette: Palette) -> ColorMap:
    return dict(zip(serie.to_list(), generate_palette(serie.len(), *base_palette)))
