# src/kaspi_parser/checks_visual.py
from typing import List, Tuple, Dict, Any
import pandas as pd
from src.kaspi_gold.layout import analyze_regions, normalize_font_name, TRUSTED_FONTS

def check_region_suspicious_fonts(span_df: pd.DataFrame,
                                  regions: Dict[str, Tuple[float,float]]):
    """
    Returns (flags, debug)
    """
    region_stats = analyze_regions(span_df, regions)
    flags = []
    debug = {}

    for region_name, stats in region_stats.items():
        normalized_detected = [
            normalize_font_name(f) for f in stats["fonts"]
        ]
        bad_fonts = [
            orig for orig, norm in zip(stats["fonts"], normalized_detected)
            if norm not in TRUSTED_FONTS
        ]
        if bad_fonts:
            flag = f"SUSPICIOUS_FONT_FAMILY_{region_name.upper()}"
            flags.append(flag)
            debug[flag] = {
                "region": region_name,
                "fonts_detected": list(stats["fonts"]),
                "fonts_flagged": bad_fonts,
                "trusted_fonts": sorted(list(TRUSTED_FONTS)),
            }

    return flags, debug

def check_region_font_size_inconsistency(span_df: pd.DataFrame,
                                         regions: Dict[str, Tuple[float,float]],
                                         max_ok_sizes=3):
    """
    Returns (flags, debug)
    """
    region_stats = analyze_regions(span_df, regions)
    flags = []
    debug = {}

    for region_name, stats in region_stats.items():
        sizes_body = sorted(list(stats["sizes_body"]))
        if len(sizes_body) > max_ok_sizes:
            flag = f"INCONSISTENT_FONTS_{region_name.upper()}"
            flags.append(flag)
            debug[flag] = {
                "region": region_name,
                "sizes_body": sizes_body,
                "unique_count": len(sizes_body),
                "max_ok_sizes": max_ok_sizes
            }

    return flags, debug
