"""Global popularity generator for participant solution."""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.platform.core.dataset import Dataset


class GlobalPopularityGenerator:
    """Recommend globally popular editions for each target user.

    The generator serves as a robust baseline source and is used as fallback
    recall in blended ranking. It scores items by unique-user popularity from
    precomputed feature tables and broadcasts top-k popular editions to all
    requested users.
    """

    name = "global_popularity"

    def __init__(
        self,
        recent_weight: float = 0.7,
        ratio_weight: float = 0.3,
        recent_days: int = 30,
        show_progress: bool = False,
    ) -> None:
        """Initialize progress behavior for user-level iteration.

        Args:
            recent_weight: Blend weight for recent-window popularity vs all-time.
            ratio_weight: Blend weight for recent/all ratio boost.
            recent_days: Window size (days) used for recent popularity.
            show_progress: Whether to render tqdm bars in interactive sessions.
        """
        self.recent_weight = recent_weight
        self.ratio_weight = ratio_weight
        self.recent_days = recent_days
        self.show_progress = show_progress

    def generate(
        self,
        dataset: Dataset,
        user_ids: np.ndarray,
        features: pd.DataFrame,
        k: int,
        seed: int,
    ) -> pd.DataFrame:
        """Generate candidate rows from global popularity statistics.

        Args:
            dataset: Runtime dataset (unused directly but part of generator contract).
            user_ids: Users for whom candidates must be emitted.
            features: Long feature table containing `edition_popularity_all`.
            k: Maximum candidate count per user.
            seed: Pipeline seed (unused by this deterministic generator).

        Returns:
            Candidate DataFrame with `user_id`, `edition_id`, `score`, `source`.
        """
        del dataset, seed
        pop_all = features[features["feature_type"] == "edition_popularity_all"][
            ["edition_id", "value"]
        ].copy()
        pop_recent = features[features["feature_type"] == f"edition_popularity_{self.recent_days}d"][
            ["edition_id", "value"]
        ].copy()
        pop_ratio = features[
            features["feature_type"] == f"edition_popularity_ratio_{self.recent_days}d_all"
        ][["edition_id", "value"]].copy()

        if pop_all.empty:
            return pd.DataFrame(columns=["user_id", "edition_id", "score", "source"])

        pop_recent_map = {int(r.edition_id): float(r.value) for r in pop_recent.itertuples()}
        pop_ratio_map = {int(r.edition_id): float(r.value) for r in pop_ratio.itertuples()}

        def blended_score(row: pd.Series) -> float:
            edition_id = int(row["edition_id"])
            base = float(row["value"])
            recent = pop_recent_map.get(edition_id, 0.0)
            ratio = pop_ratio_map.get(edition_id, 0.0)
            return (
                base * (1.0 - self.recent_weight)
                + recent * self.recent_weight
                + ratio * self.ratio_weight
            )

        pop_all["score"] = pop_all.apply(blended_score, axis=1)
        popularity = pop_all.sort_values(["score", "edition_id"], ascending=[False, True]).head(k)

        rows: list[dict[str, float | int | str]] = []
        for user_id in tqdm(
            user_ids.tolist(),
            total=len(user_ids),
            desc=f"{self.name}_users",
            leave=False,
            dynamic_ncols=True,
            disable=not (self.show_progress and sys.stdout.isatty()),
            file=sys.stdout,
        ):
            for _, row in popularity.iterrows():
                rows.append(
                    {
                        "user_id": int(user_id),
                        "edition_id": int(row["edition_id"]),
                        "score": float(row["value"]),
                        "source": self.name,
                    }
                )
        return pd.DataFrame(rows)
