"""Feature construction entrypoint for participant solution."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.platform.core.dataset import Dataset


def build_features_frame(dataset: Dataset, recent_days: int) -> pd.DataFrame:
    """Build feature matrix with recency- and decay-aware signals.

    Adds multiple popularity windows, popularity ratios, and time-decayed
    user profiles to strengthen recency sensitivity without breaking the
    existing generator contract.
    """

    positives = dataset.interactions_df[dataset.interactions_df["event_type"].isin([1, 2])]
    if positives.empty:
        return pd.DataFrame(
            columns=["feature_type", "user_id", "edition_id", "genre_id", "author_id", "value"]
        )

    max_ts = positives["event_ts"].max()

    # Global popularity (all time)
    popularity_all = (
        positives.groupby("edition_id", as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "value"})
    )
    popularity_all["feature_type"] = "edition_popularity_all"
    popularity_all["user_id"] = pd.NA
    popularity_all["genre_id"] = pd.NA
    popularity_all["author_id"] = pd.NA

    # Recency windows
    def popularity_window(days: int) -> pd.DataFrame:
        cutoff = max_ts - pd.Timedelta(days=days)
        recent = positives[positives["event_ts"] >= cutoff]
        pop = (
            recent.groupby("edition_id", as_index=False)["user_id"]
            .nunique()
            .rename(columns={"user_id": "value"})
        )
        pop["feature_type"] = f"edition_popularity_{days}d"
        pop["user_id"] = pd.NA
        pop["genre_id"] = pd.NA
        pop["author_id"] = pd.NA
        return pop

    recency_windows = [7, 14, 30]
    popularity_recents = [popularity_window(days) for days in recency_windows]

    # Ratios recent/all for each window to highlight spikes
    ratio_frames: list[pd.DataFrame] = []
    all_map = {int(row.edition_id): float(row.value) for row in popularity_all.itertuples()}
    for days, pop_df in zip(recency_windows, popularity_recents):
        ratio_df = pop_df[["edition_id", "value"]].copy()
        ratio_df["value"] = ratio_df.apply(
            lambda row: float(row["value"]) / all_map.get(int(row["edition_id"]), 1.0)
            if all_map.get(int(row["edition_id"]), 0.0) > 0
            else 0.0,
            axis=1,
        )
        ratio_df["feature_type"] = f"edition_popularity_ratio_{days}d_all"
        ratio_df["user_id"] = pd.NA
        ratio_df["genre_id"] = pd.NA
        ratio_df["author_id"] = pd.NA
        ratio_frames.append(ratio_df)

    # Time-decayed user profiles (half-life ~ 30 days)
    decay_tau_days = 30.0
    positives_with_ts = positives[["user_id", "edition_id", "event_ts"]].copy()
    positives_with_ts["weight"] = np.exp(
        -(max_ts - positives_with_ts["event_ts"]).dt.total_seconds() / 86400.0 / decay_tau_days
    )

    user_genres = positives_with_ts.merge(
        dataset.catalog_df[["edition_id", "book_id"]],
        on="edition_id",
        how="inner",
    ).merge(dataset.book_genres_df, on="book_id", how="inner")
    user_genre_profile = (
        user_genres.groupby(["user_id", "genre_id"], as_index=False)["edition_id"]
        .count()
        .rename(columns={"edition_id": "value"})
    )
    user_genre_profile["value"] = user_genre_profile["value"] / user_genre_profile.groupby(
        "user_id"
    )["value"].transform("sum")
    user_genre_profile["feature_type"] = "user_genre_profile"
    user_genre_profile["edition_id"] = pd.NA
    user_genre_profile["author_id"] = pd.NA

    user_genre_profile_decay = (
        user_genres.groupby(["user_id", "genre_id"], as_index=False)["weight"]
        .sum()
        .rename(columns={"weight": "value"})
    )
    user_genre_profile_decay["value"] = user_genre_profile_decay["value"] / user_genre_profile_decay.groupby(
        "user_id"
    )["value"].transform("sum")
    user_genre_profile_decay["feature_type"] = "user_genre_profile_decay"
    user_genre_profile_decay["edition_id"] = pd.NA
    user_genre_profile_decay["author_id"] = pd.NA

    user_authors = positives_with_ts.merge(
        dataset.catalog_df[["edition_id", "author_id"]],
        on="edition_id",
        how="inner",
    )
    user_author_profile = (
        user_authors.groupby(["user_id", "author_id"], as_index=False)["edition_id"]
        .count()
        .rename(columns={"edition_id": "value"})
    )
    user_author_profile["value"] = user_author_profile["value"] / user_author_profile.groupby(
        "user_id"
    )["value"].transform("sum")
    user_author_profile["feature_type"] = "user_author_profile"
    user_author_profile["edition_id"] = pd.NA
    user_author_profile["genre_id"] = pd.NA

    user_author_profile_decay = (
        user_authors.groupby(["user_id", "author_id"], as_index=False)["weight"]
        .sum()
        .rename(columns={"weight": "value"})
    )
    user_author_profile_decay["value"] = user_author_profile_decay["value"] / user_author_profile_decay.groupby(
        "user_id"
    )["value"].transform("sum")
    user_author_profile_decay["feature_type"] = "user_author_profile_decay"
    user_author_profile_decay["edition_id"] = pd.NA
    user_author_profile_decay["genre_id"] = pd.NA

    frames = [
        popularity_all,
        *popularity_recents,
        *ratio_frames,
        user_genre_profile,
        user_genre_profile_decay,
        user_author_profile,
        user_author_profile_decay,
    ]

    return pd.concat(
        [f[["feature_type", "user_id", "edition_id", "genre_id", "author_id", "value"]] for f in frames],
        ignore_index=True,
    )
