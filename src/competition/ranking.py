"""Ranking logic for participant solution outputs."""

from __future__ import annotations

import os

import pandas as pd
from catboost import CatBoostClassifier

try:
    from catboost.utils import get_gpu_device_count
except Exception:  # catboost<1.2 compatibility
    def get_gpu_device_count() -> int:  # type: ignore
        return 0

from src.platform.core.dataset import Dataset


class SimpleBlendRanker:
    """Blend sources with weighted scores and enforce top-k per user.

    The ranker combines candidate sources by weighted max aggregation, filters
    already seen positives, and fills missing slots with global-popularity
    fallback to preserve a valid submission shape for every target user.
    """

    def __init__(self, source_weights: dict[str, float] | None = None) -> None:
        """Capture source-level blend multipliers from experiment config.

        Args:
            source_weights: Optional mapping from source name to multiplicative
                score weight. Missing sources default to weight 1.0.
        """
        self.source_weights = source_weights or {}

    def _apply_weights(self, candidates: pd.DataFrame) -> pd.DataFrame:
        weighted = candidates.copy()
        weighted["weight"] = weighted["source"].map(self.source_weights).fillna(1.0)
        weighted["final_score"] = weighted["score"] * weighted["weight"]
        return weighted

    def _train_ml_reranker(self, dataset: Dataset, candidates: pd.DataFrame) -> pd.DataFrame:
        """Train a lightweight CatBoost classifier on in-batch positives vs negatives.

        Labels: pairs observed in full interactions are treated as positives.
        Negatives are subsampled per user to keep training fast.
        """
        if candidates.empty:
            return candidates

        seen_pairs = set(
            tuple(x)
            for x in dataset.seen_positive_df[["user_id", "edition_id"]].drop_duplicates().to_numpy()
        )
        candidates = candidates.copy()
        candidates["label"] = [
            1 if (int(u), int(e)) in seen_pairs else 0
            for u, e in zip(candidates["user_id"], candidates["edition_id"])
        ]

        # Feature engineering: aggregate statistics per edition
        edition_stats = (
            candidates.groupby("edition_id")[["sum_score", "max_score"]]
            .agg(["mean", "max"])
            .reset_index()
        )
        edition_stats.columns = [
            "edition_id",
            "sum_score_mean",
            "sum_score_max",
            "max_score_mean",
            "max_score_max",
        ]
        candidates = candidates.merge(edition_stats, on="edition_id", how="left")

        edition_pop = (
            dataset.seen_positive_df.groupby("edition_id").size().rename("edition_pop").reset_index()
        )
        user_hist = (
            dataset.seen_positive_df.groupby("user_id").size().rename("user_hist").reset_index()
        )
        candidates = candidates.merge(edition_pop, on="edition_id", how="left")
        candidates = candidates.merge(user_hist, on="user_id", how="left")
        candidates[["edition_pop", "user_hist"]] = candidates[["edition_pop", "user_hist"]].fillna(0)

        # Subsample negatives per user
        sampled = []
        for user_id, group in candidates.groupby("user_id"):
            pos = group[group["label"] == 1]
            neg = group[group["label"] == 0]
            if not pos.empty:
                n_neg = min(len(neg), len(pos) * 20)
                neg = neg.sample(n=n_neg, random_state=42) if n_neg < len(neg) else neg
            sampled.append(pd.concat([pos, neg], ignore_index=True))
        train_df = pd.concat(sampled, ignore_index=True)
        if train_df["label"].sum() == 0:
            return candidates

        feature_cols = [
            "sum_score",
            "max_score",
            "sum_score_mean",
            "sum_score_max",
            "max_score_mean",
            "max_score_max",
            "edition_pop",
            "user_hist",
            "sources",
        ]

        task_type = "GPU" if get_gpu_device_count() > 0 else "CPU"
        devices = os.environ.get("CUDA_VISIBLE_DEVICES")
        model = CatBoostClassifier(
            iterations=120,
            depth=6,
            learning_rate=0.08,
            eval_metric="AUC",
            verbose=False,
            random_seed=42,
            task_type=task_type,
            devices=devices if task_type == "GPU" and devices else None,
        )
        model.fit(train_df[feature_cols], train_df["label"])
        pred = model.predict_proba(candidates[feature_cols])[:, 1]
        candidates["final_score"] = pred
        return candidates

    def rank(self, dataset: Dataset, candidates: pd.DataFrame, k: int) -> pd.DataFrame:
        """Rank candidates and produce exactly top-k rows per user.

        Args:
            dataset: Runtime dataset with targets and seen-positive pairs.
            candidates: Candidate frame merged across generator sources.
            k: Required output cutoff per user.

        Returns:
            DataFrame with `user_id`, `edition_id`, `rank`, `final_score`.
        """
        if candidates.empty:
            return self._fallback_only(dataset, k)

        seen = dataset.seen_positive_df[["user_id", "edition_id"]].drop_duplicates()
        filtered = candidates.merge(
            seen.assign(_seen=1),
            on=["user_id", "edition_id"],
            how="left",
        )
        filtered = filtered[filtered["_seen"].isna()].drop(columns=["_seen"])
        if filtered.empty:
            return self._fallback_only(dataset, k)

        filtered = self._apply_weights(filtered)

        # Aggregate by summing weighted scores; duplicates across sources reinforce rank.
        agg = (
            filtered.groupby(["user_id", "edition_id"], as_index=False)
            .agg(
                sum_score=("final_score", "sum"),
                max_score=("final_score", "max"),
                sources=("source", "nunique"),
            )
        )
        # Boost items surfaced by multiple generators more aggressively
        agg["final_score"] = agg["sum_score"] * (1.0 + 0.25 * (agg["sources"] - 1))
        # ML rerank
        reranked = self._train_ml_reranker(dataset, agg)
        blended = reranked.sort_values(
            ["user_id", "final_score", "edition_id"], ascending=[True, False, True]
        )

        selected = blended.groupby("user_id", group_keys=False).head(k).copy()
        selected["rank"] = selected.groupby("user_id").cumcount() + 1
        selected = selected[["user_id", "edition_id", "rank", "final_score"]]

        completed = self._apply_fallback(selected, dataset, k)
        return completed.sort_values(["user_id", "rank"]).reset_index(drop=True)

    def _fallback_only(self, dataset: Dataset, k: int) -> pd.DataFrame:
        rows: list[dict[str, int | float]] = []
        positives = dataset.interactions_df[dataset.interactions_df["event_type"].isin([1, 2])]
        popularity = (
            positives.groupby("edition_id", as_index=False)["user_id"]
            .nunique()
            .rename(columns={"user_id": "pop"})
            .sort_values(["pop", "edition_id"], ascending=[False, True])
        )
        ranked_editions = popularity["edition_id"].tolist()
        seen_pairs = set(
            tuple(x)
            for x in dataset.seen_positive_df[["user_id", "edition_id"]].drop_duplicates().to_numpy()
        )
        for user_id in dataset.targets_df["user_id"].tolist():
            rank = 1
            for edition_id in ranked_editions:
                if (int(user_id), int(edition_id)) in seen_pairs:
                    continue
                rows.append(
                    {
                        "user_id": int(user_id),
                        "edition_id": int(edition_id),
                        "rank": rank,
                        "final_score": 0.0,
                    }
                )
                rank += 1
                if rank > k:
                    break
        return pd.DataFrame(rows)

    def _apply_fallback(
        self,
        selected: pd.DataFrame,
        dataset: Dataset,
        k: int,
    ) -> pd.DataFrame:
        positives = dataset.interactions_df[dataset.interactions_df["event_type"].isin([1, 2])]
        popularity = (
            positives.groupby("edition_id", as_index=False)["user_id"]
            .nunique()
            .rename(columns={"user_id": "pop"})
            .sort_values(["pop", "edition_id"], ascending=[False, True])
        )
        popular_editions = popularity["edition_id"].tolist()
        seen_pairs = set(
            tuple(x)
            for x in dataset.seen_positive_df[["user_id", "edition_id"]].drop_duplicates().to_numpy()
        )
        chosen_pairs = set(tuple(x) for x in selected[["user_id", "edition_id"]].to_numpy())
        missing_rows: list[dict[str, int | float]] = []
        by_user_counts = selected.groupby("user_id").size().to_dict()

        for user_id in dataset.targets_df["user_id"].tolist():
            count = int(by_user_counts.get(int(user_id), 0))
            rank = count + 1
            if count >= k:
                continue
            for edition_id in popular_editions:
                pair = (int(user_id), int(edition_id))
                if pair in chosen_pairs or pair in seen_pairs:
                    continue
                missing_rows.append(
                    {
                        "user_id": int(user_id),
                        "edition_id": int(edition_id),
                        "rank": rank,
                        "final_score": 0.0,
                    }
                )
                chosen_pairs.add(pair)
                rank += 1
                if rank > k:
                    break
        if missing_rows:
            selected = pd.concat([selected, pd.DataFrame(missing_rows)], ignore_index=True)
        return selected


def rank_predictions(
    dataset: Dataset,
    candidates: pd.DataFrame,
    source_weights: dict[str, float],
    k: int,
) -> pd.DataFrame:
    """Rank candidate set using configured blend strategy.

    Args:
        dataset: Runtime dataset for filtering and fallback behavior.
        candidates: Candidate rows emitted by all generators.
        source_weights: Source weights configured for blending.
        k: Required cutoff for returned top list.

    Returns:
        Ranked DataFrame with `user_id`, `edition_id`, `rank`, `final_score`.
    """
    ranker = SimpleBlendRanker(
        source_weights={key: float(value) for key, value in source_weights.items()}
    )
    return ranker.rank(dataset=dataset, candidates=candidates, k=int(k))
