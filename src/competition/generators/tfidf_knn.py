"""TF-IDF kNN content generator."""

from __future__ import annotations

import sys
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from src.platform.core.dataset import Dataset


class TfidfKnnGenerator:
    """Generate candidates using TF-IDF similarity of catalog texts."""

    name = "tfidf_knn"

    def __init__(
        self,
        max_features: int = 5000,
        ngram_max: int = 1,
        n_neighbors: int = 40,
        history_limit: int = 2,
        top_editions: int = 50000,
        show_progress: bool = False,
    ) -> None:
        self.max_features = max_features
        self.ngram_max = ngram_max
        self.n_neighbors = n_neighbors
        self.history_limit = history_limit
        self.top_editions = top_editions
        self.show_progress = show_progress

    def _build_catalog_texts(self, dataset: Dataset) -> pd.DataFrame:
        pop_counts = (
            dataset.seen_positive_df.groupby("edition_id").size().rename("pop").reset_index()
        )
        pop_counts = pop_counts.sort_values("pop", ascending=False)
        top_ids = set(pop_counts.head(self.top_editions)["edition_id"].astype(int).tolist())

        authors = dataset.authors_df[["author_id", "author_name"]]
        catalog = dataset.catalog_df.merge(authors, on="author_id", how="left")
        catalog = catalog[catalog["edition_id"].isin(top_ids)]

        # genres per book
        book_genres = dataset.book_genres_df.merge(dataset.genres_df, on="genre_id", how="left")
        genres_by_book = (
            book_genres.groupby("book_id")["genre_name"]
            .apply(lambda s: " ".join(sorted({str(x) for x in s.dropna()})))
            .rename("genre_text")
            .reset_index()
        )
        catalog = catalog.merge(genres_by_book, on="book_id", how="left")

        catalog["title"] = catalog["title"].fillna("")
        catalog["author_name"] = catalog["author_name"].fillna("")
        catalog["genre_text"] = catalog["genre_text"].fillna("")

        catalog["text"] = (
            catalog["title"].astype(str)
            + " "
            + catalog["author_name"].astype(str)
            + " "
            + catalog["genre_text"].astype(str)
        ).str.lower()
        return catalog[["edition_id", "text"]]

    def _fit_models(self, texts: Iterable[str]):
        vectorizer = TfidfVectorizer(
            max_features=self.max_features,
            ngram_range=(1, self.ngram_max),
            min_df=2,
        )
        tfidf = vectorizer.fit_transform(texts)
        nn = NearestNeighbors(
            n_neighbors=self.n_neighbors,
            metric="cosine",
            algorithm="brute",
        )
        nn.fit(tfidf)
        return vectorizer, tfidf, nn

    def generate(
        self,
        dataset: Dataset,
        user_ids: np.ndarray,
        features: pd.DataFrame,
        k: int,
        seed: int,
    ) -> pd.DataFrame:
        del features, seed

        catalog_texts = self._build_catalog_texts(dataset)
        vectorizer, tfidf_matrix, nn = self._fit_models(catalog_texts["text"].tolist())

        edition_id_to_idx = {int(eid): idx for idx, eid in enumerate(catalog_texts["edition_id"])}
        idx_to_edition_id = catalog_texts["edition_id"].astype(int).to_numpy()

        interactions = dataset.interactions_df[dataset.interactions_df["event_type"].isin([1, 2])]
        interactions = interactions[interactions["edition_id"].isin(edition_id_to_idx.keys())]
        interactions = interactions.sort_values("event_ts")

        rows: list[dict[str, int | float | str]] = []
        iterable = user_ids.tolist()
        for user_id in tqdm(
            iterable,
            total=len(iterable),
            desc=f"{self.name}_users",
            disable=not (self.show_progress and sys.stdout.isatty()),
            file=sys.stdout,
        ):
            user_hist = interactions[interactions["user_id"] == int(user_id)]
            if user_hist.empty:
                continue
            seed_editions = (
                user_hist.tail(self.history_limit)["edition_id"]
                .astype(int)
                .tolist()
            )
            candidate_scores: dict[int, float] = {}
            for eid in seed_editions:
                idx = edition_id_to_idx.get(int(eid))
                if idx is None:
                    continue
                distances, indices = nn.kneighbors(tfidf_matrix[idx], return_distance=True)
                sims = 1.0 - distances[0]
                for j, sim in zip(indices[0], sims):
                    cand_eid = int(idx_to_edition_id[j])
                    if cand_eid == int(eid):
                        continue
                    # keep max similarity per edition
                    if sim > candidate_scores.get(cand_eid, 0.0):
                        candidate_scores[cand_eid] = float(sim)

            if not candidate_scores:
                continue

            seen_pairs = set(
                tuple(x)
                for x in dataset.seen_positive_df[
                    ["user_id", "edition_id"]
                ].drop_duplicates().to_numpy()
            )

            scored = sorted(candidate_scores.items(), key=lambda x: (-x[1], x[0]))[: int(k)]
            rank = 1
            for cand_eid, score in scored:
                if (int(user_id), int(cand_eid)) in seen_pairs:
                    continue
                rows.append(
                    {
                        "user_id": int(user_id),
                        "edition_id": int(cand_eid),
                        "score": float(score),
                        "source": self.name,
                    }
                )
                rank += 1
                if rank > k:
                    break

        if not rows:
            return pd.DataFrame(columns=["user_id", "edition_id", "score", "source"])
        return pd.DataFrame(rows)
