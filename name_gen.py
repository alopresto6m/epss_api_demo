import itertools
from pprint import pprint
from typing import TextIO

import nltk as nltk

comma_separated_suffixes = ["Inc.", "LLC", "LLP"]
space_separated_suffixes = ["Incorporated", "AG", "GmbH"]
common_suffixes = comma_separated_suffixes + space_separated_suffixes


def gen_names_manual(src_name: str) -> set[str]:
    parts = src_name.rsplit(" ", 2)
    if len(parts) > 1 and parts[1].strip() in common_suffixes:
        parts.pop(1)
    base_name = ' '.join(parts)
    return set([src_name] + [f"{base_name} {i}" for i in space_separated_suffixes] + [f"{base_name}, {i}" for i in
                                                                                      comma_separated_suffixes])


def calculate_distance(src: str, candidate: str) -> float:
    """Calculates the edit distance without regard for capitalization"""
    return nltk.edit_distance(src.lower(), candidate.lower())


def get_candidates_from_file(f: TextIO) -> dict:
    # Use generator rather than intermediate list
    # pairs = dict(l.rstrip().split(',', 1) for l in f)
    # pairs = {input: candidate if (input, candidate := l.split(',', 2)) for l in n.readlines()}
    pairs = [l.rstrip().split(',', 1) for l in f]

    # Group the list of tuples by the first item in the tuple (the key)
    # -> {k1: [[k1, v1], [k1, v2], [k1, v3]], k2: [[k2, v4], [k2, v5]]}
    results = {}
    for k, g in itertools.groupby(pairs, lambda p: p[0]):
        results[k] = list(g)

    # Flatten the values lists -> {k1: [v1, v2, v3], k2: [v4, v5]}
    candidates = {k: [i[1] for i in v if i[0] != 'null'] for k, v in results.items()}
    return candidates


def score_candidates(candidates: dict) -> dict:
    scores = {}

    # TODO: Refactor to dict comprehension
    for src, candidate_list in candidates.items():
        for candidate in candidate_list:
            distance = calculate_distance(src, candidate)
            if src in scores:
                scores[src][candidate] = distance
            else:
                scores[src] = {candidate: distance}

    return scores


def select_best_candidate(candidates: dict) -> dict:
    return {k: sorted(v.items(), key=lambda x: x[1])[0] for k, v in candidates.items()}


def main() -> None:
    with open('./northrop.txt', 'r') as f:
        candidates = get_candidates_from_file(f)

    # Score all the generated candidates (nested dict)
    scores = score_candidates(candidates)
    pprint(scores)

    # Determine the single best candidate for each source
    pprint(select_best_candidate(scores))


if __name__ == '__main__':
    main()
