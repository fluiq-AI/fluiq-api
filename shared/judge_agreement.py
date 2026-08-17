"""Does the judge agree with the humans?

Everything else in this product treats a judge score as the measurement. This
asks whether the measurement is any good — by running the judge over examples a
person already graded and comparing.

That inversion is the workshop's most useful idea (74:43): SMEs label N examples,
those labels become ground truth, and then **the judge prompt is the thing under
test**. Without it, "our hallucination score is 0.82" is a number with no
established relationship to whether the output was actually hallucinated.

Three numbers, because they fail differently
--------------------------------------------
*Agreement* is the headline: how often judge and human land on the same side of
the threshold. It is the one people quote, and on its own it lies — a judge that
says "good" to everything scores 90% agreement on a dataset that is 90% good.

*Correlation* catches that. A judge that never varies has no correlation with the
humans no matter how often it happens to be right, so a high agreement next to a
near-zero correlation is the signature of a judge that has learned the base rate
rather than the task.

*Bias* says which direction it is wrong in, because those have different costs. A
lenient judge ships bad output; a harsh one blocks good output and erodes trust
in the whole system.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

#: Below this, a judge is not measuring the thing the humans are measuring.
WEAK_CORRELATION = 0.3


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    """Pearson correlation, or None when it is undefined.

    Undefined — not zero — when either side never varies. A human set that is
    all 1.0 has no signal to correlate against, and reporting 0 there would read
    as "the judge is uncorrelated" when the truth is "this dataset cannot say".
    """
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]
    numerator = sum(a * b for a, b in zip(dx, dy))
    denom_x = sum(a * a for a in dx) ** 0.5
    denom_y = sum(b * b for b in dy) ** 0.5
    # A tolerance rather than == 0: averaging twelve copies of 0.7 lands at
    # 0.7000000000000001, so an exactly-constant series has deviations of ~1e-16
    # instead of 0. That sails past a zero check and yields a meaningless
    # correlation computed entirely from rounding error — which then reads as
    # "the judge is uncorrelated" when the truth is "the judge never varied".
    if denom_x < 1e-9 or denom_y < 1e-9:
        return None
    # Clamped: floating-point drift puts a perfect correlation at 1.0000000000000002,
    # and a coefficient outside [-1, 1] is a number no reader can interpret.
    return max(-1.0, min(1.0, numerator / (denom_x * denom_y)))


def compare(
    pairs: Sequence[Dict[str, float]],
    *,
    threshold: float = 0.5,
) -> Dict[str, Any]:
    """Compare judge scores against human labels.

    ``pairs`` is ``[{"judge": 0.8, "human": 1.0}, ...]``. Rows missing either
    side are dropped — an unlabelled example says nothing about agreement — and
    the count that survived is reported, because an agreement figure from six
    examples should not be read like one from six hundred.
    """
    usable = [
        (float(p["judge"]), float(p["human"]))
        for p in pairs
        if isinstance(p.get("judge"), (int, float))
        and isinstance(p.get("human"), (int, float))
    ]
    if not usable:
        return {
            "compared":    0,
            "agreement":   None,
            "correlation": None,
            "mean_error":  None,
            "bias":        None,
            "verdict":     "no_data",
            "detail": (
                "No example carried both a judge score and a human label, so "
                "there is nothing to compare yet."
            ),
        }

    judge_scores = [j for j, _ in usable]
    human_scores = [h for _, h in usable]

    agree = sum(
        1 for j, h in usable if (j >= threshold) == (h >= threshold)
    )
    agreement = agree / len(usable)
    correlation = _pearson(judge_scores, human_scores)
    mean_error = sum(abs(j - h) for j, h in usable) / len(usable)
    # Positive: the judge scores higher than people do — it is too lenient.
    bias = sum(j - h for j, h in usable) / len(usable)

    return {
        "compared":    len(usable),
        "agreement":   agreement,
        "correlation": correlation,
        "mean_error":  mean_error,
        "bias":        bias,
        **_verdict(agreement, correlation, bias, judge_scores, human_scores, len(usable)),
    }


def _verdict(
    agreement: float,
    correlation: Optional[float],
    bias: float,
    judge_scores: List[float],
    human_scores: List[float],
    n: int,
) -> Dict[str, str]:
    """Turn the numbers into the sentence someone can act on.

    Served rather than left to the reader, because the trap here — high
    agreement on an imbalanced dataset — is exactly the thing a reader skims
    past.
    """
    if n < 10:
        return {
            "verdict": "insufficient",
            "detail": (
                f"Only {n} labelled example{'s' if n != 1 else ''}. Label at "
                f"least 10 before trusting any of these numbers."
            ),
        }

    # How lopsided the human labels are. If 90% are 'good', a judge that says
    # 'good' to everything already scores 90% agreement.
    positives = sum(1 for h in human_scores if h >= 0.5)
    base_rate = max(positives, n - positives) / n

    # A judge that returns the same score to everything is the pure form of the
    # base-rate failure, and it is worth naming as that rather than as "no
    # correlation" — those read as different problems, and only one of them is
    # about the judge. Checked before the correlation branch, which would
    # otherwise swallow it: a constant judge has no correlation *by
    # construction*, so "there is no variation to correlate" would send someone
    # off to label more data when the judge is the thing that is broken.
    if len(set(judge_scores)) == 1:
        if agreement >= base_rate - 0.05:
            return {
                "verdict": "matches_base_rate",
                "detail": (
                    f"The judge gave every example the same score. Its "
                    f"{agreement:.0%} agreement is just the share of your labels "
                    f"that happen to be one verdict ({base_rate:.0%}) — it is "
                    f"echoing the common answer, not reading the output."
                ),
            }
        return {
            "verdict": "weak",
            "detail": (
                "The judge gave every example the same score, so it is not "
                "discriminating between them at all."
            ),
        }

    if correlation is None:
        return {
            "verdict": "undetermined",
            "detail": (
                "Every human label is the same score, so there is no variation "
                "to correlate against. Label a mix of good and bad examples."
            ),
        }

    if correlation < WEAK_CORRELATION and agreement >= base_rate - 0.05:
        return {
            "verdict": "matches_base_rate",
            "detail": (
                f"Agreement is {agreement:.0%}, but correlation is only "
                f"{correlation:.2f} and {base_rate:.0%} of your labels are the "
                f"same verdict. This judge is likely echoing the common answer "
                f"rather than reading the output."
            ),
        }

    if correlation < WEAK_CORRELATION:
        return {
            "verdict": "weak",
            "detail": (
                f"Correlation of {correlation:.2f} means the judge is not "
                f"tracking what your reviewers see. Rewrite the prompt against "
                f"the examples it got wrong."
            ),
        }

    if bias > 0.15:
        return {
            "verdict": "lenient",
            "detail": (
                f"The judge scores {bias:+.2f} higher than people do. It is "
                f"passing output your reviewers reject — the expensive "
                f"direction, since those ship."
            ),
        }

    if bias < -0.15:
        return {
            "verdict": "harsh",
            "detail": (
                f"The judge scores {bias:+.2f} lower than people do. It is "
                f"failing output your reviewers accept, which erodes trust in "
                f"every score it produces."
            ),
        }

    if agreement >= 0.8:
        return {
            "verdict": "good",
            "detail": (
                f"{agreement:.0%} agreement and {correlation:.2f} correlation — "
                f"this judge is measuring what your reviewers measure."
            ),
        }

    return {
        "verdict": "mixed",
        "detail": (
            f"{agreement:.0%} agreement with {correlation:.2f} correlation. "
            f"Directionally right but noisy; look at the disagreements."
        ),
    }


def disagreements(
    pairs: Sequence[Dict[str, Any]],
    *,
    threshold: float = 0.5,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """The examples judge and human disagree about most, worst first.

    These are the whole point. The aggregate says *whether* the judge is wrong;
    only the rows say *how*, and a prompt gets fixed by reading them.
    """
    out: List[Dict[str, Any]] = []
    for pair in pairs:
        judge = pair.get("judge")
        human = pair.get("human")
        if not isinstance(judge, (int, float)) or not isinstance(human, (int, float)):
            continue
        gap = abs(float(judge) - float(human))
        if (float(judge) >= threshold) == (float(human) >= threshold) and gap < 0.34:
            continue
        out.append({
            **{k: v for k, v in pair.items() if k not in ("judge", "human")},
            "judge": float(judge),
            "human": float(human),
            "gap":   gap,
            "direction": "lenient" if judge > human else "harsh",
        })
    out.sort(key=lambda row: row["gap"], reverse=True)
    return out[:limit]


__all__ = ["WEAK_CORRELATION", "compare", "disagreements"]
