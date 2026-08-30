import pytest
from dce.golds import check_dev_gate, reconstruct, reconstruct_with_shares


def test_two_agreeing_correct_submissions_make_a_gold():
    submissions = {
        "agent_a": {"1": "0.12"},
        "agent_b": {"1": "0.12"},
    }
    scores = {"agent_a": {"1": True}, "agent_b": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {"1": "0.12"}
    assert exclusions == {}


def test_one_correct_submission_is_not_enough():
    submissions = {"agent_a": {"1": "0.12"}}
    scores = {"agent_a": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {}
    assert exclusions["1"] == "insufficient_agreement"


def test_incorrect_submissions_are_ignored_even_when_they_agree():
    submissions = {
        "agent_a": {"1": "wrong"},
        "agent_b": {"1": "wrong"},
        "agent_c": {"1": "0.12"},
        "agent_d": {"1": "0.12"},
    }
    scores = {
        "agent_a": {"1": False},
        "agent_b": {"1": False},
        "agent_c": {"1": True},
        "agent_d": {"1": True},
    }
    golds, _ = reconstruct(submissions, scores)
    assert golds == {"1": "0.12"}


def test_a_lone_dissenter_does_not_destroy_a_strong_plurality():
    # One noisy-but-scored-correct answer among many identical ones is the
    # common case in the real corpus (a pasted reasoning trace, a trailing
    # period). It must not cost us the task.
    submissions = {f"a{i}": {"1": "0.12"} for i in range(9)}
    submissions["noisy"] = {"1": "the answer is 0.12, computed as follows"}
    scores = {agent: {"1": True} for agent in submissions}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {"1": "0.12"}
    assert exclusions == {}


def test_a_genuinely_split_task_is_excluded_not_guessed():
    # 50/50 is real disagreement among answers the leaderboard scored correct.
    # No plurality clears the threshold, so we must not pick a winner.
    submissions = {
        "a": {"1": "0.12"},
        "b": {"1": "0.12"},
        "c": {"1": "totally other"},
        "d": {"1": "totally other"},
    }
    scores = {agent: {"1": True} for agent in submissions}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {}
    assert exclusions["1"] == "below_plurality_threshold"


def test_a_plurality_below_the_threshold_is_excluded():
    # 3 of 5 is a plurality but only a 60% share — short of the 0.75 default.
    submissions = {
        "a": {"1": "0.12"},
        "b": {"1": "0.12"},
        "c": {"1": "0.12"},
        "d": {"1": "other one"},
        "e": {"1": "other two"},
    }
    scores = {agent: {"1": True} for agent in submissions}
    golds, exclusions = reconstruct(submissions, scores)
    assert exclusions["1"] == "below_plurality_threshold"
    # ...and the same corpus clears a looser threshold, so the knob is live.
    golds, _ = reconstruct(submissions, scores, plurality_threshold=0.6)
    assert golds == {"1": "0.12"}


def test_list_answers_agree_regardless_of_ordering():
    # DABStep's own scorer is order-insensitive for lists, so two correct
    # submissions differing only in ordering are the same answer.
    submissions = {
        "a": {"1": "741, 709, 454"},
        "b": {"1": "454, 709, 741"},
        "c": {"1": "709,741,454"},
    }
    scores = {agent: {"1": True} for agent in submissions}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {"1": "741, 709, 454"}
    assert exclusions == {}


def test_agreement_is_normalization_insensitive():
    submissions = {"a": {"1": "['C']"}, "b": {"1": "C"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    golds, _ = reconstruct(submissions, scores)
    assert "1" in golds


def test_tasks_absent_from_every_submission_are_excluded():
    golds, exclusions = reconstruct({"a": {"1": "x"}}, {"a": {"1": False}})
    assert golds == {}
    assert exclusions["1"] == "no_correct_submission"


def test_a_gold_records_the_share_that_supported_it():
    submissions = {f"a{i}": {"1": "0.12"} for i in range(3)}
    submissions["noisy"] = {"1": "totally other"}
    scores = {agent: {"1": True} for agent in submissions}
    golds, _, shares = reconstruct_with_shares(submissions, scores)
    assert golds == {"1": "0.12"}
    assert shares["1"] == 0.75


def test_dev_gate_passes_when_every_dev_answer_is_reproduced():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches, absent = check_dev_gate({"1": "0.12", "2": "NL"}, dev)
    assert ok is True
    assert mismatches == []
    assert absent == []


def test_dev_gate_fails_loudly_on_one_mismatch():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches, _ = check_dev_gate({"1": "0.12", "2": "BE"}, dev)
    assert ok is False
    assert mismatches == ["2"]


def test_dev_gate_fails_when_a_dev_task_was_never_reconstructed():
    # With no corpus supplied nothing can be excused as absent: a dev task we
    # failed to reconstruct is a mismatch, not a silent pass.
    dev = [{"task_id": "1", "answer": "0.12"}]
    ok, mismatches, absent = check_dev_gate({}, dev)
    assert ok is False
    assert mismatches == ["1"]
    assert absent == []


def test_dev_gate_reports_absent_dev_tasks_separately_from_mismatches():
    # Four of DABStep's ten dev tasks appear in no submission file, so no rule
    # change can reconstruct them. They are unverifiable, not wrong — and they
    # must be reported rather than counted as passes.
    dev = [
        {"task_id": "1", "answer": "0.12"},
        {"task_id": "99", "answer": "unreachable"},
    ]
    ok, mismatches, absent = check_dev_gate({"1": "0.12"}, dev, corpus_ids={"1"})
    assert ok is True
    assert mismatches == []
    assert absent == ["99"]


def test_dev_gate_still_fails_on_an_in_corpus_mismatch_when_others_are_absent():
    dev = [
        {"task_id": "1", "answer": "0.12"},
        {"task_id": "99", "answer": "unreachable"},
    ]
    ok, mismatches, absent = check_dev_gate({"1": "9.99"}, dev, corpus_ids={"1"})
    assert ok is False
    assert mismatches == ["1"]
    assert absent == ["99"]


def test_the_stored_gold_is_the_modal_rendering_of_its_group():
    # Agreement was established up to _norm, but the stored string is what Task 4
    # grades against with a different normalizer. A bracketed variant that merely
    # sorts first must not become the gold.
    submissions = {f"clean{i}": {"1": "POS: 88.49, Ecommerce: 97.68"} for i in range(5)}
    submissions["aaa_bracketed"] = {"1": "[POS: 88.49, Ecommerce: 97.68, ]"}
    submissions["bbb_bracketed"] = {"1": "[POS: 88.49, Ecommerce: 97.68, ]"}
    scores = {agent: {"1": True} for agent in submissions}
    golds, _ = reconstruct(submissions, scores)
    assert golds["1"] == "POS: 88.49, Ecommerce: 97.68"


def test_a_threshold_that_would_admit_a_tie_is_rejected():
    # At exactly 0.5 a two-way tie "wins" by dict insertion order, which is not
    # a consensus. The report tabulates a 0.60 sweep; 0.50 must not be reachable.
    submissions = {"a": {"1": "x"}, "b": {"1": "x"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    for bad in (0.5, 0.4, 0.0):
        with pytest.raises(ValueError, match="must be > 0.5"):
            reconstruct(submissions, scores, plurality_threshold=bad)


def test_the_gate_fails_when_no_dev_task_could_be_checked():
    # An empty or failed download covers none of the dev ids. Absence of
    # mismatches is not evidence when nothing was compared.
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches, absent = check_dev_gate({}, dev, corpus_ids=set())
    assert ok is False
    assert mismatches == []
    assert absent == ["1", "2"]


def test_the_gate_fails_on_an_empty_dev_split():
    ok, mismatches, absent = check_dev_gate({"1": "0.12"}, [])
    assert ok is False
    assert mismatches == []
    assert absent == []


def test_the_gate_fails_when_golds_are_empty_and_the_corpus_covers_the_dev_ids():
    # Reconstruction produced nothing, but the corpus does cover the dev tasks:
    # that is a real mismatch, not an excusable absence.
    dev = [{"task_id": "1", "answer": "0.12"}]
    ok, mismatches, absent = check_dev_gate({}, dev, corpus_ids={"1"})
    assert ok is False
    assert mismatches == ["1"]
    assert absent == []
