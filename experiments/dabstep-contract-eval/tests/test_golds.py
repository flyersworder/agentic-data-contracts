from dce.golds import check_dev_gate, reconstruct


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


def test_correct_submissions_that_disagree_are_excluded_not_guessed():
    # Both scored correct but their text differs beyond normalization: the
    # leaderboard scorer accepted something our normalizer cannot reconcile,
    # so we must not pick a winner.
    submissions = {"a": {"1": "0.12"}, "b": {"1": "totally other"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {}
    assert exclusions["1"] == "conflicting_golds"


def test_agreement_is_normalization_insensitive():
    submissions = {"a": {"1": "['C']"}, "b": {"1": "C"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    golds, _ = reconstruct(submissions, scores)
    assert "1" in golds


def test_tasks_absent_from_every_submission_are_excluded():
    golds, exclusions = reconstruct({"a": {"1": "x"}}, {"a": {"1": False}})
    assert golds == {}
    assert exclusions["1"] == "no_correct_submission"


def test_dev_gate_passes_when_every_dev_answer_is_reproduced():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches = check_dev_gate({"1": "0.12", "2": "NL"}, dev)
    assert ok is True
    assert mismatches == []


def test_dev_gate_fails_loudly_on_one_mismatch():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches = check_dev_gate({"1": "0.12", "2": "BE"}, dev)
    assert ok is False
    assert mismatches == ["2"]


def test_dev_gate_fails_when_a_dev_task_was_never_reconstructed():
    # The spec's unverified assumption: dev tasks may not appear in submissions
    # at all. Missing must fail the gate, not pass it vacuously.
    dev = [{"task_id": "1", "answer": "0.12"}]
    ok, mismatches = check_dev_gate({}, dev)
    assert ok is False
    assert mismatches == ["1"]
