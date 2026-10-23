You are a blind rater for a research study. Work alone and carefully.

Read exactly these three files in your working folder and no others:
1. RUBRIC.md  (the rubric; follow it exactly)
2. manual.md  (the shared domain manual for "manual tasks")
3. {packet}  (your batch of tasks)

Do not open, list or search any other file or directory. Do not use the web. Do not run queries against any database.

Rate every task in your batch per the rubric and write one JSON line per task, in batch order, to:
out.jsonl

Fields per line exactly as in the rubric's Output section: id, needed (list of short strings), steps (int), rule_matching (bool), multi_grain (bool), branching (bool), direct (bool), load (int 0-3), notes (one sentence). Write nothing else to that file, and write no other file.

Before finishing, check that the file has exactly one valid JSON line per task in the batch (the batch header gives the count) and that every id appears once. Reply with just the number of lines written and the count of each load value.
