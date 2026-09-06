# Two pdflatex passes bracket bibtex: the first writes the .aux bibtex reads,
# and the last two resolve \ref and \cite forward references. Dropping either
# trailing pass leaves "??" in the PDF.
.PHONY: all clean check figures pvldb

all: main.pdf

FIGS := figures/fig-ladder.pdf figures/fig-interaction.pdf
RESULTS := ../../experiments/dabstep-contract-eval/results

# Regenerated from the raw result rows, not committed as opaque images.
# The script asserts every value it draws against the paper's own numbers.
figures: $(FIGS)

$(FIGS): figures/make_figures.py \
         $(RESULTS)/glm-full.jsonl $(RESULTS)/dsflash-full.jsonl \
         $(RESULTS)/sonnet5-full.jsonl $(RESULTS)/sol-full.jsonl
	cd figures && uv run make_figures.py

SOURCES := main.tex refs.bib $(FIGS) $(wildcard sections/*.tex)

# Extended build: the arXiv version, appendix included.
main.pdf: $(SOURCES)
	pdflatex -interaction=nonstopmode -halt-on-error main.tex
	-bibtex main
	pdflatex -interaction=nonstopmode -halt-on-error main.tex
	pdflatex -interaction=nonstopmode -halt-on-error main.tex

# Submission build: acmart sigconf, no appendix. PVLDB counts appendices
# toward its 12 content pages, so the appendix is arXiv-only. Needs acmart:
#   tlmgr init-usertree && tlmgr --usermode install acmart   (no admin needed)
pvldb: pvldb.pdf
pvldb.pdf: pvldb.tex refs.bib $(FIGS) $(wildcard sections/*.tex)
	pdflatex -interaction=nonstopmode -halt-on-error pvldb.tex
	-bibtex pvldb
	pdflatex -interaction=nonstopmode -halt-on-error pvldb.tex
	pdflatex -interaction=nonstopmode -halt-on-error pvldb.tex

# Fails loudly on the defects a clean exit code hides: boxes wider than the
# column, unresolved references that render as "??", and a submission whose
# content (everything before the bibliography) runs past PVLDB's 12 pages.
check: main.pdf pvldb.pdf
	@for j in main pvldb; do \
	   n=$$(grep -c 'Overfull \\hbox' $$j.log || true); \
	   echo "$$j: overfull hboxes: $$n"; test "$$n" -eq 0 || exit 1; \
	   grep 'Reference.*undefined\|Citation.*undefined' $$j.log && exit 1 || \
	   echo "$$j: all references and citations resolved"; \
	 done
	@p=$$(sed -n 's/.*newlabel{end:content}{{[^}]*}{\([0-9]*\)}.*/\1/p' pvldb.aux | head -1); \
	 echo "pvldb: content pages: $$p (limit 12)"; test "$$p" -le 12
	@c=$$(wc -m < abstract.txt | tr -d ' '); \
	 echo "abstract.txt: $$c characters (arXiv cap 1920)"; test "$$c" -le 1920
	@cd ../../experiments/dabstep-contract-eval && uv run python analysis/cost_decomposition.py --check

clean:
	rm -f main.pdf main.aux main.log main.out main.bbl main.blg \
	      pvldb.pdf pvldb.aux pvldb.log pvldb.out pvldb.bbl pvldb.blg
