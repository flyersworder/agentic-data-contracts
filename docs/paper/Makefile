# Two pdflatex passes bracket bibtex: the first writes the .aux bibtex reads,
# and the last two resolve \ref and \cite forward references. Dropping either
# trailing pass leaves "??" in the PDF.
.PHONY: all clean check

all: main.pdf

main.pdf: main.tex refs.bib $(wildcard sections/*.tex)
	pdflatex -interaction=nonstopmode -halt-on-error main.tex
	-bibtex main
	pdflatex -interaction=nonstopmode -halt-on-error main.tex
	pdflatex -interaction=nonstopmode -halt-on-error main.tex

# Fails loudly on the two defects a clean exit code hides: boxes wider than
# the column, and unresolved references that render as "??".
check: main.pdf
	@n=$$(grep -c 'Overfull \\hbox' main.log || true); \
	 echo "overfull hboxes: $$n"; test "$$n" -eq 0
	@n=$$(grep -c 'undefined' main.log | grep -v 'Font' || true); \
	 grep 'Reference.*undefined\|Citation.*undefined' main.log && exit 1 || \
	 echo "all references and citations resolved"

clean:
	rm -f main.pdf main.aux main.log main.out main.bbl main.blg
